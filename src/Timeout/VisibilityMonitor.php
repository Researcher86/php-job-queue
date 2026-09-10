<?php

declare(strict_types=1);

namespace App\Timeout;

use App\Delivery\Delivery;
use App\Job\Job;
use App\Support\Clock;
use App\Support\SystemClock;

/**
 * Holds the lease on every job that is in a worker's hands, and takes back
 * the ones nobody ever acknowledged - PLAN.md Phase 9.
 *
 * A worker that dies mid-job cannot report anything. Without this, the job
 * it was holding is simply gone: not in the queue, not completed, not
 * failed. So a dispatched job gets a deadline, and if the ACK does not
 * arrive before it, the job goes back to READY and is handed to someone
 * else.
 *
 * That is the mechanism behind at-least-once delivery, and the reason
 * handlers have to be idempotent: the queue cannot tell "the worker died
 * before starting" from "the worker did the work and died before saying
 * so". It has to assume the pessimistic one, so some jobs run twice. See
 * Idempotency/ChargePaymentJob for what that means for a handler with a
 * side effect that must not repeat.
 *
 * ## One current delivery per job
 *
 * What is stored is not "the jobs in flight" but "the delivery currently
 * entitled to answer for each job". When a deadline expires, the entry is
 * dropped and the job is handed out again as a NEW delivery with a higher
 * generation - so the old worker's answer, when it finally arrives, refers
 * to a lease that no longer exists and is refused by isCurrent().
 *
 * That refusal is the whole point of Delivery, and it is not theoretical:
 * see the class docblock there for what the state-only check let through.
 *
 * ## Tracking and expiry are separate
 *
 * Every dispatched job gets a delivery. Only deliveries created while a
 * timeout is configured get a DEADLINE. A null timeout therefore means
 * "in-flight jobs are still tracked, they just never expire" - which keeps
 * size() an honest gauge either way, keeps the fencing check working, and
 * leaves whether to reclaim jobs to configuration rather than to whether
 * the monitor bothered to notice them.
 *
 * ## Not an execution timeout
 *
 * The deadline says how long a job may stay unacknowledged, not how long a
 * handler may run. A handler that takes longer than the visibility timeout
 * gets its job handed to a second worker while the first is still working
 * on it - that is the timeout being too short, not the handler misbehaving.
 * See PLAN.md's engineering question 2.
 */
final class VisibilityMonitor
{
    /** @var array<string, Delivery> job id => the delivery entitled to answer */
    private array $current = [];

    public function __construct(
        // null disables expiry - see the class docblock. In seconds.
        private readonly ?int $timeout,
        private readonly Clock $clock = new SystemClock(),
    ) {
    }

    /**
     * Issues the lease for a job about to be handed to $workerId, and
     * returns it. Any previous delivery of the same job stops being
     * current, which is exactly what makes its late answer refusable.
     *
     * Call it after markProcessing() - that is what settles the generation.
     */
    public function track(Job $job, int $workerId): Delivery
    {
        $now = $this->clock->now();

        $delivery = Delivery::of(
            job: $job,
            workerId: $workerId,
            now: $now,
            deadline: $this->timeout === null ? null : $now + $this->timeout,
        );

        $this->current[$delivery->jobId()] = $delivery;

        return $delivery;
    }

    /**
     * Whether this delivery is still the one entitled to answer.
     *
     * False for a delivery whose deadline expired and whose job was handed
     * out again, and false for one whose job has already been resolved -
     * both are answers nobody is waiting for.
     */
    public function isCurrent(Delivery $delivery): bool
    {
        return ($this->current[$delivery->jobId()] ?? null)?->is($delivery) ?? false;
    }

    /**
     * The ACK (or the NACK) arrived: the lease is discharged.
     *
     * A delivery that is no longer current releases nothing. That guard is
     * load-bearing - releasing on a stale answer would drop the LIVE
     * delivery's lease and leave a job in flight with no deadline that
     * could ever bring it back.
     */
    public function release(Delivery $delivery): void
    {
        if ($this->isCurrent($delivery)) {
            unset($this->current[$delivery->jobId()]);
        }
    }

    public function isProcessing(Job $job): bool
    {
        return isset($this->current[$job->getId()->toString()]);
    }

    /** How many jobs are in a worker's hands right now, unacknowledged. */
    public function size(): int
    {
        return count($this->current);
    }

    /**
     * The earliest deadline still outstanding, or null if nothing can
     * expire.
     *
     * A linear scan, unlike DelayedJobScheduler's O(1) answer, and that is
     * fine: this set never holds more than one delivery per worker, so it is
     * bounded by the pool size rather than by the queue depth.
     */
    public function nextDeadline(): ?float
    {
        $deadlines = [];

        foreach ($this->current as $delivery) {
            if ($delivery->getDeadline() !== null) {
                $deadlines[] = $delivery->getDeadline();
            }
        }

        return $deadlines === [] ? null : min($deadlines);
    }

    /**
     * Every job whose ACK is overdue, moved back to READY and stripped of
     * its lease. The caller returns them to a queue - see
     * JobDispatcher::requeueExpired().
     *
     * Dropping the lease here is what makes the old worker's eventual
     * answer stale rather than authoritative.
     *
     * @return list<Job>
     */
    public function requeueExpired(?float $now = null): array
    {
        $now ??= $this->clock->now();

        $expired = [];

        foreach ($this->current as $id => $delivery) {
            if ($delivery->isOverdue($now)) {
                $delivery->getJob()->markRetry($now);
                $expired[] = $delivery->getJob();
                unset($this->current[$id]);
            }
        }

        return $expired;
    }
}
