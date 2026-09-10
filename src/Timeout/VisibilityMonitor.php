<?php

declare(strict_types=1);

namespace App\Timeout;

use App\Job\Job;
use App\Support\Clock;
use App\Support\SystemClock;

/**
 * Tracks the jobs that are in a worker's hands, and takes back the ones
 * nobody ever acknowledged - PLAN.md Phase 9.
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
 * ## Tracking and expiry are separate
 *
 * Every dispatched job is tracked. Only jobs dispatched while a timeout is
 * configured get a DEADLINE. A null timeout therefore means "in-flight jobs
 * are still counted, they just never expire" - which is what makes size()
 * an honest gauge of unacknowledged work either way, and leaves the choice
 * of whether to reclaim jobs to configuration rather than to whether the
 * monitor bothered to notice them.
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
    private Clock $clock;

    /** @var array<string, float> job id => the instant its ACK is overdue */
    private array $deadlines = [];

    /** @var array<string, Job> job id => job, for every job in flight */
    private array $processing = [];

    public function __construct(
        // null disables expiry - see the class docblock. In seconds.
        private readonly ?int $timeout,
        ?Clock $clock = null,
    ) {
        $this->clock = $clock ?? new SystemClock();
    }

    public function track(Job $job): void
    {
        $id = $job->getId()->toString();
        $this->processing[$id] = $job;

        if ($this->timeout !== null) {
            $this->deadlines[$id] = $this->clock->now() + $this->timeout;
        }
    }

    /** The ACK (or the NACK) arrived: the job is no longer our problem. */
    public function release(Job $job): void
    {
        $id = $job->getId()->toString();
        unset($this->deadlines[$id], $this->processing[$id]);
    }

    public function isProcessing(Job $job): bool
    {
        return isset($this->processing[$job->getId()->toString()]);
    }

    /** How many jobs are in a worker's hands right now, unacknowledged. */
    public function size(): int
    {
        return count($this->processing);
    }

    /**
     * Every job whose ACK is overdue, moved back to READY and dropped from
     * the monitor. The caller is responsible for actually returning them to
     * a queue - see JobDispatcher::requeueExpired().
     *
     * A job with no deadline (dispatched while the timeout was null) is
     * never overdue.
     *
     * @return list<Job>
     */
    public function requeueExpired(?float $now = null): array
    {
        $now ??= $this->clock->now();

        $expired = [];

        foreach ($this->processing as $id => $job) {
            if (($this->deadlines[$id] ?? INF) <= $now) {
                $job->markRetry($now);
                $expired[] = $job;
                unset($this->deadlines[$id], $this->processing[$id]);
            }
        }

        return $expired;
    }
}
