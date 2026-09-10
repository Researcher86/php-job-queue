<?php

declare(strict_types=1);

namespace App\Delivery;

use App\Job\Job;

/**
 * One handing-out of one job to one worker - the lease, and the thing an
 * ACK has to be answering.
 *
 * ## Why a job id is not enough
 *
 * A job can legitimately be in a worker's hands twice at once. The
 * visibility deadline passes while a slow handler is still running, the job
 * goes back to READY, and a second worker gets it - a duplicate delivery,
 * which is exactly what at-least-once means. Both workers will eventually
 * answer, about the same job id.
 *
 * Without a way to tell the two apart, the first worker's late answer looks
 * like an answer to the second delivery, because by then the job really is
 * PROCESSING again - the second worker put it there. Checking the job's
 * STATE cannot separate them; it is the same object.
 *
 * That is not a harmless duplicate. Observed before this class existed: the
 * obsolete answer completed the job while the live worker was still running
 * it, released the live delivery from the visibility monitor (leaving a job
 * in flight with no deadline that could ever bring it back), and then the
 * live worker's answer - a FAILURE - was discarded as stale. The roles were
 * inverted: the delivery nobody was waiting for decided the job's fate.
 *
 * ## The token
 *
 * $generation is which handing-out this is: 1, 2, 3... An answer carrying an
 * older generation than the one the monitor holds is answering a lease that
 * has been revoked, and is ignored. This is fencing, the same shape as a
 * lease token or an SQS receipt handle: the right to acknowledge belongs to
 * a specific claim, not to whoever holds the id.
 *
 * The number is the job's attempt count at dispatch, and no new counter was
 * needed for it - `Job::markProcessing()` already increments attempts
 * exactly once per handing-out, which is why the project documents an
 * attempt as a DELIVERY rather than a run. The fencing token was already in
 * the model; it just was not being used as one.
 *
 * ## Immutable, deliberately
 *
 * The job it points at is mutable and shared - both deliveries of a job
 * hold the same object. Everything ABOUT the delivery is a snapshot taken
 * when it was created, which is the whole point: the generation cannot
 * drift into agreeing with a later one.
 */
final readonly class Delivery
{
    public function __construct(
        private Job $job,
        /** Which handing-out of this job this is - see the class docblock. */
        private int $generation,
        private int $workerId,
        /** Per the dispatcher's clock, so execution latency is measurable. */
        private float $dispatchedAt,
        // null = this delivery has no deadline and can never go overdue,
        // which is what a null visibility timeout means.
        private ?float $deadline = null,
    ) {
    }

    /**
     * The delivery a job is about to become, given the worker taking it.
     *
     * Called after markProcessing(), because that is what settles the
     * generation.
     */
    public static function of(Job $job, int $workerId, float $now, ?float $deadline = null): self
    {
        return new self(
            job: $job,
            generation: $job->getAttempts(),
            workerId: $workerId,
            dispatchedAt: $now,
            deadline: $deadline,
        );
    }

    public function getJob(): Job
    {
        return $this->job;
    }

    public function jobId(): string
    {
        return $this->job->getId()->toString();
    }

    public function getGeneration(): int
    {
        return $this->generation;
    }

    public function getWorkerId(): int
    {
        return $this->workerId;
    }

    public function getDispatchedAt(): float
    {
        return $this->dispatchedAt;
    }

    public function getDeadline(): ?float
    {
        return $this->deadline;
    }

    /** Whether this delivery's ACK is overdue as of $now. */
    public function isOverdue(float $now): bool
    {
        return $this->deadline !== null && $this->deadline <= $now;
    }

    /**
     * Whether both refer to the same handing-out of the same job.
     *
     * The comparison the whole class exists for: same job AND same
     * generation. Same job with a different generation is a delivery that
     * has been superseded.
     */
    public function is(self $other): bool
    {
        return $this->generation === $other->generation && $this->jobId() === $other->jobId();
    }

    /** For logs and failure messages: "job 8f2a…, delivery 2, worker 3". */
    public function describe(): string
    {
        return sprintf('job %s, delivery %d, worker %d', $this->jobId(), $this->generation, $this->workerId);
    }
}
