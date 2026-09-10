<?php

declare(strict_types=1);

namespace App\Scheduler;

use App\Job\Job;
use App\Job\JobState;
use RuntimeException;

/**
 * Holds jobs that are not allowed to run yet, and hands them back the
 * moment they are - PLAN.md Phase 8.
 *
 * Two different things end up here, and it matters that they are the same
 * mechanism:
 *
 *   - a DELAYED job, dispatched with `delay: 60` and not due until later;
 *   - a READY job with a future availableAt, which is what a retry under a
 *     backoff policy is (see Job::markRetry()).
 *
 * A retry is a delayed job. Backoff is not a second waiting mechanism
 * bolted onto the first; it is the first one, reused.
 *
 * ## Why a heap
 *
 * The obvious implementation is an array kept sorted on every push. That
 * costs O(n log n) per push and re-sorts jobs whose position never changed;
 * with a fixed-delay retry policy and a queue full of failing jobs, every
 * single failure pays for a full sort. A heap pays O(log n) to insert and
 * O(log n) to pop, and - the part that actually matters for the runtime
 * loop - answers "when is the next one due?" in O(1), by looking at the
 * root. Nothing ever scans the whole set.
 *
 * That last property is what nextDeadline() exists for: a loop that knows
 * the next deadline can sleep until it instead of waking up to poll.
 */
final class DelayedJobScheduler
{
    private DelayedJobHeap $heap;

    /** Insertion counter, the heap's tie-break for equal deadlines. */
    private int $sequence = 0;

    public function __construct()
    {
        // Built here rather than taken as a promoted parameter: the heap is
        // this object's own, and a constructor argument would advertise that
        // two schedulers could be made to share one.
        $this->heap = new DelayedJobHeap();
    }

    /**
     * Takes a job whose availableAt is already set - by markDelayed() for a
     * delay, or markRetry() for a backoff. A job with no availableAt has no
     * deadline to be scheduled against, which is a caller bug rather than
     * something to paper over with a default.
     */
    public function schedule(Job $job): void
    {
        $availableAt = $job->getAvailableAt();

        if ($availableAt === null) {
            throw new RuntimeException(sprintf(
                'Cannot schedule job %s: it has no availableAt',
                $job->getId(),
            ));
        }

        $this->heap->insert([
            'job' => $job,
            'at' => $availableAt,
            'seq' => $this->sequence++,
        ]);
    }

    /**
     * Decides whether a job being pushed has to wait, and keeps it if so.
     * Returns true when the scheduler took it, false when the job is
     * available now and belongs in the caller's ready set.
     *
     * This is the whole of a queue's push() decision, in one place, because
     * both queues were making it identically:
     *
     *   - A CREATED job is entering the system, and $delay says when it may
     *     run. Nothing else may pass a delay: a job coming back from a
     *     retry already carries its own availableAt, and a second delay
     *     applied to it would silently move its deadline.
     *   - Anything with a deadline in the future waits, whether it got there
     *     by markDelayed() (a delay) or markRetry() (a backoff).
     *
     * @param int $delay seconds, and only meaningful for a CREATED job
     */
    public function holdIfNotDue(Job $job, int $delay, float $now): bool
    {
        if ($job->getState() === JobState::CREATED) {
            $delay > 0 ? $job->markDelayed($now + $delay) : $job->markReady($now);
        }

        $availableAt = $job->getAvailableAt();

        if ($job->getState() !== JobState::DELAYED && ($availableAt === null || $availableAt <= $now)) {
            return false;
        }

        $this->schedule($job);

        return true;
    }

    /**
     * Every job whose deadline has passed, in deadline order, removed from
     * the scheduler.
     *
     * Stops at the first job that is not due yet - the root is the earliest
     * deadline, so if that one is in the future, so is everything behind
     * it. This is the loop the sorted-array version also ran, except it
     * needed the array sorted first.
     *
     * A DELAYED job is moved to READY on the way out. One that is already
     * READY (a retry) is left alone: it never stopped being ready, it was
     * only unavailable.
     *
     * @return list<Job>
     */
    public function releaseDue(float $now): array
    {
        $due = [];

        while (!$this->heap->isEmpty() && $this->heap->top()['at'] <= $now) {
            $job = $this->heap->extract()['job'];

            if ($job->getState() === JobState::DELAYED) {
                $job->markReady($now);
            }

            $due[] = $job;
        }

        return $due;
    }

    /**
     * When the earliest job becomes available, or null if nothing is
     * waiting. O(1) - see the class docblock.
     */
    public function nextDeadline(): ?float
    {
        return $this->heap->isEmpty() ? null : $this->heap->top()['at'];
    }

    public function size(): int
    {
        return $this->heap->count();
    }
}
