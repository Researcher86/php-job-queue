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
