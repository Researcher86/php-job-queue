<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\Job;

/**
 * Where jobs wait - PLAN.md Phase 2.
 *
 * Two implementations: InMemoryQueue (one FIFO) and PriorityQueue (one FIFO
 * per priority, plus a LaneSelector to choose between them). Both hold
 * delayed jobs in a DelayedJobScheduler, which is why delayedSize() is part
 * of the contract and not an implementation detail: "40 jobs waiting" means
 * something very different when 39 of them are not due for an hour.
 */
interface Queue
{
    /**
     * $delay is in seconds, and only applies to a job that has not been
     * pushed before - a job coming back from a retry already carries its
     * own availableAt.
     */
    public function push(Job $job, int $delay = 0): void;

    /**
     * The next available job, or null if there is none - which is not the
     * same as the queue being empty: everything in it may be delayed.
     *
     * $now overrides the queue's clock, so a test can ask what the queue
     * would hand out at a given instant.
     */
    public function pop(?float $now = null): ?Job;

    /** Everything waiting, available or not. */
    public function size(): int;

    /** Jobs waiting and available now. */
    public function readySize(): int;

    /** Jobs waiting for a deadline: a delay, or a retry under backoff. */
    public function delayedSize(): int;

    /**
     * When the earliest delayed job becomes available, or null if none is
     * waiting. What lets a runtime loop sleep until there is something to
     * do instead of waking up to ask.
     */
    public function nextDeadline(): ?float;
}
