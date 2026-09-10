<?php

declare(strict_types=1);

namespace App\Scheduler;

use App\Job\Job;
use SplHeap;

/**
 * Min-heap of jobs ordered by the time they become available, earliest at
 * the root. An implementation detail of DelayedJobScheduler - nothing else
 * should touch it.
 *
 * SplHeap rather than SplMinHeap because the ordering is not the natural
 * ordering of the stored values: the values are entries, and the key being
 * compared is one field of them plus a tie-break.
 *
 * The tie-break is $seq, the insertion order. Two jobs scheduled for the
 * same instant are common (a burst of retries under a fixed-delay policy
 * all land on the same timestamp), and without it the order they come back
 * out in is whatever the heap's internal swaps happen to produce. With it,
 * equal deadlines keep FIFO - the same thing the sorted array this replaced
 * gave, since PHP's sort has been stable since 8.0.
 *
 * @extends SplHeap<array{job: Job, at: float, seq: int}>
 */
final class DelayedJobHeap extends SplHeap
{
    /**
     * Inverted on purpose. SplHeap is a max-heap - it puts the value its
     * compare() calls "greater" at the root - and what belongs at the root
     * here is the EARLIEST deadline. So the comparison reads backwards:
     * $b before $a.
     *
     * @param array{job: Job, at: float, seq: int} $value1
     * @param array{job: Job, at: float, seq: int} $value2
     */
    protected function compare(mixed $value1, mixed $value2): int
    {
        return [$value2['at'], $value2['seq']] <=> [$value1['at'], $value1['seq']];
    }
}
