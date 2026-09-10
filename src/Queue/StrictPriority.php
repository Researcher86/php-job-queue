<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\JobPriority;

/**
 * Always the highest non-empty lane. The obvious policy, and the one that
 * starves.
 *
 * It is the default because it is what "priority queue" means to most
 * people, and because the failure it has is worth seeing rather than
 * hiding: while HIGH keeps arriving faster than the workers drain it, a LOW
 * job waits forever. Not "waits a long time" - forever. There is no
 * mechanism in this policy that ever gives it a turn.
 *
 * That is a legitimate choice when LOW work is genuinely optional. When it
 * is not, use WeightedRoundRobin. Both are tested, including the starvation
 * itself - see PriorityQueueTest.
 */
final readonly class StrictPriority implements LaneSelector
{
    public function next(array $depth): ?JobPriority
    {
        foreach (JobPriority::cases() as $priority) {
            if (($depth[$priority->name] ?? 0) > 0) {
                return $priority;
            }
        }

        return null;
    }
}
