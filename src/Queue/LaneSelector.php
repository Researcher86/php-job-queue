<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\JobPriority;

/**
 * Decides which priority lane PriorityQueue serves next - PLAN.md Phase 13.
 *
 * Pulled out into its own object for the same reason RetryPolicy is one:
 * the queue's job is to hold jobs in lanes, and the policy question ("does
 * LOW ever get a turn?") has more than one defensible answer. Two are
 * shipped, StrictPriority and WeightedRoundRobin, and swapping them changes
 * nothing else.
 *
 * A selector may be stateful - WeightedRoundRobin is - so one belongs to
 * one queue.
 */
interface LaneSelector
{
    /**
     * The lane to take the next job from, or null if there is nothing to
     * take.
     *
     * $depth is how many jobs are waiting per lane, keyed by
     * JobPriority::name, and includes lanes that are empty. A selector must
     * never name an empty lane: the queue asks once and pops what it is
     * told.
     *
     * @param array<string, int> $depth priority name => jobs waiting
     */
    public function next(array $depth): ?JobPriority;
}
