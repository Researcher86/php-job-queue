<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\JobPriority;
use InvalidArgumentException;

/**
 * Serves the lanes in rounds, each lane getting as many turns per round as
 * its weight - PLAN.md Phase 13's answer to starvation.
 *
 * With the plan's example weights (HIGH 5, NORMAL 3, LOW 1) and all three
 * lanes busy, the order is:
 *
 *   H H H H H  N N N  L   H H H H H  N N N  L   ...
 *
 * so HIGH still gets 5/9 of the throughput and LOW still goes last, but LOW
 * goes. That is the whole difference from StrictPriority: priority becomes
 * a share of the workers rather than a veto over them.
 *
 * ## How a round works
 *
 * Each lane starts a round with credits equal to its weight. next() serves
 * the highest-priority lane that has both jobs waiting and credits left,
 * and spends one credit. When no lane qualifies, the round is over and the
 * credits are refilled.
 *
 * Two consequences of "has jobs AND credits", both wanted:
 *
 *  - An empty lane spends nothing. A queue with only HIGH jobs in it runs
 *    at full speed on HIGH; the fairness only costs something when there is
 *    something to be fair to.
 *  - A lane that runs out of credits is skipped even though it has jobs,
 *    which is exactly the pressure release. HIGH hitting its limit is what
 *    lets NORMAL and LOW move at all.
 */
final class WeightedRoundRobin implements LaneSelector
{
    /** @var array<string, int> priority name => turns per round */
    private array $weights;

    /** @var array<string, int> priority name => turns left this round */
    private array $credits;

    /**
     * Defaults to PLAN.md Phase 13's example: 5 HIGH per 3 NORMAL per 1 LOW.
     */
    public function __construct(int $high = 5, int $normal = 3, int $low = 1)
    {
        if ($high < 1 || $normal < 1 || $low < 1) {
            throw new InvalidArgumentException(
                'Every lane needs a weight of at least 1, or it would be starved by construction',
            );
        }

        $this->weights = [
            JobPriority::HIGH->name => $high,
            JobPriority::NORMAL->name => $normal,
            JobPriority::LOW->name => $low,
        ];
        $this->credits = $this->weights;
    }

    public function next(array $depth): ?JobPriority
    {
        $lane = $this->highestFundedLane($depth);

        if ($lane === null) {
            // Either the round is spent or the queue is empty. Refill and
            // ask once more: if it is still null, there is genuinely
            // nothing to serve.
            $this->credits = $this->weights;
            $lane = $this->highestFundedLane($depth);
        }

        if ($lane !== null) {
            $this->credits[$lane->name]--;
        }

        return $lane;
    }

    /**
     * The highest-priority lane with a job waiting and a turn left.
     *
     * @param array<string, int> $depth
     */
    private function highestFundedLane(array $depth): ?JobPriority
    {
        foreach (JobPriority::cases() as $priority) {
            if (($depth[$priority->name] ?? 0) > 0 && $this->credits[$priority->name] > 0) {
                return $priority;
            }
        }

        return null;
    }
}
