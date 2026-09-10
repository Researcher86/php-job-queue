<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\Job;
use App\Job\JobPriority;
use App\Job\JobState;
use App\Scheduler\DelayedJobScheduler;
use App\Support\Clock;
use App\Support\SystemClock;

/**
 * One FIFO lane per priority - PLAN.md Phase 13.
 *
 * The queue holds the lanes; a LaneSelector decides which one to serve
 * next. That split is the point of the phase: StrictPriority always takes
 * the highest non-empty lane and can starve LOW forever, WeightedRoundRobin
 * gives each lane a share of the turns, and neither policy needs the queue
 * to know which one it is running.
 *
 * The default is StrictPriority, because that is what "priority queue"
 * usually means - and because its failure mode is worth being able to see.
 *
 * Delayed jobs are held by the same DelayedJobScheduler InMemoryQueue uses,
 * and land in the lane for their own priority once due. Waiting and
 * ordering are separate questions: a job's priority does not change while
 * it waits.
 */
final class PriorityQueue implements Queue
{
    private Clock $clock;

    private LaneSelector $selector;

    private DelayedJobScheduler $scheduler;

    /** @var array<string, list<Job>> priority name => FIFO lane */
    private array $ready;

    public function __construct(?Clock $clock = null, ?LaneSelector $selector = null)
    {
        $this->clock = $clock ?? new SystemClock();
        $this->selector = $selector ?? new StrictPriority();
        $this->scheduler = new DelayedJobScheduler();
        $this->ready = array_fill_keys(array_map(
            static fn (JobPriority $priority): string => $priority->name,
            JobPriority::cases(),
        ), []);
    }

    public function push(Job $job, int $delay = 0): void
    {
        if ($job->getState() === JobState::CREATED) {
            $delay > 0
                ? $job->markDelayed($this->clock->now() + $delay)
                : $job->markReady($this->clock->now());
        }

        $availableAt = $job->getAvailableAt();

        if ($job->getState() === JobState::DELAYED || ($availableAt !== null && $availableAt > $this->clock->now())) {
            $this->scheduler->schedule($job);

            return;
        }

        $this->ready[$job->getPriority()->name][] = $job;
    }

    public function pop(?float $now = null): ?Job
    {
        $now ??= $this->clock->now();

        foreach ($this->scheduler->releaseDue($now) as $job) {
            $this->ready[$job->getPriority()->name][] = $job;
        }

        $lane = $this->selector->next(array_map('count', $this->ready));

        return $lane === null ? null : array_shift($this->ready[$lane->name]);
    }

    public function size(): int
    {
        return array_sum(array_map('count', $this->ready)) + $this->scheduler->size();
    }

    public function delayedSize(): int
    {
        return $this->scheduler->size();
    }
}
