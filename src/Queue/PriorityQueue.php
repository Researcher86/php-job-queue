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
 * One FIFO lane per priority, drained highest-first - PLAN.md Phase 13.
 *
 * Delayed jobs are held by the same DelayedJobScheduler InMemoryQueue uses,
 * and land in the lane for their own priority once due. Waiting and
 * ordering are separate questions: a job's priority does not change when it
 * becomes available.
 */
final class PriorityQueue implements Queue
{
    /** Drain order. Highest first, and LOW only once nothing else is ready. */
    private const array LANES = [JobPriority::HIGH, JobPriority::NORMAL, JobPriority::LOW];

    private Clock $clock;

    private DelayedJobScheduler $scheduler;

    /** @var array<string, list<Job>> priority name => FIFO lane */
    private array $ready;

    public function __construct(?Clock $clock = null)
    {
        $this->clock = $clock ?? new SystemClock();
        $this->scheduler = new DelayedJobScheduler();
        $this->ready = array_fill_keys(array_map(
            static fn (JobPriority $priority): string => $priority->name,
            self::LANES,
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

        foreach (self::LANES as $priority) {
            $job = array_shift($this->ready[$priority->name]);

            if ($job !== null) {
                return $job;
            }
        }

        return null;
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
