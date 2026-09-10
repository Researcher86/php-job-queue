<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\Job;
use App\Job\JobPriority;
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
    /**
     * One FIFO per priority. Written out rather than built from
     * JobPriority::cases(), so that pop() can index a lane without
     * checking whether it exists - every case has one, and adding a case
     * to the enum without a lane here is a fatal error rather than a
     * silently dropped job.
     *
     * @var array<string, list<Job>> priority name => FIFO lane
     */
    private array $ready = [
        JobPriority::HIGH->name => [],
        JobPriority::NORMAL->name => [],
        JobPriority::LOW->name => [],
    ];

    public function __construct(
        private readonly Clock $clock = new SystemClock(),
        private readonly LaneSelector $selector = new StrictPriority(),
        // A new one per queue - see InMemoryQueue.
        private readonly DelayedJobScheduler $scheduler = new DelayedJobScheduler(),
    ) {
    }

    public function push(Job $job, int $delay = 0): void
    {
        if (!$this->scheduler->holdIfNotDue($job, $delay, $this->clock->now())) {
            $this->ready[$job->getPriority()->name][] = $job;
        }
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
        return $this->readySize() + $this->scheduler->size();
    }

    public function readySize(): int
    {
        return array_sum(array_map('count', $this->ready));
    }

    public function delayedSize(): int
    {
        return $this->scheduler->size();
    }

    public function nextDeadline(): ?float
    {
        return $this->scheduler->nextDeadline();
    }
}
