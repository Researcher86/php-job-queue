<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\Job;
use App\Job\JobPriority;
use App\Scheduler\DelayedJobScheduler;
use App\Support\Clock;
use App\Support\SystemClock;
use SplQueue;

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
     * One FIFO per priority, keyed by JobPriority::name.
     *
     * SplQueue rather than an array for the reason InMemoryQueue explains:
     * array_shift() is O(n), which makes draining a lane O(n^2).
     *
     * @var array<string, SplQueue<Job>>
     */
    private array $ready = [];

    public function __construct(
        private readonly Clock $clock = new SystemClock(),
        private readonly LaneSelector $selector = new StrictPriority(),
        // A new one per queue - see InMemoryQueue.
        private readonly DelayedJobScheduler $scheduler = new DelayedJobScheduler(),
    ) {
        // The one thing here that cannot be a property default: an SplQueue
        // per lane needs `new` once per case, and a property initializer
        // takes constant expressions only. Built from cases() so that a
        // priority added to the enum gets a lane without anyone
        // remembering to add one.
        foreach (JobPriority::cases() as $priority) {
            $this->ready[$priority->name] = new SplQueue();
        }
    }

    public function push(Job $job, int $delay = 0): void
    {
        if (!$this->scheduler->holdIfNotDue($job, $delay, $this->clock->now())) {
            $this->ready[$job->getPriority()->name]->enqueue($job);
        }
    }

    public function pop(?float $now = null): ?Job
    {
        $now ??= $this->clock->now();

        foreach ($this->scheduler->releaseDue($now) as $job) {
            $this->ready[$job->getPriority()->name]->enqueue($job);
        }

        $lane = $this->selector->next($this->depth());

        return $lane === null ? null : $this->ready[$lane->name]->dequeue();
    }

    public function size(): int
    {
        return $this->readySize() + $this->scheduler->size();
    }

    public function readySize(): int
    {
        return array_sum($this->depth());
    }

    /**
     * How many jobs are waiting per lane, keyed the way LaneSelector wants
     * them.
     *
     * @return array<string, int>
     */
    private function depth(): array
    {
        return array_map(static fn (SplQueue $lane): int => $lane->count(), $this->ready);
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
