<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\Job;
use App\Job\JobState;
use App\Persistence\JobStorage;
use App\Scheduler\DelayedJobScheduler;
use App\Support\Clock;
use App\Support\SystemClock;

/**
 * The FIFO queue from PLAN.md Phase 2, grown two things since:
 *
 *   - a DelayedJobScheduler for jobs that are not available yet (Phase 8);
 *   - an optional JobStorage, so the queue can be rebuilt after the queue
 *     process dies (Phase 12).
 *
 * Ready jobs are a plain array used as a FIFO. There is no ordering
 * decision to make here - that is what PriorityQueue is for.
 */
final class InMemoryQueue implements Queue
{
    /** @var list<Job> */
    private array $ready = [];

    public function __construct(
        private readonly Clock $clock = new SystemClock(),
        // Not readonly: restoreFromStorage() attaches the log only after the
        // replay, so the replay does not write back what it just read.
        private readonly ?JobStorage $storage = new DelayedJobScheduler(),
    ) {
    }

    public function push(Job $job, int $delay = 0): void
    {
        // Waiting or not is the scheduler's decision, and the same one for
        // both queues - see DelayedJobScheduler::holdIfNotDue().
        if (!$this->scheduler->holdIfNotDue($job, $delay, $this->clock->now())) {
            $this->ready[] = $job;
        }

        // Either way the job's state changed, and the log has to know:
        // recovery reads the LAST thing written about a job.
        $this->persist($job);
    }

    public function pop(?float $now = null): ?Job
    {
        $now ??= $this->clock->now();

        foreach ($this->scheduler->releaseDue($now) as $job) {
            $this->ready[] = $job;
        }

        return array_shift($this->ready);
    }

    public function size(): int
    {
        return $this->readySize() + $this->scheduler->size();
    }

    public function readySize(): int
    {
        return count($this->ready);
    }

    public function delayedSize(): int
    {
        return $this->scheduler->size();
    }

    public function nextDeadline(): ?float
    {
        return $this->scheduler->nextDeadline();
    }

    /**
     * Rebuilds a queue from what storage remembers - PLAN.md Phase 12.
     *
     * PROCESSING is the interesting state. A job the log last saw as
     * PROCESSING was in a worker's hands when the process died, and nobody
     * is left to ACK it, so it comes back as READY and runs again. That is
     * the at-least-once bargain restated at the persistence layer: the same
     * job may execute twice, and handlers must be idempotent.
     *
     * COMPLETED and FAILED are terminal and are simply not restored - a
     * finished job is not work.
     */
    public static function restoreFromStorage(JobStorage $storage, Clock $clock = new SystemClock()): self
    {
        // Replayed with no storage attached, then attached afterwards: a
        // push() writes, and writing back every job we just read would add
        // a full copy of the log on every restart. Nothing is lost by
        // waiting - a job restored as READY that dies again before being
        // dispatched is still logged as PROCESSING, and comes back the same
        // way next time.
        $queue = new self($clock);

        foreach ($storage->load() as $data) {
            $job = Job::fromArray($data);

            if ($job->getState() === JobState::PROCESSING) {
                $job->markRetry($queue->clock->now());
            }

            if (!$job->getState()->isTerminal()) {
                $queue->push($job);
            }
        }

        $queue->storage = $storage;

        return $queue;
    }

    private function persist(Job $job): void
    {
        $this->storage?->store($job->getId()->toString(), $job->toArray());
    }
}
