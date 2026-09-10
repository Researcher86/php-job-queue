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
    private Clock $clock;

    private DelayedJobScheduler $scheduler;

    /** @var list<Job> */
    private array $ready = [];

    public function __construct(?Clock $clock = null, private ?JobStorage $storage = null)
    {
        $this->clock = $clock ?? new SystemClock();
        $this->scheduler = new DelayedJobScheduler();
    }

    public function push(Job $job, int $delay = 0): void
    {
        if ($job->getState() === JobState::CREATED) {
            $delay > 0
                ? $job->markDelayed($this->clock->now() + $delay)
                : $job->markReady($this->clock->now());
        }

        $availableAt = $job->getAvailableAt();

        // Two separate reasons to wait, one mechanism - see
        // DelayedJobScheduler. DELAYED is a job dispatched with a delay; a
        // READY job with a future availableAt is a retry under backoff.
        if ($job->getState() === JobState::DELAYED || ($availableAt !== null && $availableAt > $this->clock->now())) {
            $this->scheduler->schedule($job);
            $this->persist($job);

            return;
        }

        $this->ready[] = $job;
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
    public static function restoreFromStorage(JobStorage $storage, ?Clock $clock = null): self
    {
        $queue = new self($clock, $storage);

        foreach ($storage->load() as $data) {
            $job = Job::fromArray($data);

            if ($job->getState() === JobState::PROCESSING) {
                $job->markRetry($queue->clock->now());
            }

            if ($job->getState() === JobState::READY || $job->getState() === JobState::DELAYED) {
                $queue->push($job);
            }
        }

        return $queue;
    }

    private function persist(Job $job): void
    {
        $this->storage?->store($job->getId()->toString(), $job->toArray());
    }
}
