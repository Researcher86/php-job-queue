<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\Job;
use App\Job\JobState;
use App\Persistence\JobStorage;
use App\Support\Clock;
use App\Support\SystemClock;

final class InMemoryQueue implements Queue
{
    private Clock $clock;

    /** @var list<Job> */
    private array $ready = [];

    /** @var list<Job> */
    private array $delayed = [];

    public function __construct(?Clock $clock = null, private ?JobStorage $storage = null)
    {
        $this->clock = $clock ?? new SystemClock();
    }

    public function push(Job $job, int $delay = 0): void
    {
        $state = $job->getState();

        if ($state === JobState::CREATED) {
            if ($delay > 0) {
                $job->markDelayed($this->clock->now() + $delay);
            } else {
                $job->markReady($this->clock->now());
            }
        }

        if ($job->getState() === JobState::DELAYED) {
            $this->delayed[] = $job;
            $this->sortDelayed();
            $this->persist($job);
            return;
        }

        $availableAt = $job->getAvailableAt();
        if ($availableAt !== null && $availableAt > $this->clock->now()) {
            $this->delayed[] = $job;
            $this->sortDelayed();
            $this->persist($job);
            return;
        }

        $this->ready[] = $job;
        $this->persist($job);
    }

    public function pop(?float $now = null): ?Job
    {
        $now ??= $this->clock->now();

        $this->promoteDelayed($now);

        return array_shift($this->ready);
    }

    public function size(): int
    {
        return count($this->ready) + count($this->delayed);
    }

    public function delayedSize(): int
    {
        return count($this->delayed);
    }

    public static function restoreFromStorage(JobStorage $storage, ?Clock $clock = null): self
    {
        $queue = new self($clock, $storage);

        foreach ($storage->load() as $data) {
            $job = Job::fromArray($data);
            $state = $job->getState();

            if ($state === JobState::PROCESSING) {
                $job->markRetry($queue->clock->now());
                $queue->ready[] = $job;
                continue;
            }

            if ($state === JobState::READY || $state === JobState::DELAYED) {
                $queue->push($job);
            }
        }

        return $queue;
    }

    private function persist(Job $job): void
    {
        $this->storage?->store($job->getId()->toString(), $job->toArray());
    }

    private function sortDelayed(): void
    {
        usort($this->delayed, static fn (Job $a, Job $b): int => $a->getAvailableAt() <=> $b->getAvailableAt());
    }

    private function promoteDelayed(float $now): void
    {
        while ($this->delayed !== [] && ($this->delayed[0])->getAvailableAt() <= $now) {
            $job = array_shift($this->delayed);
            if ($job->getState() === JobState::DELAYED) {
                $job->markReady($now);
            }
            $this->ready[] = $job;
        }
    }
}
