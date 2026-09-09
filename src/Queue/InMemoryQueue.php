<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\Job;
use App\Job\JobState;
use App\Support\Clock;
use App\Support\SystemClock;

final class InMemoryQueue implements Queue
{
    private Clock $clock;

    /** @var list<Job> */
    private array $ready = [];

    /** @var list<Job> */
    private array $delayed = [];

    public function __construct(?Clock $clock = null)
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
            return;
        }

        $availableAt = $job->getAvailableAt();
        if ($availableAt !== null && $availableAt > $this->clock->now()) {
            $this->delayed[] = $job;
            $this->sortDelayed();
            return;
        }

        $this->ready[] = $job;
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
