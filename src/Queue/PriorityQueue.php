<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\Job;
use App\Job\JobPriority;
use App\Job\JobState;
use App\Support\Clock;
use App\Support\SystemClock;

final class PriorityQueue implements Queue
{
    private Clock $clock;

    /** @var array<string, list<Job>> */
    private array $ready = [];

    /** @var list<Job> */
    private array $delayed = [];

    public function __construct(?Clock $clock = null)
    {
        $this->clock = $clock ?? new SystemClock();
        $this->ready = [
            JobPriority::HIGH->name => [],
            JobPriority::NORMAL->name => [],
            JobPriority::LOW->name => [],
        ];
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

        if ($job->getAvailableAt() !== null && $job->getAvailableAt() > $this->clock->now()) {
            $this->delayed[] = $job;
            $this->sortDelayed();
            return;
        }

        if ($state === JobState::DELAYED) {
            $job->markReady($this->clock->now());
        }

        $this->ready[$job->getPriority()->name][] = $job;
    }

    public function pop(?float $now = null): ?Job
    {
        $now ??= $this->clock->now();
        $this->promoteDelayed($now);

        foreach ([JobPriority::HIGH, JobPriority::NORMAL, JobPriority::LOW] as $priority) {
            if ($this->ready[$priority->name] !== []) {
                return array_shift($this->ready[$priority->name]);
            }
        }

        return null;
    }

    public function size(): int
    {
        return array_sum(array_map('count', $this->ready)) + count($this->delayed);
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
            $this->ready[$job->getPriority()->name][] = $job;
        }
    }
}
