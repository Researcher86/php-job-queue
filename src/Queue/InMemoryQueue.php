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
    private array $jobs = [];

    public function __construct(?Clock $clock = null)
    {
        $this->clock = $clock ?? new SystemClock();
    }

    public function push(Job $job): void
    {
        if ($job->getState() === JobState::CREATED) {
            $job->markReady($this->clock->now());
        }

        $this->jobs[] = $job;
    }

    public function pop(): ?Job
    {
        return array_shift($this->jobs);
    }

    public function size(): int
    {
        return count($this->jobs);
    }
}
