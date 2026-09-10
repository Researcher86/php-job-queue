<?php

declare(strict_types=1);

namespace App\Worker;

use App\Job\Job;
use App\Job\JobResult;

final readonly class WorkerOutcome
{
    public function __construct(
        private Job $job,
        private ?JobResult $result,
        private float $startedAt,
    ) {}

    public function getJob(): Job
    {
        return $this->job;
    }

    public function getResult(): ?JobResult
    {
        return $this->result;
    }

    public function getStartedAt(): float
    {
        return $this->startedAt;
    }
}
