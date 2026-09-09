<?php

declare(strict_types=1);

namespace App\DLQ;

use App\Job\Job;
use Throwable;

final readonly class DeadLetterRecord
{
    public function __construct(
        private Job $job,
        private Throwable $exception,
        private int $attempts,
        private float $failedAt,
    ) {}

    public function getJob(): Job
    {
        return $this->job;
    }

    public function getException(): Throwable
    {
        return $this->exception;
    }

    public function getAttempts(): int
    {
        return $this->attempts;
    }

    public function getFailedAt(): float
    {
        return $this->failedAt;
    }
}
