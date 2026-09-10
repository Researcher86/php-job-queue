<?php

declare(strict_types=1);

namespace App\DLQ;

use App\Job\Job;
use Throwable;

/**
 * One dead-lettered job, with everything needed to understand why.
 *
 * The attempt count is copied rather than read from the job on demand,
 * because the job is mutable and retry() puts it back into circulation: the
 * record has to keep saying "this failed three times" after the fourth
 * attempt starts.
 */
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
