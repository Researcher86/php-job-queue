<?php

declare(strict_types=1);

namespace App\Retry;

use App\Job\Job;

final readonly class ExponentialBackoffRetry implements RetryPolicy
{
    public function __construct(
        private int $baseDelay = 1,
        private float $factor = 2.0,
    ) {}

    public function nextDelay(Job $job): int
    {
        $attempt = max($job->getAttempts(), 1);

        return (int) ($this->baseDelay * ($this->factor ** ($attempt - 1)));
    }
}
