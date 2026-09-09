<?php

declare(strict_types=1);

namespace App\Retry;

use App\Job\Job;

final readonly class FixedDelayRetry implements RetryPolicy
{
    public function __construct(
        private int $delay = 1,
    ) {}

    public function nextDelay(Job $job): int
    {
        return $this->delay;
    }
}
