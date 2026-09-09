<?php

declare(strict_types=1);

namespace App\Producer;

use App\Job\Job;
use App\Job\JobPriority;
use App\Support\Clock;
use App\Support\SystemClock;

final class JobFactory
{
    private Clock $clock;

    public function __construct(?Clock $clock = null)
    {
        $this->clock = $clock ?? new SystemClock();
    }

    /**
     * @param array<string, mixed> $payload
     */
    public function create(
        string $type,
        array $payload = [],
        int $maxAttempts = 3,
        JobPriority $priority = JobPriority::NORMAL,
    ): Job {
        return Job::create(
            type: $type,
            payload: $payload,
            maxAttempts: $maxAttempts,
            clock: $this->clock,
            priority: $priority,
        );
    }
}
