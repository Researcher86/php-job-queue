<?php

declare(strict_types=1);

namespace App\Producer;

use App\Job\Job;
use App\Job\JobPriority;
use App\Metrics\MetricsCollector;
use App\Support\Clock;
use App\Support\SystemClock;

/**
 * The one place new jobs are born - PLAN.md Phase 3.
 *
 * It exists so that the clock and the metrics counter are decided once, at
 * wiring time, rather than at every call site that wants a job. It is also
 * why "jobs created" is counted here and not in Producer: a job is created
 * exactly once, here, whichever route it takes to a queue afterwards.
 */
final class JobFactory
{
    private Clock $clock;

    public function __construct(?Clock $clock = null, private readonly ?MetricsCollector $metrics = null)
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
        ?string $idempotencyKey = null,
    ): Job {
        $this->metrics?->increment(MetricsCollector::JOBS_CREATED);

        return Job::create(
            type: $type,
            payload: $payload,
            maxAttempts: $maxAttempts,
            clock: $this->clock,
            priority: $priority,
            idempotencyKey: $idempotencyKey,
        );
    }
}
