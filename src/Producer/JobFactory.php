<?php

declare(strict_types=1);

namespace PhpJobQueue\Producer;

use PhpJobQueue\Job\Job;
use PhpJobQueue\Job\JobPriority;
use PhpJobQueue\Metrics\MetricsCollector;
use PhpJobQueue\Support\Clock;
use PhpJobQueue\Support\SystemClock;

/**
 * The one place new jobs are born - PLAN.md Phase 3.
 *
 * It exists so that the clock and the metrics counter are decided once, at
 * wiring time, rather than at every call site that wants a job. It is also
 * why "jobs created" is counted here and not in Producer: a job is created
 * exactly once, here, whichever route it takes to a queue afterwards.
 */
final readonly class JobFactory
{
    public function __construct(
        private Clock $clock = new SystemClock(),
        private ?MetricsCollector $metrics = null,
    ) {
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
