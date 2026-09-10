<?php

declare(strict_types=1);

namespace App\Producer;

use App\Job\Job;
use App\Job\JobPriority;
use App\Queue\Queue;

/**
 * The application's way in - PLAN.md Phase 3.
 *
 * A one-line seam: dispatch a type and a payload, and the job is created
 * and queued. Application code that uses this never touches a Job object, a
 * state, or a queue implementation, which is the whole point - the caller's
 * side of an asynchronous system should be as small as the synchronous call
 * it replaced.
 *
 * It returns the Job anyway, because a test wants to follow the thing it
 * just dispatched.
 */
final readonly class Producer
{
    public function __construct(
        private Queue $queue,
        private JobFactory $jobFactory,
    ) {
    }

    /**
     * @param array<string, mixed> $payload
     */
    public function dispatch(
        string $type,
        array $payload = [],
        int $maxAttempts = 3,
        int $delay = 0,
        JobPriority $priority = JobPriority::NORMAL,
        ?string $idempotencyKey = null,
    ): Job {
        $job = $this->jobFactory->create(
            type: $type,
            payload: $payload,
            maxAttempts: $maxAttempts,
            priority: $priority,
            idempotencyKey: $idempotencyKey,
        );

        $this->queue->push($job, $delay);

        return $job;
    }
}
