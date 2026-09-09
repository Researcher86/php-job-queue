<?php

declare(strict_types=1);

namespace App\Producer;

use App\Job\Job;
use App\Queue\Queue;

final readonly class Producer
{
    public function __construct(
        private Queue $queue,
        private JobFactory $jobFactory,
    ) {}

    /**
     * @param array<string, mixed> $payload
     */
    public function dispatch(
        string $type,
        array $payload = [],
        int $maxAttempts = 3,
    ): Job {
        $job = $this->jobFactory->create(
            type: $type,
            payload: $payload,
            maxAttempts: $maxAttempts,
        );

        $this->queue->push($job);

        return $job;
    }
}
