<?php

declare(strict_types=1);

namespace App\Retry;

use App\Job\Job;

/**
 * Doubling delays: 1s, 2s, 4s, 8s - PLAN.md Phase 7.
 *
 * The delay grows with the attempt count, so a job that keeps failing asks
 * less and less often. Which is the useful behaviour when the reason it
 * fails is that something else is overloaded: the retries themselves stop
 * being part of the problem.
 *
 * No jitter here, deliberately - it would be the right thing in production
 * (a thousand jobs failing together retry together, and the pattern
 * repeats at every doubling) and it would make the delays unpredictable to
 * read in an example. This is a project for reading.
 */
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
