<?php

declare(strict_types=1);

namespace App\Retry;

use App\Job\Job;

/**
 * The same delay every time: 1s, 1s, 1s.
 *
 * Right when failures are independent - a lost packet, one bad row - and
 * wrong when they are not: a dependency that is down stays down, and a
 * hundred jobs retrying every second are a hundred requests a second
 * against something already failing. That is what ExponentialBackoffRetry
 * is for.
 */
final readonly class FixedDelayRetry implements RetryPolicy
{
    public function __construct(
        private int $delay = 1,
    ) {
    }

    public function nextDelay(Job $job): int
    {
        return $this->delay;
    }
}
