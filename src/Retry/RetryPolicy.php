<?php

declare(strict_types=1);

namespace App\Retry;

use App\Job\Job;

/**
 * How long to wait before trying a failed job again - PLAN.md Phase 7.
 *
 * An interface with two implementations because the choice matters and has
 * no universally right answer: a fixed delay is predictable, and a
 * backoff stops a queue of failing jobs from hammering whatever is already
 * struggling. Passing no policy at all means retrying immediately, which
 * is the fastest way to turn one broken dependency into a busy loop.
 */
interface RetryPolicy
{
    public function nextDelay(Job $job): int;
}
