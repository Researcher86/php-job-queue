<?php

declare(strict_types=1);

namespace App\Support;

/**
 * The current time, as an injectable thing.
 *
 * Everything about a queue is time: deadlines, delays, backoff, timeouts,
 * latency. Reading the clock through an interface is what lets a test
 * advance sixty seconds instantly and assert on what the system does, and
 * what keeps the tests for delayed jobs and visibility timeouts from being
 * sleep() calls that fail on a loaded machine.
 *
 * One exception, marked where it happens: the shutdown grace period is
 * measured against the wall clock, because it is how long a real forked
 * process gets to finish and no amount of faking time makes it faster.
 */
interface Clock
{
    public function now(): float;
}
