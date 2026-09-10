<?php

declare(strict_types=1);

namespace App\Tests\Support;

use App\Job\Job;
use Closure;

/**
 * The job handlers the tests reuse.
 *
 * Most tests do not care what the handler does - they are about what the
 * queue does around it - and forty-odd of them used to say so with an empty
 * closure body. A name says it better: `Handlers::succeeds()` states the
 * one thing about the handler that the test depends on.
 */
final class Handlers
{
    /** Succeeds at once, without doing anything. */
    public static function succeeds(): Closure
    {
        return static function (Job $job): void {
            // Deliberately nothing. The test is about the queue, not the work.
        };
    }
}
