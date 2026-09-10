<?php

declare(strict_types=1);

namespace App\Job;

/**
 * How urgent a job is - PLAN.md Phase 13.
 *
 * Declaration order IS priority order, highest first, and everything that
 * needs to walk the priorities in order walks JobPriority::cases(). There
 * used to be an order() method returning 0/1/2 alongside it; two sources of
 * truth for one fact, and the enum is the better one.
 */
enum JobPriority
{
    case HIGH;
    case NORMAL;
    case LOW;

    public static function fromName(string $name): self
    {
        return constant("self::$name");
    }
}
