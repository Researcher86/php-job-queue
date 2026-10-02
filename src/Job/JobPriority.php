<?php

declare(strict_types=1);

namespace PhpJobQueue\Job;

use ValueError;

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

    /** Rebuilds a case from the name toArray() wrote - see JobState::fromName(). */
    public static function fromName(string $name): self
    {
        return array_find(self::cases(), static fn (self $case): bool => $case->name === $name)
            ?? throw new ValueError(sprintf('"%s" is not a valid job priority', $name));
    }
}
