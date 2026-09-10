<?php

declare(strict_types=1);

namespace App\Job;

use ValueError;

/**
 * Where a job is in its lifecycle - PLAN.md Phase 1. The transitions
 * between these live in Job::TRANSITIONS, which is the only thing allowed
 * to move a job from one to another.
 */
enum JobState
{
    case CREATED;
    case DELAYED;
    case READY;
    case PROCESSING;
    case COMPLETED;
    case FAILED;

    /**
     * Whether the job is finished, for better or worse. Terminal states are
     * not restored from the persistence log - a finished job is not work.
     */
    public function isTerminal(): bool
    {
        return match ($this) {
            self::COMPLETED, self::FAILED => true,
            default => false,
        };
    }

    /**
     * Rebuilds a case from the name toArray() wrote.
     *
     * A pure enum has no from()/tryFrom(), so this is the equivalent. It
     * used to be constant("self::$name"), which would resolve any constant
     * on the class - a persistence log with an unexpected value in it
     * deserves a ValueError, not a lookup.
     */
    public static function fromName(string $name): self
    {
        foreach (self::cases() as $case) {
            if ($case->name === $name) {
                return $case;
            }
        }

        throw new ValueError(sprintf('"%s" is not a valid job state', $name));
    }
}
