<?php

declare(strict_types=1);

namespace App\Job;

enum JobState
{
    case CREATED;
    case DELAYED;
    case READY;
    case PROCESSING;
    case COMPLETED;
    case FAILED;

    public function isTerminal(): bool
    {
        return match ($this) {
            self::COMPLETED, self::FAILED => true,
            default => false,
        };
    }
}
