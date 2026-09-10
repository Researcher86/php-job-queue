<?php

declare(strict_types=1);

namespace App\Job;

enum JobPriority
{
    case HIGH;
    case NORMAL;
    case LOW;

    public function order(): int
    {
        return match ($this) {
            self::HIGH => 0,
            self::NORMAL => 1,
            self::LOW => 2,
        };
    }

    public static function fromName(string $name): self
    {
        return constant("self::$name");
    }
}
