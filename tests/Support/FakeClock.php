<?php

declare(strict_types=1);

namespace App\Tests\Support;

use App\Support\Clock;

final class FakeClock implements Clock
{
    public function __construct(
        private float $time = 0.0,
    ) {
    }

    public function now(): float
    {
        return $this->time;
    }

    public function advance(float $seconds): void
    {
        $this->time += $seconds;
    }
}
