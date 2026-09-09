<?php

declare(strict_types=1);

namespace App\Tests\Support;

use App\Support\Clock;

final class FakeClock implements Clock
{
    private float $time;

    public function __construct(float $startTime = 0.0)
    {
        $this->time = $startTime;
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
