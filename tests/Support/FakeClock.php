<?php

declare(strict_types=1);

namespace PhpJobQueue\Tests\Support;

use PhpJobQueue\Support\Clock;

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
