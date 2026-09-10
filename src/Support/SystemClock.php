<?php

declare(strict_types=1);

namespace App\Support;

/** The real clock. microtime(true), because sub-second matters here. */
final readonly class SystemClock implements Clock
{
    public function now(): float
    {
        return microtime(true);
    }
}
