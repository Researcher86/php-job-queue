<?php

declare(strict_types=1);

namespace App\Support;

interface Clock
{
    public function now(): float;
}
