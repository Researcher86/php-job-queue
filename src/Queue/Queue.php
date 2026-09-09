<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\Job;

interface Queue
{
    public function push(Job $job, int $delay = 0): void;

    public function pop(?float $now = null): ?Job;

    public function size(): int;
}
