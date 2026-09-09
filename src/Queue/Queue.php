<?php

declare(strict_types=1);

namespace App\Queue;

use App\Job\Job;

interface Queue
{
    public function push(Job $job): void;

    public function pop(): ?Job;

    public function size(): int;
}
