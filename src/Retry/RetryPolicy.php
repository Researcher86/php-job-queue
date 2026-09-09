<?php

declare(strict_types=1);

namespace App\Retry;

use App\Job\Job;

interface RetryPolicy
{
    public function nextDelay(Job $job): int;
}
