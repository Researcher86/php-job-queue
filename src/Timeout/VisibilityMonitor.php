<?php

declare(strict_types=1);

namespace App\Timeout;

use App\Job\Job;
use App\Support\Clock;
use App\Support\SystemClock;

final class VisibilityMonitor
{
    private Clock $clock;

    private int $timeout;

    /** @var array<string, float> */
    private array $deadlines = [];

    /** @var array<string, Job> */
    private array $processing = [];

    public function __construct(int $timeout, ?Clock $clock = null)
    {
        $this->timeout = $timeout;
        $this->clock = $clock ?? new SystemClock();
    }

    public function track(Job $job): void
    {
        $id = $job->getId()->toString();
        $this->deadlines[$id] = $this->clock->now() + $this->timeout;
        $this->processing[$id] = $job;
    }

    public function release(Job $job): void
    {
        $id = $job->getId()->toString();
        unset($this->deadlines[$id], $this->processing[$id]);
    }

    public function isProcessing(Job $job): bool
    {
        return isset($this->processing[$job->getId()->toString()]);
    }

    /** @return list<Job> */
    public function requeueExpired(?float $now = null): array
    {
        $now ??= $this->clock->now();

        $expired = [];
        foreach ($this->processing as $id => $job) {
            if (($this->deadlines[$id] ?? INF) <= $now) {
                $job->markRetry($now);
                $expired[] = $job;
                unset($this->deadlines[$id], $this->processing[$id]);
            }
        }

        return $expired;
    }
}
