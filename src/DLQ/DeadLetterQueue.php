<?php

declare(strict_types=1);

namespace App\DLQ;

use App\Job\Job;
use App\Job\JobState;
use App\Support\Clock;
use App\Support\SystemClock;
use Throwable;

final class DeadLetterQueue
{
    private Clock $clock;

    /** @var array<string, DeadLetterRecord> */
    private array $records = [];

    public function __construct(?Clock $clock = null)
    {
        $this->clock = $clock ?? new SystemClock();
    }

    public function add(Job $job, Throwable $exception): void
    {
        $this->records[$job->getId()->toString()] = new DeadLetterRecord(
            job: $job,
            exception: $exception,
            attempts: $job->getAttempts(),
            failedAt: $this->clock->now(),
        );
    }

    /** @return list<DeadLetterRecord> */
    public function all(): array
    {
        return array_values($this->records);
    }

    public function size(): int
    {
        return count($this->records);
    }

    public function find(string $jobId): ?DeadLetterRecord
    {
        return $this->records[$jobId] ?? null;
    }

    public function contains(Job $job): bool
    {
        return isset($this->records[$job->getId()->toString()]);
    }

    public function delete(string $jobId): void
    {
        unset($this->records[$jobId]);
    }

    public function retry(string $jobId): ?Job
    {
        $record = $this->records[$jobId] ?? null;
        if ($record === null) {
            return null;
        }

        $job = $record->getJob();
        if ($job->getState() !== JobState::FAILED) {
            return null;
        }

        unset($this->records[$jobId]);
        $job->markRequeued($this->clock->now());

        return $job;
    }
}
