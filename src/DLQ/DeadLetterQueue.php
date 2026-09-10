<?php

declare(strict_types=1);

namespace App\DLQ;

use App\Job\Job;
use App\Job\JobState;
use App\Support\Clock;
use App\Support\SystemClock;
use Throwable;

/**
 * Where jobs go when retrying them has stopped being useful - PLAN.md
 * Phase 10.
 *
 * Without it, a job that can never succeed has two possible endings, and
 * both are bad: retry forever, burning workers on work that will not
 * complete, or drop it, and lose the fact that it existed. The DLQ is the
 * third: stop trying, keep everything, and wait for a human.
 *
 * So a record holds the job, the exception that finished it, the attempt
 * count and the time - enough to answer "what broke, and how often" without
 * going to the logs. And retry() puts a job back once whatever was broken
 * is fixed, which is why this is a queue and not a log file.
 *
 * Keyed by job id, so a job cannot be dead-lettered twice.
 */
final class DeadLetterQueue
{
    /** @var array<string, DeadLetterRecord> */
    private array $records = [];

    public function __construct(
        private readonly Clock $clock = new SystemClock(),
    ) {
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
