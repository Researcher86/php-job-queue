<?php

declare(strict_types=1);

namespace App\Job;

use App\Support\Clock;
use App\Support\SystemClock;
use LogicException;

final class Job
{
    private JobState $state;

    private int $attempts;

    private ?float $availableAt;

    /**
     * @param array<string, mixed> $payload
     */
    private function __construct(
        private readonly JobId $id,
        private readonly string $type,
        private readonly array $payload,
        private readonly int $maxAttempts,
        private readonly float $createdAt,
        JobState $state,
    ) {
        $this->state = $state;
        $this->attempts = 0;
        $this->availableAt = null;
    }

    /**
     * @param array<string, mixed> $payload
     */
    public static function create(
        string $type,
        array $payload = [],
        int $maxAttempts = 3,
        ?Clock $clock = null,
    ): self {
        $clock ??= new SystemClock();

        return new self(
            id: JobId::generate(),
            type: $type,
            payload: $payload,
            maxAttempts: $maxAttempts,
            createdAt: $clock->now(),
            state: JobState::CREATED,
        );
    }

    public function getId(): JobId
    {
        return $this->id;
    }

    public function getType(): string
    {
        return $this->type;
    }

    /** @return array<string, mixed> */
    public function getPayload(): array
    {
        return $this->payload;
    }

    public function getState(): JobState
    {
        return $this->state;
    }

    public function getAttempts(): int
    {
        return $this->attempts;
    }

    public function getMaxAttempts(): int
    {
        return $this->maxAttempts;
    }

    public function getCreatedAt(): float
    {
        return $this->createdAt;
    }

    public function getAvailableAt(): ?float
    {
        return $this->availableAt;
    }

    public function markReady(float $now): void
    {
        $this->assertTransition(JobState::READY);
        $this->state = JobState::READY;
        $this->availableAt = $now;
    }

    public function markProcessing(): void
    {
        $this->assertTransition(JobState::PROCESSING);
        $this->state = JobState::PROCESSING;
        $this->attempts++;
        $this->availableAt = null;
    }

    public function markCompleted(): void
    {
        $this->assertTransition(JobState::COMPLETED);
        $this->state = JobState::COMPLETED;
    }

    public function markFailed(): void
    {
        $this->assertTransition(JobState::FAILED);
        $this->state = JobState::FAILED;
    }

    private function assertTransition(JobState $target): void
    {
        $allowed = match ($this->state) {
            JobState::CREATED => [JobState::READY],
            JobState::READY => [JobState::PROCESSING],
            JobState::PROCESSING => [JobState::COMPLETED, JobState::FAILED],
            default => [],
        };

        if (!in_array($target, $allowed, true)) {
            throw new LogicException(sprintf(
                'Invalid transition: cannot move job from %s to %s',
                $this->state->name,
                $target->name,
            ));
        }
    }
}
