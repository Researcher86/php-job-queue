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
        private readonly JobPriority $priority,
        JobState $state,
        int $attempts = 0,
        ?float $availableAt = null,
    ) {
        $this->state = $state;
        $this->attempts = $attempts;
        $this->availableAt = $availableAt;
    }

    /**
     * @param array<string, mixed> $payload
     */
    public static function create(
        string $type,
        array $payload = [],
        int $maxAttempts = 3,
        ?Clock $clock = null,
        JobPriority $priority = JobPriority::NORMAL,
    ): self {
        $clock ??= new SystemClock();

        return new self(
            id: JobId::generate(),
            type: $type,
            payload: $payload,
            maxAttempts: $maxAttempts,
            createdAt: $clock->now(),
            priority: $priority,
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

    public function getPriority(): JobPriority
    {
        return $this->priority;
    }

    public function getCreatedAt(): float
    {
        return $this->createdAt;
    }

    public function getAvailableAt(): ?float
    {
        return $this->availableAt;
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return [
            'id' => $this->id->toString(),
            'type' => $this->type,
            'payload' => $this->payload,
            'state' => $this->state->name,
            'attempts' => $this->attempts,
            'maxAttempts' => $this->maxAttempts,
            'createdAt' => $this->createdAt,
            'availableAt' => $this->availableAt,
            'priority' => $this->priority->name,
        ];
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            id: JobId::fromString((string) $data['id']),
            type: (string) $data['type'],
            payload: (array) $data['payload'],
            maxAttempts: (int) $data['maxAttempts'],
            createdAt: (float) $data['createdAt'],
            priority: JobPriority::fromName((string) ($data['priority'] ?? 'NORMAL')),
            state: JobState::fromName((string) $data['state']),
            attempts: (int) $data['attempts'],
            availableAt: $data['availableAt'] !== null ? (float) $data['availableAt'] : null,
        );
    }

    public function markReady(float $now): void
    {
        if ($this->state !== JobState::CREATED && $this->state !== JobState::DELAYED) {
            throw $this->illegalTransition(JobState::READY);
        }
        $this->state = JobState::READY;
        $this->availableAt = $now;
    }

    public function markDelayed(float $availableAt): void
    {
        if ($this->state !== JobState::CREATED) {
            throw $this->illegalTransition(JobState::DELAYED);
        }
        $this->state = JobState::DELAYED;
        $this->availableAt = $availableAt;
    }

    public function markProcessing(): void
    {
        if ($this->state !== JobState::READY) {
            throw $this->illegalTransition(JobState::PROCESSING);
        }
        $this->state = JobState::PROCESSING;
        $this->attempts++;
        $this->availableAt = null;
    }

    public function markCompleted(): void
    {
        if ($this->state !== JobState::PROCESSING) {
            throw $this->illegalTransition(JobState::COMPLETED);
        }
        $this->state = JobState::COMPLETED;
    }

    public function markFailed(): void
    {
        if ($this->state !== JobState::PROCESSING) {
            throw $this->illegalTransition(JobState::FAILED);
        }
        $this->state = JobState::FAILED;
    }

    public function markRequeued(float $availableAt): void
    {
        if ($this->state !== JobState::FAILED) {
            throw $this->illegalTransition(JobState::READY);
        }
        $this->state = JobState::READY;
        $this->availableAt = $availableAt;
    }

    public function markRetry(float $availableAt): void
    {
        if ($this->state !== JobState::PROCESSING) {
            throw $this->illegalTransition(JobState::READY);
        }
        $this->state = JobState::READY;
        $this->availableAt = $availableAt;
    }

    private function illegalTransition(JobState $target): LogicException
    {
        return new LogicException(sprintf(
            'Invalid transition: cannot move job from %s to %s',
            $this->state->name,
            $target->name,
        ));
    }
}
