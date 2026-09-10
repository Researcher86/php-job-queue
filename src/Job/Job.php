<?php

declare(strict_types=1);

namespace App\Job;

use App\Support\Clock;
use App\Support\SystemClock;
use LogicException;

/**
 * One unit of asynchronous work, and its position in its own lifecycle.
 *
 * The state is not a label someone sets from outside - every change goes
 * through one of the mark*() methods, and each of those is one entry in
 * TRANSITIONS. Anything not in the table throws. That is the difference
 * between a job that HAS a state and a job that IS in a state: there is no
 * path by which a COMPLETED job quietly becomes PROCESSING again.
 *
 *                  ┌─────────┐
 *                  │ CREATED │
 *                  └────┬────┘
 *          markDelayed()│  │markReady()
 *                  ┌────▼──┐│
 *                  │DELAYED││   deadline passes
 *                  └────┬──┘│   (markReady)
 *                       └───┤
 *                      ┌────▼───┐
 *              ┌──────►│ READY  │◄──────────┐
 *              │       └────┬───┘           │
 *              │            │markProcessing()   markRequeued()
 *              │       ┌────▼───────┐       │   (a DLQ record, by hand)
 *              │       │ PROCESSING │       │
 *              │       └──┬──┬──┬───┘       │
 *  markRetry() │          │  │  │           │
 *  (attempts   └──────────┘  │  └───────────┼──┐
 *   left, or a              │markCompleted()│  │markFailed()
 *   visibility          ┌───▼──────┐    ┌───┴──▼─┐
 *   timeout)            │COMPLETED │    │ FAILED │
 *                       └──────────┘    └────────┘
 *                        (terminal)      (terminal, and where the
 *                                         DLQ record comes from)
 *
 * Two arrows into READY that look like one and are not:
 *
 *  - markRetry() is PROCESSING -> READY, and it is what both a NACK with
 *    attempts left and an expired visibility timeout do. The job never
 *    reached a terminal state.
 *  - markRequeued() is FAILED -> READY, which only happens when a human
 *    retries a dead-letter record. A job that went to the DLQ is finished
 *    unless somebody decides otherwise.
 *
 * ## Attempts count deliveries
 *
 * markProcessing() is what increments attempts, so an attempt is a
 * DELIVERY rather than a run: a job handed to a worker that died before
 * starting has still used one. That is the honest accounting for an
 * at-least-once system, and it is what real queues count too.
 *
 * ## The object is mutable, and the copy the worker sees is not
 *
 * A job crosses a socket as JSON (toArray/fromArray) and is rebuilt inside
 * the worker. The worker mutates its own copy; nothing it does to that
 * copy comes back. What comes back is a JobResult, and the ACK is what
 * moves the original.
 */
final class Job
{
    /**
     * The whole lifecycle, stated once: event => (state it is legal from
     * => state it leads to). Anything absent throws.
     *
     * Keyed by ->name because enum cases cannot be array keys. Written as a
     * table rather than seven hand-rolled guards so that adding a state
     * means editing one place and immediately seeing every event it has to
     * answer for.
     *
     *   FROM         ready     delay     dispatch    complete   fail     retry   requeue
     *   ──────────────────────────────────────────────────────────────────────────────────
     *   CREATED      READY     DELAYED   -           -          -        -       -
     *   DELAYED      READY     -         -           -          -        -       -
     *   READY        -         -          PROCESSING -          -        -       -
     *   PROCESSING   -         -         -           COMPLETED  FAILED   READY   -
     *   COMPLETED    -         -         -           -          -        -       -
     *   FAILED       -         -         -           -          -        -       READY
     *
     * @var array<string, array<string, JobState>>
     */
    private const array TRANSITIONS = [
        'ready' => [
            'CREATED' => JobState::READY,
            'DELAYED' => JobState::READY,
        ],
        'delay' => [
            'CREATED' => JobState::DELAYED,
        ],
        'dispatch' => [
            'READY' => JobState::PROCESSING,
        ],
        'complete' => [
            'PROCESSING' => JobState::COMPLETED,
        ],
        'fail' => [
            'PROCESSING' => JobState::FAILED,
        ],
        'retry' => [
            'PROCESSING' => JobState::READY,
        ],
        'requeue' => [
            'FAILED' => JobState::READY,
        ],
    ];

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
        private readonly ?string $idempotencyKey,
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
        ?string $idempotencyKey = null,
    ): self {
        $clock ??= new SystemClock();

        return new self(
            id: JobId::generate(),
            type: $type,
            payload: $payload,
            maxAttempts: $maxAttempts,
            createdAt: $clock->now(),
            priority: $priority,
            idempotencyKey: $idempotencyKey,
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

    public function getIdempotencyKey(): ?string
    {
        return $this->idempotencyKey;
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
            'idempotencyKey' => $this->idempotencyKey,
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
            idempotencyKey: isset($data['idempotencyKey']) ? (string) $data['idempotencyKey'] : null,
            state: JobState::fromName((string) $data['state']),
            attempts: (int) ($data['attempts'] ?? 0),
            availableAt: isset($data['availableAt']) ? (float) $data['availableAt'] : null,
        );
    }

    /** Available for dispatch as of $now. */
    public function markReady(float $now): void
    {
        $this->apply('ready');
        $this->availableAt = $now;
    }

    /** Not available until $availableAt - PLAN.md Phase 8. */
    public function markDelayed(float $availableAt): void
    {
        $this->apply('delay');
        $this->availableAt = $availableAt;
    }

    /**
     * Handed to a worker. Consumes an attempt - see the class docblock on
     * why an attempt is a delivery - and clears availableAt, because a job
     * in a worker's hands is not waiting for anything.
     */
    public function markProcessing(): void
    {
        $this->apply('dispatch');
        $this->attempts++;
        $this->availableAt = null;
    }

    /** ACK. */
    public function markCompleted(): void
    {
        $this->apply('complete');
    }

    /** NACK with no attempts left - the end of the road, and the DLQ's input. */
    public function markFailed(): void
    {
        $this->apply('fail');
    }

    /**
     * Back to READY from the dead letter queue, because somebody decided to
     * try again - PLAN.md Phase 10. Not to be confused with markRetry().
     */
    public function markRequeued(float $availableAt): void
    {
        $this->apply('requeue');
        $this->availableAt = $availableAt;
    }

    /**
     * Back to READY without ever finishing: a NACK with attempts left, or a
     * visibility timeout that expired. $availableAt is now for an immediate
     * retry, or later for one under a backoff policy - which is why a retry
     * and a delayed job go through the same scheduler.
     */
    public function markRetry(float $availableAt): void
    {
        $this->apply('retry');
        $this->availableAt = $availableAt;
    }

    /**
     * Applies $event per TRANSITIONS, or throws if this state has no answer
     * for it. The single gate every state change goes through - there is no
     * other assignment to $this->state after construction.
     */
    private function apply(string $event): void
    {
        $next = self::TRANSITIONS[$event][$this->state->name] ?? null;

        if ($next === null) {
            throw new LogicException(sprintf(
                'Invalid transition: cannot %s a job in state %s',
                $event,
                $this->state->name,
            ));
        }

        $this->state = $next;
    }
}
