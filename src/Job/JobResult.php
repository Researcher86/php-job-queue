<?php

declare(strict_types=1);

namespace App\Job;

use Throwable;

/**
 * What a handler decided: ACK or NACK - PLAN.md Phase 6.
 *
 * Constructed only through success() and failure(), so a failure always
 * carries its exception. "It failed" without a reason is not a state the
 * DLQ could do anything with.
 *
 * Note what this cannot express: a worker that died. That is why a worker
 * reports a WorkerOutcome, whose result may be null - see WorkerOutcome.
 */
final readonly class JobResult
{
    private function __construct(
        private bool $success,
        private ?Throwable $exception = null,
    ) {}

    public static function success(): self
    {
        return new self(true);
    }

    public static function failure(Throwable $exception): self
    {
        return new self(false, $exception);
    }

    public function isSuccess(): bool
    {
        return $this->success;
    }

    public function getException(): ?Throwable
    {
        return $this->exception;
    }
}
