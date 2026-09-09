<?php

declare(strict_types=1);

namespace App\Job;

use Throwable;

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
