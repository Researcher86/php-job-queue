<?php

declare(strict_types=1);

namespace App\Idempotency;

use App\Persistence\JobStorage;

/**
 * Remembers which side effects have already happened, so a redelivered job
 * does not repeat them.
 *
 * The queue cannot promise a job runs exactly once - it cannot tell "the
 * worker died before doing the work" from "the worker did the work and
 * died before saying so", and has to assume the first. So exactly-once has
 * to be built at the other end, by the handler, out of at-least-once
 * delivery plus a key it can check. That is the whole of it: this class is
 * a set of keys.
 *
 * The key must name the OPERATION, not the delivery - "charge order 123",
 * not a job id that changes when the job is recreated. See
 * ChargePaymentJob.
 *
 * Given a JobStorage it survives a restart, which matters because a
 * restart is exactly when a job that already ran comes back: PROCESSING
 * jobs in the log return to READY, side effect and all.
 */
final class IdempotencyGuard
{
    private const string RECORD_KIND = 'idempotency';

    /** @var array<string, true> */
    private array $processed = [];

    public function __construct(private readonly ?JobStorage $storage = null)
    {
        if ($storage === null) {
            return;
        }

        foreach ($storage->load() as $key => $data) {
            if (($data['kind'] ?? null) === self::RECORD_KIND) {
                $this->processed[$key] = true;
            }
        }
    }

    public function isProcessed(string $key): bool
    {
        return isset($this->processed[$key]);
    }

    public function markProcessed(string $key): void
    {
        $this->processed[$key] = true;
        $this->storage?->store($key, ['kind' => self::RECORD_KIND]);
    }

    /** @return list<string> */
    public function getProcessedKeys(): array
    {
        return array_keys($this->processed);
    }
}
