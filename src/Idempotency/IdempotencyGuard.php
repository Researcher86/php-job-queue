<?php

declare(strict_types=1);

namespace App\Idempotency;

use App\Persistence\JobStorage;

/**
 * A set of keys for side effects that have already happened, so that a
 * redelivered job can skip them.
 *
 * The queue cannot promise a job runs exactly once - it cannot tell "the
 * worker died before doing the work" from "the worker did the work and
 * died before saying so", and has to assume the first. So DEDUPLICATION
 * has to happen at the other end, in the handler, out of at-least-once
 * delivery plus a key it can check.
 *
 * The key must name the OPERATION, not the delivery - "charge order 123",
 * not a job id that changes when the job is recreated. See
 * ChargePaymentJob.
 *
 * Given a JobStorage it survives a restart, which matters because a
 * restart is exactly when a job that already ran comes back: PROCESSING
 * jobs in the log return to READY, side effect and all.
 *
 * ## What this is not
 *
 * It is not exactly-once execution, and the gap is worth being precise
 * about. Checking the key and performing the side effect are two steps,
 * and so are the side effect and recording it:
 *
 *   isProcessed()  ->  false
 *   charge         ->  the money has moved
 *   💀              ->  markProcessed() never runs
 *   restart        ->  isProcessed() is still false, so it charges AGAIN
 *
 * IdempotencyTest::testACrashBetweenTheChargeAndItsRecordChargesTwice
 * does exactly that, with a real SIGKILL, and asserts the double charge.
 *
 * Closing the window needs the side effect and its record to COMMIT
 * TOGETHER - one database transaction that both charges and stores the
 * key, or the remote system's own idempotency key so the second call is
 * the one that deduplicates. Both are properties of the thing being
 * charged, not something a queue can hand you. What a queue can do is
 * deliver at least once, say so, and make the seam visible - which is what
 * this class is here for.
 */
final class IdempotencyGuard
{
    private const string RECORD_KIND = 'idempotency';

    /** @var array<string, true> */
    private array $processed = [];

    public function __construct(
        private readonly ?JobStorage $storage = null,
    ) {
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
