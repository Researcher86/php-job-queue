<?php

declare(strict_types=1);

namespace PhpJobQueue\Idempotency;

use PhpJobQueue\Persistence\JobStorage;

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
 * ## Across workers
 *
 * Each forked worker has its own copy of this object, so the keys it
 * holds in memory are only ever its own. What the workers share is the
 * storage, and isProcessed() goes back to it on a miss - which is what
 * lets a key recorded by one worker stop a duplicate on another within
 * the same run. That needs a storage the processes actually share: a
 * FileStorage. An InMemoryStorage is copied by the fork like everything
 * else, and dedups within one worker only.
 *
 * Give the guard its OWN storage, not the queue's. The queue replays
 * every record in its log as a job, and a guard record is not one -
 * InMemoryQueue::restoreFromStorage() refuses such a log rather than
 * guess.
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
 * Two workers holding duplicates of one job AT THE SAME TIME fall through
 * the same gap without any crash: both check, both miss, both charge.
 * Nothing here locks the key between the check and the record.
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
        $this->reload();
    }

    /**
     * A miss is checked against the storage before it is believed.
     *
     * The keys loaded at construction are a snapshot, and after the fork
     * every worker holds its own copy of it: a key worker A records lands
     * in the shared log, but not in worker B's array. Trusting the array
     * alone made deduplication work across restarts and NOT across workers
     * in the same run - a duplicate delivery that happened to land on a
     * different worker charged again. A hit needs no re-read: keys are
     * never removed.
     *
     * The re-read is a full load() of the log, on every miss. That is the
     * price of a log-only store with no index, paid on the path that is
     * about to do the expensive, non-repeatable thing anyway.
     */
    public function isProcessed(string $key): bool
    {
        if (isset($this->processed[$key])) {
            return true;
        }

        $this->reload();

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

    /** Picks up every key in the storage, other processes' writes included. */
    private function reload(): void
    {
        foreach ($this->storage?->load() ?? [] as $key => $data) {
            if (($data['kind'] ?? null) === self::RECORD_KIND) {
                $this->processed[$key] = true;
            }
        }
    }
}
