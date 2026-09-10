<?php

declare(strict_types=1);

namespace App\Idempotency;

use App\Job\Job;
use App\Job\JobResult;
use Closure;
use RuntimeException;

/**
 * A handler that deduplicates its own side effect - the worked example for
 * PLAN.md's engineering question 5.
 *
 * A payment may legally be charged once, and an at-least-once queue will
 * sometimes deliver the same operation twice: a worker that crashed after
 * charging, or a handler that outlived its visibility deadline. So the
 * handler cannot rely on the delivery count. It checks a key that names the
 * OPERATION - "payment:order-7" - and does nothing if that key has already
 * been seen.
 *
 * Note what makes the key usable: it comes from the job's payload-level
 * idempotencyKey, set by the producer, so it is the same string on every
 * redelivery. A key derived from the job id or the attempt number would be
 * different each time and would deduplicate nothing.
 *
 * This is deduplication, not exactly-once execution - see IdempotencyGuard
 * for the crash window that remains and what actually closes it.
 */
final readonly class ChargePaymentJob
{
    /**
     * @param Closure(string, float): void $charger executes the real charge
     */
    public function __construct(
        private IdempotencyGuard $guard,
        private Closure $charger,
    ) {
    }

    public function __invoke(Job $job): JobResult
    {
        $orderId = (string) ($job->getPayload()['order_id'] ?? '');
        $amount = (float) ($job->getPayload()['amount'] ?? 0.0);
        $key = $job->getIdempotencyKey();

        if ($key === null) {
            throw new RuntimeException(sprintf(
                'ChargePaymentJob requires an idempotency key, got none for job %s',
                $job->getId(),
            ));
        }

        if ($this->guard->isProcessed($key)) {
            return JobResult::success();
        }

        ($this->charger)($orderId, $amount);
        $this->guard->markProcessed($key);

        return JobResult::success();
    }
}
