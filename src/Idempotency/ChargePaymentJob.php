<?php

declare(strict_types=1);

namespace App\Idempotency;

use App\Job\Job;
use App\Job\JobResult;
use Closure;
use RuntimeException;

/**
 * Demo of an idempotent job handler.
 *
 * A payment may legally be charged only once, but an at-least-once queue can
 * deliver the same logical operation more than once. The handler deduplicates
 * the side effect by idempotency key instead of relying on delivery guarantees.
 */
final class ChargePaymentJob
{
    /**
     * @param Closure(string, float): void $charger executes the real charge
     */
    public function __construct(
        private readonly IdempotencyGuard $guard,
        private readonly Closure $charger,
    ) {}

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