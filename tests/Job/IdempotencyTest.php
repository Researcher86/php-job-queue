<?php

declare(strict_types=1);

namespace App\Tests\Job;

use App\Dispatcher\JobDispatcher;
use App\Idempotency\ChargePaymentJob;
use App\Idempotency\IdempotencyGuard;
use App\Job\Job;
use App\Job\JobState;
use App\Persistence\FileStorage;
use App\Producer\JobFactory;
use App\Producer\Producer;
use App\Queue\InMemoryQueue;
use App\Tests\Support\FakeClock;
use App\Worker\WorkerPool;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class IdempotencyTest extends TestCase
{
    public function testJobPreservesIdempotencyKey(): void
    {
        $job = Job::create(type: 'charge_payment', idempotencyKey: 'payment:order-1');

        $this->assertSame('payment:order-1', $job->getIdempotencyKey());
    }

    public function testJobIdempotencyKeyDefaultsToNull(): void
    {
        $job = Job::create(type: 'charge_payment');

        $this->assertNull($job->getIdempotencyKey());
    }

    public function testIdempotencyKeyRoundTripsThroughArray(): void
    {
        $job = Job::create(type: 'charge_payment', idempotencyKey: 'payment:order-1');
        $job->markReady(1000.0);

        $restored = Job::fromArray($job->toArray());

        $this->assertSame('payment:order-1', $restored->getIdempotencyKey());
    }

    public function testLegacyArrayWithoutIdempotencyKeyRestoresAsNull(): void
    {
        $job = Job::create(type: 'charge_payment');
        $data = $job->toArray();
        unset($data['idempotencyKey']);

        $restored = Job::fromArray($data);

        $this->assertNull($restored->getIdempotencyKey());
    }

    public function testProducerThreadsIdempotencyKey(): void
    {
        $queue = new InMemoryQueue(new FakeClock());
        $producer = new Producer($queue, new JobFactory(new FakeClock()));

        $job = $producer->dispatch(type: 'charge_payment', idempotencyKey: 'payment:order-1');

        $this->assertSame('payment:order-1', $job->getIdempotencyKey());
    }

    public function testGuardDetectsProcessedKey(): void
    {
        $guard = new IdempotencyGuard();

        $this->assertFalse($guard->isProcessed('payment:order-1'));

        $guard->markProcessed('payment:order-1');

        $this->assertTrue($guard->isProcessed('payment:order-1'));
        $this->assertSame(['payment:order-1'], $guard->getProcessedKeys());
    }

    public function testGuardPersistsAcrossInstances(): void
    {
        $path = sys_get_temp_dir() . '/php-job-queue-idempotency-' . uniqid('', true) . '.log';

        try {
            $storage = new FileStorage($path);
            $guard = new IdempotencyGuard($storage);
            $guard->markProcessed('payment:order-1');

            $restored = new IdempotencyGuard($storage);

            $this->assertTrue($restored->isProcessed('payment:order-1'));
        } finally {
            if (file_exists($path)) {
                unlink($path);
            }
        }
    }

    public function testChargesOnlyOncePerIdempotencyKey(): void
    {
        $charges = [];
        $chargeJob = new ChargePaymentJob(
            new IdempotencyGuard(),
            static function (string $orderId, float $amount) use (&$charges): void {
                $charges[] = ['order_id' => $orderId, 'amount' => $amount];
            },
        );

        $job = Job::create(
            type: 'charge_payment',
            payload: ['order_id' => 'order-1', 'amount' => 19.99],
            idempotencyKey: 'payment:order-1',
        );

        $chargeJob($job);
        $chargeJob($job);

        $this->assertCount(1, $charges);
        $this->assertSame(19.99, $charges[0]['amount']);
    }

    public function testDifferentIdempotencyKeysChargeSeparately(): void
    {
        $charges = [];
        $chargeJob = new ChargePaymentJob(
            new IdempotencyGuard(),
            static function (string $orderId, float $amount) use (&$charges): void {
                $charges[] = $orderId;
            },
        );

        $chargeJob(Job::create(type: 'charge_payment', payload: ['order_id' => 'order-1'], idempotencyKey: 'payment:order-1'));
        $chargeJob(Job::create(type: 'charge_payment', payload: ['order_id' => 'order-2'], idempotencyKey: 'payment:order-2'));

        $this->assertSame(['order-1', 'order-2'], $charges);
        $this->assertCount(2, $charges);
    }

    public function testMissingIdempotencyKeyIsRejected(): void
    {
        $chargeJob = new ChargePaymentJob(
            new IdempotencyGuard(),
            static function (string $orderId, float $amount): void {
                // Never reached: the missing key is rejected before the charge.
            },
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('requires an idempotency key');

        $chargeJob(Job::create(type: 'charge_payment'));
    }

    public function testDuplicateDeliveryChargesOnlyOnceEndToEnd(): void
    {
        $log = sys_get_temp_dir() . '/php-job-queue-charges-' . uniqid('', true) . '.log';
        $guardLog = sys_get_temp_dir() . '/php-job-queue-guard-' . uniqid('', true) . '.log';

        try {
            $clock = new FakeClock(1000.0);
            $guard = new IdempotencyGuard(new FileStorage($guardLog));
            $charger = static function (string $orderId, float $amount) use ($log): void {
                file_put_contents($log, json_encode([$orderId, $amount]) . "\n", FILE_APPEND | LOCK_EX);
            };
            $chargeJob = new ChargePaymentJob($guard, $charger);

            $queue = new InMemoryQueue($clock);
            $job1 = Job::create(
                type: 'charge_payment',
                payload: ['order_id' => 'order-1', 'amount' => 19.99],
                idempotencyKey: 'payment:order-1',
            );
            $job2 = Job::create(
                type: 'charge_payment',
                payload: ['order_id' => 'order-1', 'amount' => 19.99],
                idempotencyKey: 'payment:order-1',
            );
            $queue->push($job1);
            $queue->push($job2);

            $pool = new WorkerPool(1, $chargeJob->__invoke(...));
            $pool->start();
            $dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
            $dispatcher->drain();

            $lines = file_exists($log) ? file($log, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) : [];
            $this->assertSame(1, count($lines));
            $this->assertSame(JobState::COMPLETED, $job1->getState());
            $this->assertSame(JobState::COMPLETED, $job2->getState());
            $this->assertSame(0, $queue->size());
        } finally {
            if (file_exists($log)) {
                unlink($log);
            }
            if (file_exists($guardLog)) {
                unlink($guardLog);
            }
        }
    }

    public function testRedeliveryAfterRestartChargesOnce(): void
    {
        $log = sys_get_temp_dir() . '/php-job-queue-charges-' . uniqid('', true) . '.log';
        $guardLog = sys_get_temp_dir() . '/php-job-queue-guard-' . uniqid('', true) . '.log';

        try {
            $clock = new FakeClock(1000.0);
            $charger = static function (string $orderId, float $amount) use ($log): void {
                file_put_contents($log, json_encode([$orderId, $amount]) . "\n", FILE_APPEND | LOCK_EX);
            };

            // First delivery charges and marks the key.
            $first = new ChargePaymentJob(new IdempotencyGuard(new FileStorage($guardLog)), $charger);
            $first(Job::create(type: 'charge_payment', payload: ['order_id' => 'order-1'], idempotencyKey: 'payment:order-1'));

            // A separate, fresh handler (e.g. a new worker) must not charge again.
            $second = new ChargePaymentJob(new IdempotencyGuard(new FileStorage($guardLog)), $charger);
            $second(Job::create(type: 'charge_payment', payload: ['order_id' => 'order-1'], idempotencyKey: 'payment:order-1'));

            $lines = file_exists($log) ? file($log, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) : [];
            $this->assertSame(1, count($lines));
        } finally {
            if (file_exists($log)) {
                unlink($log);
            }
            if (file_exists($guardLog)) {
                unlink($guardLog);
            }
        }
    }
}
