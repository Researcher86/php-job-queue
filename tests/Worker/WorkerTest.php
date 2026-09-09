<?php

declare(strict_types=1);

namespace App\Tests\Worker;

use App\Job\Job;
use App\Worker\Worker;
use App\Worker\WorkerState;
use LogicException;
use PHPUnit\Framework\TestCase;

final class WorkerTest extends TestCase
{
    public function testWorkerStartsStarting(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});

        $this->assertSame(WorkerState::STARTING, $worker->getState());
        $this->assertFalse($worker->isAvailable());
    }

    public function testWorkerBecomesIdleAfterReady(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});
        $worker->markReady();

        $this->assertSame(WorkerState::IDLE, $worker->getState());
        $this->assertTrue($worker->isAvailable());
    }

    public function testWorkerBecomesBusyWhileProcessing(): void
    {
        $stateDuringProcessing = null;
        $worker = new Worker(1, static function (Job $job) use (&$stateDuringProcessing, &$worker): void {
            $stateDuringProcessing = $worker->getState();
        });
        $worker->markReady();

        $worker->process(Job::create(type: 'test'));

        $this->assertSame(WorkerState::BUSY, $stateDuringProcessing);
    }

    public function testWorkerProcessesJob(): void
    {
        $processed = [];
        $worker = new Worker(1, static function (Job $job) use (&$processed): void {
            $processed[] = $job->getType();
        });
        $worker->markReady();

        $worker->process(Job::create(type: 'send_email'));

        $this->assertSame(['send_email'], $processed);
    }

    public function testCurrentJobIsSetWhileProcessing(): void
    {
        $seen = null;
        $worker = new Worker(1, static function (Job $job) use (&$seen, &$worker): void {
            $seen = $worker->getCurrentJob();
        });
        $worker->markReady();

        $job = Job::create(type: 'test');
        $worker->process($job);

        $this->assertSame($job->getId()->toString(), $seen?->getId()->toString());
    }

    public function testWorkerReturnsToIdleAfterJob(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});
        $worker->markReady();

        $worker->process(Job::create(type: 'test'));

        $this->assertSame(WorkerState::IDLE, $worker->getState());
        $this->assertNull($worker->getCurrentJob());
    }

    public function testWorkerBecomesIdleEvenIfHandlerThrows(): void
    {
        $worker = new Worker(1, static function (Job $job): void {
            throw new \RuntimeException('boom');
        });
        $worker->markReady();

        $result = $worker->process(Job::create(type: 'test'));

        $this->assertFalse($result->isSuccess());
        $this->assertNotNull($result->getException());
        $this->assertSame(WorkerState::IDLE, $worker->getState());
    }

    public function testWorkerReturnsSuccessResult(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});
        $worker->markReady();

        $result = $worker->process(Job::create(type: 'test'));

        $this->assertTrue($result->isSuccess());
        $this->assertNull($result->getException());
    }

    public function testWorkerReturnsFailureResultWithException(): void
    {
        $expected = new \RuntimeException('boom');
        $worker = new Worker(1, static function (Job $job) use ($expected): void {
            throw $expected;
        });
        $worker->markReady();

        $result = $worker->process(Job::create(type: 'test'));

        $this->assertFalse($result->isSuccess());
        $this->assertSame($expected, $result->getException());
    }

    public function testWorkerCannotProcessFromStarting(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Illegal transition: cannot work a worker in state STARTING');

        $worker = new Worker(1, static function (Job $job): void {});
        $worker->process(Job::create(type: 'test'));
    }

    public function testWorkerCannotBeReadyTwice(): void
    {
        $this->expectException(LogicException::class);

        $worker = new Worker(1, static function (Job $job): void {});
        $worker->markReady();
        $worker->markReady();
    }

    public function testWorkerGetId(): void
    {
        $worker = new Worker(7, static function (Job $job): void {});

        $this->assertSame(7, $worker->getId());
    }

    public function testIdleWorkerEntersDrainingOnDrain(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});
        $worker->markReady();

        $worker->drain();

        $this->assertSame(WorkerState::DRAINING, $worker->getState());
        $this->assertTrue($worker->isDraining());
        $this->assertFalse($worker->isAvailable());
    }

    public function testBusyWorkerEntersStoppingAfterFinishingJob(): void
    {
        $worker = new Worker(1, static function (Job $job) use (&$worker): void {
            // Drain is requested while the worker is still busy
            $worker->drain();
        });
        $worker->markReady();

        $worker->process(Job::create(type: 'test'));

        $this->assertSame(WorkerState::STOPPING, $worker->getState());
        $this->assertTrue($worker->isDraining());
    }

    public function testDrainingWorkerCannotProcessNewJob(): void
    {
        $this->expectException(LogicException::class);

        $worker = new Worker(1, static function (Job $job): void {});
        $worker->markReady();
        $worker->drain();

        $worker->process(Job::create(type: 'test'));
    }
}
