<?php

declare(strict_types=1);

namespace App\Tests\Worker;

use App\Job\Job;
use App\Worker\Worker;
use App\Worker\WorkerDiedException;
use App\Worker\WorkerState;
use LogicException;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class WorkerTest extends TestCase
{
    public function testWorkerSpawnsProcess(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});
        $worker->spawn();

        $this->assertGreaterThan(0, $worker->getPid());
        $this->assertSame(WorkerState::IDLE, $worker->getState());
        $this->assertTrue($worker->isAvailable());

        $worker->shutdown();
    }

    public function testWorkerProcessesJob(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});
        $worker->spawn();

        $job = Job::create(type: 'send_email');
        $worker->assign($job);
        $outcome = $worker->collect(null);

        $this->assertNotNull($outcome);
        $this->assertSame($job->getId()->toString(), $outcome->getJob()->getId()->toString());
        $this->assertTrue($outcome->getResult()?->isSuccess());
        $this->assertSame(WorkerState::IDLE, $worker->getState());

        $worker->shutdown();
    }

    public function testWorkerReturnsFailureResultWithException(): void
    {
        $worker = new Worker(1, static function (Job $job): void {
            throw new RuntimeException('boom');
        });
        $worker->spawn();

        $worker->assign(Job::create(type: 'test'));
        $outcome = $worker->collect(null);

        $this->assertNotNull($outcome);
        $result = $outcome->getResult();
        $this->assertNotNull($result);
        $this->assertFalse($result->isSuccess());
        $this->assertSame('boom', $result->getException()->getMessage());

        $worker->shutdown();
    }

    public function testWorkerBecomesBusyWhileProcessing(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});
        $worker->spawn();

        $job = Job::create(type: 'test');
        $worker->assign($job);

        $this->assertSame(WorkerState::BUSY, $worker->getState());
        $this->assertTrue($worker->isBusy());
        $this->assertSame($job->getId()->toString(), $worker->getCurrentJob()?->getId()->toString());

        $worker->collect(null);
        $this->assertSame(WorkerState::IDLE, $worker->getState());
        $this->assertNull($worker->getCurrentJob());

        $worker->shutdown();
    }

    public function testWorkerCannotAssignTwice(): void
    {
        $this->expectException(LogicException::class);

        $worker = new Worker(1, static function (Job $job): void {});
        $worker->spawn();

        $worker->assign(Job::create(type: 'a'));
        $worker->assign(Job::create(type: 'b'));

        $worker->shutdown();
    }

    public function testWorkerCannotAssignBeforeSpawn(): void
    {
        $this->expectException(LogicException::class);

        $worker = new Worker(1, static function (Job $job): void {});
        $worker->assign(Job::create(type: 'a'));
    }

    public function testWorkerCrashIsDetected(): void
    {
        $worker = new Worker(1, static function (Job $job): void {
            usleep(100_000);
        });
        $worker->spawn();

        $job = Job::create(type: 'test');
        $worker->assign($job);
        posix_kill($worker->getPid(), SIGKILL);

        $outcome = $worker->collect(null);

        $this->assertNotNull($outcome);
        $this->assertNull($outcome->getResult());
        $this->assertTrue($worker->isDead());
        $this->assertSame($job->getId()->toString(), $outcome->getJob()->getId()->toString());

        $worker->shutdown();
    }

    public function testReapNoticesAWorkerThatDiedWhileIdle(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});
        $worker->spawn();
        $this->assertTrue($worker->isAvailable());

        posix_kill($worker->getPid(), SIGKILL);
        $this->waitForExit($worker->getPid());

        $this->assertTrue($worker->reap());
        $this->assertTrue($worker->isDead());
        // Idempotent: the second call has nothing left to find.
        $this->assertFalse($worker->reap());
    }

    public function testReapLeavesALiveWorkerAlone(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});
        $worker->spawn();

        $this->assertFalse($worker->reap());
        $this->assertTrue($worker->isAvailable());

        $worker->shutdown();
    }

    /**
     * A busy worker's death belongs to poll()/collect(), which is the only
     * path that knows which job went down with it. reap() must not get
     * there first and swallow the job.
     */
    public function testReapIgnoresABusyWorker(): void
    {
        $worker = new Worker(1, static function (Job $job): void {
            usleep(100_000);
        });
        $worker->spawn();
        $worker->assign(Job::create(type: 'slow'));

        posix_kill($worker->getPid(), SIGKILL);
        $this->waitForExit($worker->getPid());

        $this->assertFalse($worker->reap());
        $this->assertTrue($worker->isBusy());

        $outcome = $worker->collect(null);
        $this->assertNotNull($outcome);
        $this->assertNull($outcome->getResult());
        $this->assertSame('slow', $outcome->getJob()->getType());

        $worker->shutdown();
    }

    public function testAssignToAWorkerKilledWhileIdleReportsItsDeath(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});
        $worker->spawn();

        posix_kill($worker->getPid(), SIGKILL);
        $this->waitForExit($worker->getPid());

        try {
            $worker->assign(Job::create(type: 'a'));
            $this->fail('Expected the assign to report a dead worker');
        } catch (WorkerDiedException) {
            $this->assertTrue($worker->isDead());
        }
    }

    public function testWorkerGetId(): void
    {
        $worker = new Worker(7, static function (Job $job): void {});
        $worker->spawn();

        $this->assertSame(7, $worker->getId());

        $worker->shutdown();
    }

    public function testIdleWorkerStopsOnDrain(): void
    {
        $worker = new Worker(1, static function (Job $job): void {});
        $worker->spawn();

        $worker->drain();

        $this->assertSame(WorkerState::STOPPING, $worker->getState());
        $this->assertTrue($worker->isDraining());
        $this->assertFalse($worker->isAvailable());

        $worker->shutdown();
    }

    public function testBusyWorkerStopsAfterFinishingJob(): void
    {
        $worker = new Worker(1, static function (Job $job): void {
            usleep(50_000);
        });
        $worker->spawn();

        $worker->assign(Job::create(type: 'test'));
        $worker->drain();

        $outcome = $worker->collect(null);

        $this->assertNotNull($outcome);
        $this->assertSame(WorkerState::STOPPING, $worker->getState());
        $this->assertTrue($worker->isDraining());

        $worker->shutdown();
    }

    private function waitForExit(int $pid): void
    {
        for ($i = 0; $i < 200 && posix_kill($pid, 0); $i++) {
            usleep(5_000);
        }
    }
}
