<?php

declare(strict_types=1);

namespace App\Tests\Worker;

use App\Job\Job;
use App\Tests\Support\Handlers;
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
        $worker = new Worker(1, Handlers::succeeds());
        $worker->spawn();

        $this->assertGreaterThan(0, $worker->getPid());
        $this->assertSame(WorkerState::IDLE, $worker->getState());
        $this->assertTrue($worker->isAvailable());

        $worker->shutdown();
    }

    public function testWorkerProcessesJob(): void
    {
        $worker = new Worker(1, Handlers::succeeds());
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
        $worker = new Worker(1, Handlers::succeeds());
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

        $worker = new Worker(1, Handlers::succeeds());
        $worker->spawn();

        $worker->assign(Job::create(type: 'a'));
        $worker->assign(Job::create(type: 'b'));

        $worker->shutdown();
    }

    public function testWorkerCannotAssignBeforeSpawn(): void
    {
        $this->expectException(LogicException::class);

        $worker = new Worker(1, Handlers::succeeds());
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
        $worker = new Worker(1, Handlers::succeeds());
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
        $worker = new Worker(1, Handlers::succeeds());
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
        $worker = new Worker(1, Handlers::succeeds());
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
        $worker = new Worker(7, Handlers::succeeds());
        $worker->spawn();

        $this->assertSame(7, $worker->getId());

        $worker->shutdown();
    }

    public function testIdleWorkerStopsOnDrain(): void
    {
        $worker = new Worker(1, Handlers::succeeds());
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

    /**
     * A worker that goes out of scope takes its process with it. Without a
     * destructor the child outlived its handle, exited when the parent did,
     * and was left unreaped - which is how a test suite accumulates
     * zombies.
     */
    public function testAWorkerGoingOutOfScopeLeavesNoProcessBehind(): void
    {
        $worker = new Worker(1, Handlers::succeeds());
        $worker->spawn();
        $pid = $worker->getPid();

        $this->assertTrue(posix_kill($pid, 0), 'the worker process is running');

        unset($worker);

        $this->waitForExit($pid);

        $this->assertFalse(posix_kill($pid, 0), 'the worker process is gone');
        // And reaped: waitpid finds nothing left to collect.
        $this->assertSame(-1, pcntl_waitpid($pid, $status, WNOHANG));
    }

    /**
     * The worst failure this class can have: a child that RETURNS from
     * spawn() carries on executing whatever the parent was doing, as a
     * second copy of the parent process.
     *
     * It used to happen on an ordinary path. workerLoop()'s final write
     * throws when the parent has closed its end - which is what a shutdown
     * mid-job does - and the exception propagated out of spawn() into the
     * caller's stack. In this test suite that produced a second PHPUnit
     * process, which went on to fork workers of its own.
     *
     * The assertion is that the child exits promptly. An escaped child
     * would still be running whatever came next, and would not.
     */
    public function testAWorkerWhoseParentClosedMidJobExitsInsteadOfEscaping(): void
    {
        $worker = new Worker(1, static function (Job $job): void {
            usleep(150_000);
        });
        $worker->spawn();
        $pid = $worker->getPid();

        $worker->assign(Job::create(type: 'slow'));

        // Close our end while the handler is still running: the child's
        // write will fail when it finishes.
        $worker->terminate();

        $exited = false;
        for ($i = 0; $i < 400; $i++) {
            if (pcntl_waitpid($pid, $status, WNOHANG) !== 0) {
                $exited = true;
                break;
            }

            usleep(10_000);
        }

        $this->assertTrue($exited, 'the worker process escaped instead of exiting');
    }
}
