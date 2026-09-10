<?php

declare(strict_types=1);

namespace App\Tests\Chaos;

use App\Dispatcher\JobDispatcher;
use App\DLQ\DeadLetterQueue;
use App\Job\Job;
use App\Queue\InMemoryQueue;
use App\Persistence\InMemoryStorage;
use App\Retry\FixedDelayRetry;
use App\Tests\Support\FakeClock;
use App\Timeout\VisibilityMonitor;
use App\Worker\WorkerPool;
use PHPUnit\Framework\TestCase;

final class ChaosTest extends TestCase
{
    public function testAlwaysFailingJobEndsInDeadLetterQueue(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(2, static function (Job $job): void {
            throw new \RuntimeException('boom');
        });
        $pool->start();
        $dlq = new DeadLetterQueue($clock);

        for ($i = 0; $i < 10; $i++) {
            $queue->push(Job::create(type: "bad-$i", maxAttempts: 3, clock: $clock));
        }

        $dispatcher = new JobDispatcher(
            $queue,
            $pool,
            new FixedDelayRetry(0),
            $clock,
            dlq: $dlq,
        );
        $dispatcher->drain();

        $this->assertSame(10, $dlq->size());
        $this->assertSame(0, $queue->size());
    }

    public function testWorkerCrashDoesNotLoseJob(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, static function (Job $job): void {
            usleep(100_000);
        });
        $pool->start();

        // Worker dies while holding a job
        $job = Job::create(type: 'important', clock: $clock);
        $queue->push($job);

        $worker = $pool->getAvailableWorker();
        $this->assertNotNull($worker);
        $worker->assign($job);
        posix_kill($worker->getPid(), SIGKILL);

        $result = $pool->poll(true);
        $this->assertNotNull($result);
        $this->assertNull($result->getOutcome()->getResult());

        // The job is not lost: it can be recovered and processed again
        $pool->replaceDeadWorkers();
        $recovered = $pool->getAvailableWorker();
        $this->assertNotNull($recovered);
        $recovered->assign($job);
        $retried = $pool->poll(true);
        $this->assertNotNull($retried);
        $this->assertTrue($retried->getOutcome()->getResult()?->isSuccess());

        $pool->shutdown();
    }

    public function testQueueSurvivesCrashAndRestart(): void
    {
        $storage = new InMemoryStorage();
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock, $storage);

        $queue->push(Job::create(type: 'a', clock: $clock));
        $queue->push(Job::create(type: 'b', clock: $clock));

        // Simulate the queue process dying and restarting from storage
        $restored = InMemoryQueue::restoreFromStorage($storage, new FakeClock(2000.0));
        $pool = new WorkerPool(1, static function (Job $job): void {});
        $pool->start();

        $dispatcher = new JobDispatcher($restored, $pool, clock: new FakeClock(2000.0));
        $dispatcher->drain();

        $this->assertSame(0, $restored->size());
    }

    public function testSlowJobsDoNotBreakTheQueue(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(2, static function (Job $job): void {
            usleep(20_000);
        });
        $pool->start();

        for ($i = 0; $i < 5; $i++) {
            $queue->push(Job::create(type: "slow-$i", clock: $clock));
        }

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
        $dispatcher->drain();

        $this->assertSame(0, $queue->size());
    }

    // --- Failure Table Tests ---

    public function testDelayedJobNotExecutedBeforeAvailableAt(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, static function (Job $job): never {
            throw new \RuntimeException('Should not execute delayed job early');
        });
        $pool->start();

        // Dispatch job with 60s delay
        $queue->push(Job::create(type: 'delayed', clock: $clock), delay: 60);

        // Queue size shows the job exists, but pop() returns null
        $this->assertSame(1, $queue->size());
        $this->assertNull($queue->pop($clock->now()));

        // Advance time past the delay
        $clock->advance(61);
        $this->assertNotNull($queue->pop($clock->now()));

        $pool->shutdown();
    }

    public function testDuplicateExecutionPossibleAfterCrashBeforeAck(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);

        $counterFile = tempnam(sys_get_temp_dir(), 'chaos_exec_');
        file_put_contents($counterFile, '0');

        $pool = new WorkerPool(1, static function (Job $job) use ($counterFile): void {
            $count = (int) file_get_contents($counterFile);
            file_put_contents($counterFile, (string) ($count + 1), LOCK_EX);
            usleep(200_000);
        });
        $pool->start();

        $job = Job::create(type: 'risky', maxAttempts: 3, clock: $clock);

        $worker = $pool->getAvailableWorker();
        $this->assertNotNull($worker);
        $worker->assign($job);

        usleep(150_000);
        posix_kill($worker->getPid(), SIGKILL);
        $pool->poll(true);

        $countAfterFirst = (int) file_get_contents($counterFile);
        $this->assertSame(1, $countAfterFirst);

        $pool->replaceDeadWorkers();
        $recovered = $pool->getAvailableWorker();
        $this->assertNotNull($recovered);
        $recovered->assign($job);
        $pool->poll(true);

        $countAfterSecond = (int) file_get_contents($counterFile);
        $this->assertSame(2, $countAfterSecond);

        $pool->shutdown();
        @unlink($counterFile);
    }

    public function testVisibilityTimeoutRequeuesExpiredJobs(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $monitor = new VisibilityMonitor(1, $clock);

        $job = Job::create(type: 'slow', clock: $clock);
        $queue->push($job);

        $popped = $queue->pop($clock->now());
        $this->assertNotNull($popped);
        $popped->markProcessing();
        $monitor->track($popped);

        $this->assertTrue($monitor->isProcessing($popped));

        $clock->advance(2);
        $expired = $monitor->requeueExpired();

        $this->assertCount(1, $expired);
        $this->assertFalse($monitor->isProcessing($popped));
    }

    public function testWorkerReplacementMaintainsPoolCapacity(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(3, static function (Job $job): void {
            usleep(200_000);
        });
        $pool->start();

        $this->assertSame(3, $pool->count());

        $job1 = Job::create(type: 'j1', clock: $clock);
        $job2 = Job::create(type: 'j2', clock: $clock);
        $queue->push($job1);
        $queue->push($job2);

        $w1 = $pool->getAvailableWorker();
        $this->assertNotNull($w1);
        $w1->assign($job1);

        $w2 = $pool->getAvailableWorker();
        $this->assertNotNull($w2);
        $w2->assign($job2);

        usleep(10_000);

        posix_kill($w1->getPid(), SIGKILL);
        posix_kill($w2->getPid(), SIGKILL);
        usleep(100_000);

        while ($pool->busyCount() > 0) {
            $pool->poll(true);
        }

        $this->assertCount(2, $pool->getDeadWorkers());

        $replaced = $pool->replaceDeadWorkers();
        $this->assertSame(2, $replaced);
        $this->assertSame(3, $pool->count());
        $this->assertCount(0, $pool->getDeadWorkers());

        $job3 = Job::create(type: 'after-crash', clock: $clock);
        $queue->push($job3);
        $worker = $pool->getAvailableWorker();
        $this->assertNotNull($worker);
        $worker->assign($job3);
        $result = $pool->poll(true);
        $this->assertNotNull($result);
        $this->assertTrue($result->getOutcome()->getResult()?->isSuccess());

        $pool->shutdown();
    }

    public function testMultipleWorkerCrashesAllJobsRecovered(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, static function (Job $job): void {
            usleep(50_000);
        });
        $pool->start();

        // Crash 3 times in a row, each time with a different job
        for ($i = 0; $i < 3; $i++) {
            $job = Job::create(type: "crash-$i", clock: $clock);
            $queue->push($job);

            $worker = $pool->getAvailableWorker();
            $this->assertNotNull($worker);
            $worker->assign($job);

            usleep(20_000);
            posix_kill($worker->getPid(), SIGKILL);
            $pool->poll(true);

            $pool->replaceDeadWorkers();
        }

        // All 3 jobs are still in the queue, recoverable
        $this->assertSame(3, $queue->size());

        // Process them all successfully
        $pool2 = new WorkerPool(1, static function (Job $job): void {});
        $pool2->start();
        $dispatcher = new JobDispatcher($queue, $pool2, clock: $clock);
        $dispatcher->drain();
        $this->assertSame(0, $queue->size());

        $pool2->shutdown();
    }

    public function testMaxAttemptsExhaustedMovesJobToDlq(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $dlq = new DeadLetterQueue($clock);

        $pool = new WorkerPool(1, static function (Job $job): never {
            throw new \RuntimeException('fail');
        });
        $pool->start();

        $job = Job::create(type: 'doomed', maxAttempts: 3, clock: $clock);
        $queue->push($job);

        $dispatcher = new JobDispatcher(
            $queue,
            $pool,
            new FixedDelayRetry(0),
            $clock,
            dlq: $dlq,
        );
        $dispatcher->drain();

        $this->assertSame(1, $dlq->size());
        $this->assertSame(0, $queue->size());

        $record = $dlq->find($job->getId()->toString());
        $this->assertNotNull($record);
        $this->assertSame(3, $record->getAttempts());
    }

    public function testDelayedJobBecomesAvailableAfterTimePasses(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);

        $job = Job::create(type: 'scheduled', clock: $clock);
        $queue->push($job, delay: 30);

        // Not yet available
        $this->assertNull($queue->pop($clock->now()));

        // Exactly at available time
        $clock->advance(30);
        $this->assertNotNull($queue->pop($clock->now()));
        $this->assertSame(0, $queue->size());
    }

    public function testGracefulShutdownFinishesInFlightJobs(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);

        $completedFile = tempnam(sys_get_temp_dir(), 'chaos_drain_');
        file_put_contents($completedFile, '0');

        $pool = new WorkerPool(2, static function (Job $job) use ($completedFile): void {
            usleep(100_000);
            $count = (int) file_get_contents($completedFile);
            file_put_contents($completedFile, (string) ($count + 1), LOCK_EX);
        });
        $pool->start();

        $job1 = Job::create(type: 'g1', clock: $clock);
        $job2 = Job::create(type: 'g2', clock: $clock);
        $queue->push($job1);
        $queue->push($job2);

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
        $dispatcher->drain();

        // drain() waits for in-flight jobs to complete before returning
        $this->assertSame(0, $queue->size());
        $this->assertSame(2, (int) file_get_contents($completedFile));

        $pool->shutdown();
        @unlink($completedFile);
    }
}