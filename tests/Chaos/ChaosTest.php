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
}