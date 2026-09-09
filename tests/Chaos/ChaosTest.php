<?php

declare(strict_types=1);

namespace App\Tests\Chaos;

use App\Dispatcher\JobDispatcher;
use App\DLQ\DeadLetterQueue;
use App\Job\Job;
use App\Job\JobState;
use App\Persistence\InMemoryStorage;
use App\Queue\InMemoryQueue;
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
        $processed = [];
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, static function (Job $job) use (&$processed): void {
            $processed[] = $job->getType();
        });
        $pool->start();

        // Worker dies while holding a job in PROCESSING
        $job = Job::create(type: 'important', clock: $clock);
        $job->markReady(1000.0);
        $job->markProcessing();
        $monitor = new VisibilityMonitor(30, $clock);
        $monitor->track($job);

        $pool->getWorkers()[0]->markDead();

        // Visibility timeout recovers the job, dead worker is replaced
        $clock->advance(30.0);
        foreach ($monitor->requeueExpired() as $expired) {
            $queue->push($expired);
        }
        $pool->replaceDeadWorkers();

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, visibilityTimeout: 30);
        $dispatcher->drain();

        $this->assertSame(['important'], $processed);
        $this->assertSame(JobState::COMPLETED, $job->getState());
        $this->assertSame(0, $queue->size());
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

    public function testSlowJobDoesNotBreakTheQueue(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, static function (Job $job) use ($clock): void {
            // Simulate a slow handler without real sleeping
            $clock->advance(5.0);
        });
        $pool->start();

        for ($i = 0; $i < 5; $i++) {
            $queue->push(Job::create(type: "slow-$i", clock: $clock));
        }

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
        $dispatcher->drain();

        $this->assertSame(0, $queue->size());
        $this->assertSame(1025.0, $clock->now());
    }
}