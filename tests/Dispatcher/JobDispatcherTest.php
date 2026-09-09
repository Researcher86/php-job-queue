<?php

declare(strict_types=1);

namespace App\Tests\Dispatcher;

use App\Job\Job;
use App\Dispatcher\JobDispatcher;
use App\Queue\InMemoryQueue;
use App\Tests\Support\FakeClock;
use App\Worker\WorkerPool;
use PHPUnit\Framework\TestCase;

final class JobDispatcherTest extends TestCase
{
    public function testJobGoesToAvailableWorker(): void
    {
        $processed = [];
        $queue = new InMemoryQueue(new FakeClock());
        $pool = new WorkerPool(1, static function (Job $job) use (&$processed): void {
            $processed[] = $job->getType();
        });
        $pool->start();

        $queue->push(Job::create(type: 'send_email'));

        $dispatcher = new JobDispatcher($queue, $pool);
        $result = $dispatcher->dispatchNext();

        $this->assertTrue($result);
        $this->assertSame(['send_email'], $processed);
        $this->assertSame(0, $queue->size());
    }

    public function testJobsRemainQueuedWhenNoWorkersExist(): void
    {
        $queue = new InMemoryQueue(new FakeClock());
        $pool = new WorkerPool(1, static function (Job $job): void {});
        // pool not started -> no available workers

        $queue->push(Job::create(type: 'send_email'));

        $dispatcher = new JobDispatcher($queue, $pool);
        $result = $dispatcher->dispatchNext();

        $this->assertFalse($result);
        $this->assertSame(1, $queue->size());
    }

    public function testJobStaysQueuedWhenQueueEmpty(): void
    {
        $queue = new InMemoryQueue(new FakeClock());
        $pool = new WorkerPool(1, static function (Job $job): void {});
        $pool->start();

        $dispatcher = new JobDispatcher($queue, $pool);
        $result = $dispatcher->dispatchNext();

        $this->assertFalse($result);
        $this->assertSame(0, $queue->size());
    }

    public function testBusyWorkerDoesNotReceiveAnotherJobSimultaneously(): void
    {
        // Track how many jobs a single worker is handed at once
        $concurrent = 0;
        $maxConcurrent = 0;
        $queue = new InMemoryQueue(new FakeClock());
        $pool = new WorkerPool(1, static function (Job $job) use (&$concurrent, &$maxConcurrent): void {
            $concurrent++;
            $maxConcurrent = max($maxConcurrent, $concurrent);
            $concurrent--;
        });
        $pool->start();

        $queue->push(Job::create(type: 'a'));
        $queue->push(Job::create(type: 'b'));

        $dispatcher = new JobDispatcher($queue, $pool);
        $dispatcher->drain();

        $this->assertSame(1, $maxConcurrent);
    }

    public function testDrainProcessesAllQueuedJobs(): void
    {
        $processed = [];
        $queue = new InMemoryQueue(new FakeClock());
        $pool = new WorkerPool(2, static function (Job $job) use (&$processed): void {
            $processed[] = $job->getType();
        });
        $pool->start();

        $queue->push(Job::create(type: 'a'));
        $queue->push(Job::create(type: 'b'));
        $queue->push(Job::create(type: 'c'));

        $dispatcher = new JobDispatcher($queue, $pool);
        $dispatched = $dispatcher->drain();

        $this->assertSame(3, $dispatched);
        $this->assertCount(3, $processed);
        $this->assertSame(0, $queue->size());
    }

    public function testDispatchDoesNotPopWhenNoWorkerAvailable(): void
    {
        $queue = new InMemoryQueue(new FakeClock());
        $pool = new WorkerPool(1, static function (Job $job): void {});

        $queue->push(Job::create(type: 'a'));

        $dispatcher = new JobDispatcher($queue, $pool);

        $this->assertFalse($dispatcher->dispatchNext());
        // The job was not popped from the queue
        $this->assertSame(1, $queue->size());
    }
}
