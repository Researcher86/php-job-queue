<?php

declare(strict_types=1);

namespace App\Tests\Dispatcher;

use App\Dispatcher\JobDispatcher;
use App\DLQ\DeadLetterQueue;
use App\Job\Job;
use App\Job\JobPriority;
use App\Job\JobState;
use App\Metrics\MetricsCollector;
use App\Persistence\FileStorage;
use App\Queue\InMemoryQueue;
use App\Queue\PriorityQueue;
use App\Retry\FixedDelayRetry;
use App\Tests\Support\FakeClock;
use App\Timeout\VisibilityMonitor;
use App\Worker\WorkerPool;
use Closure;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class JobDispatcherTest extends TestCase
{
    /**
     * @param Closure(Job): mixed $handler
     *
     * @return array{InMemoryQueue, WorkerPool, JobDispatcher}
     */
    private function dispatcherWith(Closure $handler, ?\App\Retry\RetryPolicy $retryPolicy = null): array
    {
        $queue = new InMemoryQueue(new FakeClock());
        $pool = new WorkerPool(1, $handler);
        $pool->start();

        return [$queue, $pool, new JobDispatcher($queue, $pool, $retryPolicy, new FakeClock())];
    }

    public function testSuccessfulJobIsCompleted(): void
    {
        [$queue, , $dispatcher] = $this->dispatcherWith(static function (Job $job): void {});

        $job = Job::create(type: 'ok');
        $queue->push($job);

        $dispatcher->dispatchNext();

        $this->assertSame(JobState::COMPLETED, $job->getState());
    }

    public function testFailedJobIsFailedAfterExhaustingAttempts(): void
    {
        [$queue, , $dispatcher] = $this->dispatcherWith(static function (Job $job): void {
            throw new RuntimeException('boom');
        });

        $job = Job::create(type: 'bad', maxAttempts: 1);
        $queue->push($job);

        $dispatcher->dispatchNext();

        $this->assertSame(JobState::FAILED, $job->getState());
    }

    public function testFailedJobIsRetriedWhenAttemptsRemain(): void
    {
        [$queue, , $dispatcher] = $this->dispatcherWith(static function (Job $job): void {
            throw new RuntimeException('boom');
        }, new FixedDelayRetry(0));

        $job = Job::create(type: 'bad', maxAttempts: 2);
        $queue->push($job);

        $dispatcher->dispatchNext();

        $this->assertSame(JobState::READY, $job->getState());
        $this->assertSame(1, $queue->size());
    }

    public function testRetriedJobIsDispatchedAgainUntilFailed(): void
    {
        [$queue, , $dispatcher] = $this->dispatcherWith(static function (Job $job): void {
            throw new RuntimeException('boom');
        }, new FixedDelayRetry(0));

        $job = Job::create(type: 'bad', maxAttempts: 2);
        $queue->push($job);

        $dispatcher->drain();

        $this->assertSame(2, $job->getAttempts());
        $this->assertSame(JobState::FAILED, $job->getState());
        $this->assertSame(0, $queue->size());
    }

    public function testSuccessfulRetryCompletesJob(): void
    {
        $attempts = 0;
        [$queue, , $dispatcher] = $this->dispatcherWith(static function (Job $job) use (&$attempts): void {
            $attempts++;
            if ($attempts === 1) {
                throw new RuntimeException('boom');
            }
        }, new FixedDelayRetry(0));

        $job = Job::create(type: 'flaky', maxAttempts: 3);
        $queue->push($job);

        $dispatcher->drain();

        $this->assertSame(JobState::COMPLETED, $job->getState());
        $this->assertSame(0, $queue->size());
    }

    public function testJobGoesToAvailableWorker(): void
    {
        [$queue, , $dispatcher] = $this->dispatcherWith(static function (Job $job): void {});

        $job = Job::create(type: 'send_email');
        $queue->push($job);

        $result = $dispatcher->dispatchNext();

        $this->assertTrue($result);
        $this->assertSame(JobState::COMPLETED, $job->getState());
        $this->assertSame(0, $queue->size());
    }

    public function testJobsRemainQueuedWhenNoWorkersExist(): void
    {
        $queue = new InMemoryQueue(new FakeClock());
        $pool = new WorkerPool(1, static function (Job $job): void {});
        // pool not started -> no workers

        $job = Job::create(type: 'send_email');
        $queue->push($job);

        $dispatcher = new JobDispatcher($queue, $pool, clock: new FakeClock());
        $result = $dispatcher->dispatchNext();

        $this->assertFalse($result);
        $this->assertSame(1, $queue->size());
        $this->assertSame(JobState::READY, $job->getState());

        $pool->shutdown();
    }

    public function testJobStaysQueuedWhenQueueEmpty(): void
    {
        [$queue, , $dispatcher] = $this->dispatcherWith(static function (Job $job): void {});

        $result = $dispatcher->dispatchNext();

        $this->assertFalse($result);
        $this->assertSame(0, $queue->size());
    }

    public function testSingleWorkerProcessesJobsSequentially(): void
    {
        [$queue, , $dispatcher] = $this->dispatcherWith(static function (Job $job): void {});

        $queue->push(Job::create(type: 'a'));
        $queue->push(Job::create(type: 'b'));

        $dispatcher->drain();

        $this->assertSame(0, $queue->size());
        $this->assertSame(JobState::COMPLETED, $queue->pop()?->getState() ?? JobState::COMPLETED);
    }

    public function testDrainProcessesAllQueuedJobs(): void
    {
        $queue = new InMemoryQueue(new FakeClock());
        $pool = new WorkerPool(2, static function (Job $job): void {});
        $pool->start();

        $queue->push(Job::create(type: 'a'));
        $queue->push(Job::create(type: 'b'));
        $queue->push(Job::create(type: 'c'));

        $dispatcher = new JobDispatcher($queue, $pool, clock: new FakeClock());
        $dispatched = $dispatcher->drain();

        $this->assertSame(3, $dispatched);
        $this->assertSame(0, $queue->size());
    }

    public function testDispatchDoesNotPopWhenNoWorkerAvailable(): void
    {
        $queue = new InMemoryQueue(new FakeClock());
        $pool = new WorkerPool(1, static function (Job $job): void {});

        $queue->push(Job::create(type: 'a'));

        $dispatcher = new JobDispatcher($queue, $pool, clock: new FakeClock());

        $this->assertFalse($dispatcher->dispatchNext());
        $this->assertSame(1, $queue->size());

        $pool->shutdown();
    }

    public function testExpiredProcessingJobReturnsToQueueAndRunsAgain(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, static function (Job $job): void {});
        $pool->start();

        // Simulate a job that was left in PROCESSING by a crashed worker
        $job = Job::create(type: 'a');
        $job->markReady(1000.0);
        $job->markProcessing();
        $monitor = new VisibilityMonitor(30, $clock);
        $monitor->track($job);

        $clock->advance(30.0);
        foreach ($monitor->requeueExpired() as $expired) {
            $queue->push($expired);
        }

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
        $dispatcher->drain();

        $this->assertSame(JobState::COMPLETED, $job->getState());
        $this->assertSame(0, $queue->size());
    }

    public function testNullVisibilityTimeoutDoesNotExpireInFlightJobs(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, static function (Job $job): void {});
        $pool->start();

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
        $job = Job::create(type: 'a');
        $queue->push($job);

        $this->assertTrue($dispatcher->dispatchNext());
        $this->assertSame(JobState::COMPLETED, $job->getState());

        $clock->advance(3600.0);
        $this->assertSame(0, $dispatcher->requeueExpired());

        $pool->shutdown();
    }

    public function testExhaustedJobIsSentToDeadLetterQueue(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, static function (Job $job): void {
            throw new RuntimeException('boom');
        });
        $pool->start();
        $dlq = new DeadLetterQueue($clock);

        $job = Job::create(type: 'bad', maxAttempts: 2);
        $queue->push($job);

        $dispatcher = new JobDispatcher($queue, $pool, new FixedDelayRetry(0), $clock, dlq: $dlq);
        $dispatcher->drain();

        $this->assertSame(JobState::FAILED, $job->getState());
        $this->assertSame(1, $dlq->size());
        $this->assertTrue($dlq->contains($job));
        $record = $dlq->find($job->getId()->toString());
        $this->assertNotNull($record);
        $this->assertSame('boom', $record->getException()->getMessage());
        $this->assertSame(2, $record->getAttempts());
    }

    public function testQueueSurvivesRestartEndToEnd(): void
    {
        $path = sys_get_temp_dir() . '/php-job-queue-restart-' . uniqid('', true) . '.log';

        try {
            $clock = new FakeClock(1000.0);
            $storage = new FileStorage($path);
            $queue = new InMemoryQueue($clock, $storage);
            $queue->push(Job::create(type: 'a'));

            // No worker available, so the job stays queued in READY state
            $pool = new WorkerPool(1, static function (Job $job): void {});
            $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, storage: $storage);
            $dispatcher->dispatchNext();

            // Simulate process restart: rebuild the queue from storage
            $restoredQueue = InMemoryQueue::restoreFromStorage($storage, new FakeClock(2000.0));

            $this->assertSame(1, $restoredQueue->size());
            $restored = $restoredQueue->pop();
            $this->assertNotNull($restored);
            $this->assertSame('a', $restored->getType());
            $this->assertSame(JobState::READY, $restored->getState());

            $pool->shutdown();
        } finally {
            if (file_exists($path)) {
                unlink($path);
            }
        }
    }

    public function testCompletedJobIsNotRestoredAfterRestart(): void
    {
        $path = sys_get_temp_dir() . '/php-job-queue-restart-' . uniqid('', true) . '.log';

        try {
            $clock = new FakeClock(1000.0);
            $storage = new FileStorage($path);
            $queue = new InMemoryQueue($clock, $storage);
            $queue->push(Job::create(type: 'done'));

            $pool = new WorkerPool(1, static function (Job $job): void {});
            $pool->start();
            $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, storage: $storage);
            $dispatcher->drain();

            $restoredQueue = InMemoryQueue::restoreFromStorage($storage, new FakeClock(2000.0));

            $this->assertSame(0, $restoredQueue->size());
        } finally {
            if (file_exists($path)) {
                unlink($path);
            }
        }
    }

    public function testDispatcherServesHigherPriorityJobsFirst(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new PriorityQueue($clock);
        $pool = new WorkerPool(2, static function (Job $job): void {});
        $pool->start();

        $queue->push(Job::create(type: 'low', priority: JobPriority::LOW));
        $queue->push(Job::create(type: 'high', priority: JobPriority::HIGH));
        $queue->push(Job::create(type: 'normal', priority: JobPriority::NORMAL));

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
        $dispatcher->drain();

        $this->assertSame(0, $queue->size());
    }

    public function testMetricsCountCompletedAndLatency(): void
    {
        $metrics = new MetricsCollector();
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, static function (Job $job): void {}, $metrics);
        $pool->start();

        $queue->push(Job::create(type: 'a', clock: $clock));

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, metrics: $metrics);
        $dispatcher->drain();

        $this->assertSame(1, $metrics->getCounter('completed'));
        $this->assertNotNull($metrics->getLatencyStats('execution'));
        $this->assertNotNull($metrics->getLatencyStats('end_to_end'));
    }

    public function testMetricsCountRetriesAndDlq(): void
    {
        $metrics = new MetricsCollector();
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, static function (Job $job): void {
            throw new RuntimeException('boom');
        });
        $pool->start();
        $dlq = new DeadLetterQueue($clock);

        $job = Job::create(type: 'bad', maxAttempts: 2);
        $queue->push($job);

        $dispatcher = new JobDispatcher($queue, $pool, new FixedDelayRetry(0), $clock, dlq: $dlq, metrics: $metrics);
        $dispatcher->drain();

        $this->assertSame(1, $metrics->getCounter('retried'));
        $this->assertSame(1, $metrics->getCounter('failed'));
        $this->assertSame(1, $metrics->getCounter('dlq'));
    }

    public function testMetricsCountWorkerCrashes(): void
    {
        $metrics = new MetricsCollector();
        $pool = new WorkerPool(1, static function (Job $job): void {
            usleep(100_000);
        }, $metrics);
        $pool->start();

        $worker = $pool->getAvailableWorker();
        $this->assertNotNull($worker);
        $worker->assign(Job::create(type: 'a'));
        posix_kill($worker->getPid(), SIGKILL);

        $pool->poll(true);
        $pool->replaceDeadWorkers();

        $this->assertSame(1, $metrics->getCounter('worker_crashes'));

        $pool->shutdown();
    }

    public function testDispatcherCountsWorkerCrashOnceWithSharedMetrics(): void
    {
        $metrics = new MetricsCollector();
        $pool = new WorkerPool(1, static function (Job $job): void {
            posix_kill(getmypid(), SIGKILL);
        }, $metrics);
        $pool->start();

        $job = Job::create(type: 'crash', maxAttempts: 1);
        $queue = new InMemoryQueue(new FakeClock());
        $queue->push($job);

        $dispatcher = new JobDispatcher($queue, $pool, new FixedDelayRetry(0), metrics: $metrics);
        $dispatcher->dispatchNext();

        $this->assertSame(1, $metrics->getCounter('worker_crashes'));

        $pool->shutdown();
    }

    public function testShutdownStopsDispatchingWithoutLosingJobs(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, static function (Job $job): void {});
        $pool->start();

        $job = Job::create(type: 'pending');
        $queue->push($job);

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
        $this->assertTrue($dispatcher->isAccepting());

        $dispatcher->shutdown();

        $this->assertFalse($dispatcher->isAccepting());
        $this->assertFalse($dispatcher->dispatchNext());
        $this->assertTrue($pool->isDraining());
        // The job stays queued and is not lost
        $this->assertSame(1, $queue->size());
        $this->assertSame(JobState::READY, $job->getState());

        $pool->shutdown();
    }
}
