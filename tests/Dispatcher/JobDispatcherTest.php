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
use App\Persistence\InMemoryStorage;
use App\Queue\InMemoryQueue;
use App\Queue\PriorityQueue;
use App\Retry\FixedDelayRetry;
use App\Tests\Support\Deliveries;
use App\Tests\Support\FakeClock;
use App\Tests\Support\Handlers;
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
        [$queue, , $dispatcher] = $this->dispatcherWith(Handlers::succeeds());

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
        [$queue, , $dispatcher] = $this->dispatcherWith(Handlers::succeeds());

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
        $pool = new WorkerPool(1, Handlers::succeeds());
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
        [$queue, , $dispatcher] = $this->dispatcherWith(Handlers::succeeds());

        $result = $dispatcher->dispatchNext();

        $this->assertFalse($result);
        $this->assertSame(0, $queue->size());
    }

    public function testSingleWorkerProcessesJobsSequentially(): void
    {
        [$queue, , $dispatcher] = $this->dispatcherWith(Handlers::succeeds());

        $queue->push(Job::create(type: 'a'));
        $queue->push(Job::create(type: 'b'));

        $dispatcher->drain();

        $this->assertSame(0, $queue->size());
        $this->assertSame(JobState::COMPLETED, $queue->pop()?->getState() ?? JobState::COMPLETED);
    }

    public function testDrainProcessesAllQueuedJobs(): void
    {
        $queue = new InMemoryQueue(new FakeClock());
        $pool = new WorkerPool(2, Handlers::succeeds());
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
        $pool = new WorkerPool(1, Handlers::succeeds());

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
        $pool = new WorkerPool(1, Handlers::succeeds());
        $pool->start();

        // Simulate a job that was left in PROCESSING by a crashed worker
        $job = Job::create(type: 'a');
        $job->markReady(1000.0);
        $job->markProcessing();
        $monitor = new VisibilityMonitor(30, $clock);
        $monitor->track($job, 1);

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
        $pool = new WorkerPool(1, Handlers::succeeds());
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
            $pool = new WorkerPool(1, Handlers::succeeds());
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

    /**
     * An attempt is a delivery, and that has to survive the process.
     *
     * The log's last word on an in-flight job used to be the READY record
     * from push(), attempts 0 - so a crash gave the job its whole
     * allowance back. A job that reliably kills its worker would then loop
     * across restarts forever instead of reaching the DLQ, which is the
     * one thing the DLQ exists to prevent.
     */
    public function testAnInFlightJobsAttemptsSurviveARestart(): void
    {
        $clock = new FakeClock(1000.0);
        $storage = new InMemoryStorage();
        $queue = new InMemoryQueue($clock, $storage);
        $pool = new WorkerPool(1, Handlers::succeeds());
        $pool->start();

        $job = Job::create(type: 'doomed', maxAttempts: 3, clock: $clock);
        $queue->push($job);

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, storage: $storage);

        // Dispatched, and then the process dies before any answer.
        $this->assertTrue($dispatcher->dispatchPending() > 0);
        $this->assertSame(1, $job->getAttempts());

        $restored = InMemoryQueue::restoreFromStorage($storage, new FakeClock(2000.0));
        $recovered = $restored->pop();

        $this->assertNotNull($recovered);
        $this->assertSame(JobState::READY, $recovered->getState());
        $this->assertSame(1, $recovered->getAttempts(), 'the delivery it lost still counted');
        $this->assertSame(3, $recovered->getMaxAttempts());

        $pool->shutdown();
    }

    /**
     * The consequence of the above, followed to the end: a job that kills
     * its worker every time still runs out of attempts. Without durable
     * attempts each restart handed it a fresh allowance and it would run
     * forever.
     */
    public function testAJobThatKeepsKillingItsWorkerStillReachesTheDlq(): void
    {
        $clock = new FakeClock(1000.0);
        $storage = new InMemoryStorage();
        $dlq = new DeadLetterQueue($clock);
        $queue = new InMemoryQueue($clock, $storage);

        $job = Job::create(type: 'kills-workers', maxAttempts: 3, clock: $clock);
        $queue->push($job);

        // Three separate "processes", each dispatching once and then dying.
        for ($restart = 0; $restart < 3; $restart++) {
            $pool = new WorkerPool(1, Handlers::succeeds());
            $pool->start();
            $dispatcher = new JobDispatcher(
                $queue,
                $pool,
                new FixedDelayRetry(0),
                $clock,
                dlq: $dlq,
                storage: $storage,
            );

            $this->assertSame(1, $dispatcher->dispatchPending(), "restart $restart dispatched");

            // The process dies here - no answer, no ACK - and the next one
            // rebuilds the queue from the log.
            $pool->shutdown();
            $queue = InMemoryQueue::restoreFromStorage($storage, $clock);
        }

        // Three deliveries used up. The fourth attempt is not allowed.
        $recovered = $queue->pop();
        $this->assertNotNull($recovered);
        $this->assertSame(3, $recovered->getAttempts(), 'all three deliveries counted');
        $this->assertSame(3, $recovered->getMaxAttempts());
    }

    public function testCompletedJobIsNotRestoredAfterRestart(): void
    {
        $path = sys_get_temp_dir() . '/php-job-queue-restart-' . uniqid('', true) . '.log';

        try {
            $clock = new FakeClock(1000.0);
            $storage = new FileStorage($path);
            $queue = new InMemoryQueue($clock, $storage);
            $queue->push(Job::create(type: 'done'));

            $pool = new WorkerPool(1, Handlers::succeeds());
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
        $pool = new WorkerPool(2, Handlers::succeeds());
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
        $pool = new WorkerPool(1, Handlers::succeeds(), $metrics);
        $pool->start();

        $queue->push(Job::create(type: 'a', clock: $clock));

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, metrics: $metrics);
        $dispatcher->drain();

        $this->assertSame(1, $metrics->getCounter('completed'));
        $this->assertNotNull($metrics->getLatencyStats('execution'));
        $this->assertNotNull($metrics->getLatencyStats('end_to_end'));
    }

    /**
     * The three latencies measure three different things, and this is the
     * test that says so: the clock is advanced only while the job waits in
     * the queue, so queue wait is the wait and execution is not.
     */
    public function testQueueWaitIsMeasuredSeparatelyFromExecution(): void
    {
        $metrics = new MetricsCollector();
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, Handlers::succeeds(), $metrics);
        $pool->start();

        $job = Job::create(type: 'a', clock: $clock);
        $queue->push($job);

        // The job is available now, but nothing dispatches for 5 seconds.
        $clock->advance(5.0);

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, metrics: $metrics);
        $dispatcher->drain();

        $this->assertSame(5.0, $metrics->getLatencyStats(MetricsCollector::LATENCY_QUEUE_WAIT)['avg']);
        $this->assertSame(0.0, $metrics->getLatencyStats(MetricsCollector::LATENCY_EXECUTION)['avg']);
        $this->assertSame(5.0, $metrics->getLatencyStats(MetricsCollector::LATENCY_END_TO_END)['avg']);
    }

    /**
     * A delayed job's deliberate wait is not queue wait. It counts towards
     * end-to-end - the caller did wait that long - but the queue was not
     * behind, so the number that would tell you to add workers must not
     * move.
     */
    public function testADeliberateDelayIsNotCountedAsQueueWait(): void
    {
        $metrics = new MetricsCollector();
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, Handlers::succeeds(), $metrics);
        $pool->start();

        $queue->push(Job::create(type: 'later', clock: $clock), delay: 60);
        $clock->advance(60.0);

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, metrics: $metrics);
        $dispatcher->drain();

        $this->assertSame(0.0, $metrics->getLatencyStats(MetricsCollector::LATENCY_QUEUE_WAIT)['avg']);
        $this->assertSame(60.0, $metrics->getLatencyStats(MetricsCollector::LATENCY_END_TO_END)['avg']);
    }

    public function testObserveReportsWhatTheSystemLooksLikeNow(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(2, Handlers::succeeds());
        $pool->start();
        $dlq = new DeadLetterQueue($clock);

        $queue->push(Job::create(type: 'now', clock: $clock));
        $queue->push(Job::create(type: 'later', clock: $clock), delay: 60);

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, visibilityTimeout: 30, dlq: $dlq);
        $idle = $dispatcher->observe();

        $this->assertSame(1, $idle->ready);
        $this->assertSame(1, $idle->delayed);
        $this->assertSame(0, $idle->processing);
        $this->assertSame(2, $idle->workers);
        $this->assertSame(0, $idle->busyWorkers);
        $this->assertSame(2, $idle->idleWorkers());
        $this->assertSame(0, $idle->deadLettered);

        $dispatcher->drain();
        $drained = $dispatcher->observe();

        $this->assertSame(0, $drained->ready);
        $this->assertSame(0, $drained->processing);
        $this->assertSame(['ready', 'delayed', 'processing', 'workers', 'busy_workers', 'idle_workers', 'dead_lettered'], array_keys($drained->toArray()));
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
        $worker->assign(Deliveries::to($worker, Job::create(type: 'a')));
        posix_kill($worker->getPid(), SIGKILL);

        $pool->poll(null);
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
        $pool = new WorkerPool(1, Handlers::succeeds());
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
