<?php

declare(strict_types=1);

namespace App\Tests\Chaos;

use App\Dispatcher\JobDispatcher;
use App\DLQ\DeadLetterQueue;
use App\Job\Job;
use App\Job\JobState;
use App\Metrics\MetricsCollector;
use App\Persistence\InMemoryStorage;
use App\Queue\InMemoryQueue;
use App\Retry\FixedDelayRetry;
use App\Tests\Support\Deliveries;
use App\Tests\Support\FakeClock;
use App\Tests\Support\Handlers;
use App\Timeout\VisibilityMonitor;
use App\Worker\WorkerPool;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class ChaosTest extends TestCase
{
    public function testAlwaysFailingJobEndsInDeadLetterQueue(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(2, static function (Job $job): void {
            throw new RuntimeException('boom');
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

    /**
     * PLAN.md Phase 11: "Kill worker while idle".
     *
     * Nobody is selecting on an idle worker's socket, so this death is
     * invisible until something goes looking - which is what maintain()
     * is for.
     */
    public function testIdleWorkerCrashIsNoticedAndReplaced(): void
    {
        $pool = new WorkerPool(2, Handlers::succeeds());
        $pool->start();

        $idle = $pool->getAvailableWorker();
        $this->assertNotNull($idle);
        $this->assertTrue($idle->isAvailable());

        posix_kill($idle->getPid(), SIGKILL);
        $this->waitForExit($idle->getPid());

        // Nothing has looked yet, so nothing knows.
        $this->assertFalse($pool->hasDeadWorkers());

        $this->assertSame(1, $pool->maintain());
        $this->assertFalse($pool->hasDeadWorkers());
        $this->assertSame(2, $pool->count());

        // And the replacement is a usable worker, not just a slot.
        $replacement = $pool->getAvailableWorker();
        $this->assertNotNull($replacement);
        $replacement->assign(Deliveries::to($replacement, Job::create(type: 'after-crash')));
        $result = $pool->poll(null);
        $this->assertTrue($result?->getResult()?->isSuccess());

        $pool->shutdown();
    }

    /**
     * PLAN.md Phase 11, the same kill through the dispatcher: an idle
     * worker dies, and the job dispatched to it is not lost - not to the
     * crash, and not to an exception escaping the dispatch loop either.
     */
    public function testJobSurvivesDispatchToAWorkerKilledWhileIdle(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(1, Handlers::succeeds());
        $pool->start();

        $idle = $pool->getAvailableWorker();
        $this->assertNotNull($idle);
        posix_kill($idle->getPid(), SIGKILL);
        $this->waitForExit($idle->getPid());

        $job = Job::create(type: 'important', clock: $clock);
        $queue->push($job);

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
        $dispatcher->drain();

        $this->assertSame(JobState::COMPLETED, $job->getState());
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
        $worker->assign(Deliveries::to($worker, $job));
        posix_kill($worker->getPid(), SIGKILL);

        $result = $pool->poll(null);
        $this->assertNotNull($result);
        $this->assertNull($result->getResult());

        // The job is not lost: it can be recovered and processed again
        $pool->replaceDeadWorkers();
        $recovered = $pool->getAvailableWorker();
        $this->assertNotNull($recovered);
        $recovered->assign(Deliveries::to($recovered, $job));
        $retried = $pool->poll(null);
        $this->assertNotNull($retried);
        $this->assertTrue($retried->getResult()?->isSuccess());

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
        $pool = new WorkerPool(1, Handlers::succeeds());
        $pool->start();

        $dispatcher = new JobDispatcher($restored, $pool, clock: new FakeClock(2000.0));
        $dispatcher->drain();

        $this->assertSame(0, $restored->size());
    }

    /**
     * PLAN.md Phase 16's slow job, and the thing it actually breaks: a
     * handler that takes longer than the visibility timeout gets its job
     * handed to a SECOND worker while the first is still working on it.
     *
     * That is not the handler misbehaving, it is the timeout being too
     * short - see PLAN.md's engineering question 2, request timeout versus
     * visibility timeout. And the consequence is a duplicate execution
     * with nothing crashed and nothing failing, which is the version of
     * at-least-once delivery that surprises people.
     */
    public function testAHandlerSlowerThanTheVisibilityTimeoutRunsTwice(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $runFile = tempnam(sys_get_temp_dir(), 'slow-');
        $pool = new WorkerPool(2, static function (Job $job) use ($runFile): void {
            file_put_contents($runFile, getmypid() . "\n", FILE_APPEND);
            usleep(150_000);
        });
        $pool->start();

        $job = Job::create(type: 'slow', maxAttempts: 5, clock: $clock);
        $queue->push($job);

        $metrics = new MetricsCollector();
        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, visibilityTimeout: 1, metrics: $metrics);

        // Out it goes to the first worker, which will be busy for 150ms.
        $this->assertTrue($dispatcher->dispatchPending() > 0);
        $this->assertTrue($dispatcher->isProcessing($job));

        // The deadline passes while it is still working. Nothing has gone
        // wrong; the queue simply has no way to know that.
        $clock->advance(2.0);
        $this->assertSame(1, $dispatcher->requeueExpired());
        $this->assertSame(JobState::READY, $job->getState());

        // And so it is dispatched again - to the other worker, while the
        // first one is still running it.
        $this->assertTrue($dispatcher->dispatchPending() > 0);

        $dispatcher->shutdown(2.0);

        $pids = array_filter(explode("\n", (string) file_get_contents($runFile)));
        @unlink($runFile);

        $this->assertCount(2, $pids, 'the same job ran twice');
        $this->assertCount(2, array_unique($pids), 'in two different workers');
        $this->assertSame(2, $job->getAttempts());
        $this->assertSame(JobState::COMPLETED, $job->getState());

        // The first worker's answer arrived for a delivery nobody was
        // waiting for any more, and was counted rather than applied.
        $this->assertSame(1, $metrics->getCounter(MetricsCollector::STALE_ACKS));
    }

    /**
     * The late answer from an expired delivery must not be mistaken for the
     * answer to the delivery that replaced it.
     *
     * Worker A gets the job and is slow. The visibility deadline passes, so
     * the job goes back to READY and worker B gets it - a duplicate
     * delivery, which is expected. Then worker A finishes and reports.
     *
     * At that moment the job IS in state PROCESSING, because worker B put
     * it there. So a stale-answer check that only looks at the job's state
     * cannot tell the two deliveries apart: it accepts A's answer as B's.
     */
    public function testALateAnswerFromAnExpiredDeliveryIsNotAppliedToTheNewOne(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $metrics = new MetricsCollector();

        // Delivery 1 answers in 100ms, delivery 2 takes 600ms - so A's
        // answer is certain to arrive while B is still working.
        $pool = new WorkerPool(2, static function (Job $job): void {
            usleep($job->getAttempts() === 1 ? 100_000 : 600_000);
        });
        $pool->start();

        $job = Job::create(type: 'slow', maxAttempts: 5, clock: $clock);
        $queue->push($job);

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, visibilityTimeout: 1, metrics: $metrics);

        $this->assertSame(1, $dispatcher->dispatchPending(), 'delivery 1 → worker A');

        $clock->advance(2.0);
        $this->assertSame(1, $dispatcher->requeueExpired());
        $this->assertSame(1, $dispatcher->dispatchPending(), 'delivery 2 → worker B');
        $this->assertSame(2, $job->getAttempts());

        // Worker A's answer, for a delivery nobody is waiting for.
        $this->assertSame(1, $dispatcher->collect(null));

        $this->assertSame(1, $metrics->getCounter(MetricsCollector::STALE_ACKS), 'A answered a dead delivery');
        $this->assertSame(0, $metrics->getCounter(MetricsCollector::JOBS_COMPLETED), 'B has not answered yet');
        $this->assertSame(JobState::PROCESSING, $job->getState(), 'B is still working on it');
        $this->assertTrue($dispatcher->isProcessing($job), 'B\'s delivery is still tracked');

        $dispatcher->shutdown(2.0);
    }

    /**
     * The same two deliveries, followed to the end: the live one decides
     * the job's fate and the expired one changes nothing but a counter.
     *
     * The invariant that matters is the last one. The job stays in flight -
     * leased, with a deadline that can still reclaim it - until the LIVE
     * delivery answers. If the stale answer had released the lease, a job
     * would have been sitting in a worker's hands with nothing left that
     * could ever bring it back.
     */
    public function testOnlyTheLiveDeliveryDecidesTheOutcome(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $metrics = new MetricsCollector();
        $dlq = new DeadLetterQueue($clock);

        // Delivery 1 succeeds quickly; delivery 2 is slower and FAILS. So
        // if the answers were ever confused, the job would come out
        // COMPLETED instead of retried - which is what used to happen.
        $pool = new WorkerPool(2, static function (Job $job): void {
            if ($job->getAttempts() === 1) {
                usleep(100_000);

                return;
            }

            usleep(500_000);

            throw new RuntimeException('delivery 2 failed');
        });
        $pool->start();

        $job = Job::create(type: 'slow', maxAttempts: 5, clock: $clock);
        $queue->push($job);

        $dispatcher = new JobDispatcher(
            $queue,
            $pool,
            new FixedDelayRetry(0),
            $clock,
            visibilityTimeout: 1,
            dlq: $dlq,
            metrics: $metrics,
        );

        $dispatcher->dispatchPending();
        $clock->advance(2.0);
        $dispatcher->requeueExpired();
        $dispatcher->dispatchPending();

        // Delivery 1's late success, then delivery 2's real failure.
        $dispatcher->collect(null);
        $this->assertTrue($dispatcher->isProcessing($job), 'still leased by delivery 2');

        $dispatcher->collect(null);

        $this->assertSame(1, $metrics->getCounter(MetricsCollector::STALE_ACKS));
        $this->assertSame(0, $metrics->getCounter(MetricsCollector::JOBS_COMPLETED), 'delivery 1 did not complete it');
        $this->assertSame(1, $metrics->getCounter(MetricsCollector::JOBS_RETRIED), 'delivery 2 failed, so it retries');
        $this->assertSame(0, $dlq->size());

        // The lease is discharged only now, by the delivery that held it.
        $this->assertFalse($dispatcher->isProcessing($job));
        $this->assertSame(JobState::READY, $job->getState());
        $this->assertSame(1, $queue->readySize());

        $dispatcher->shutdown(2.0);
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
            throw new RuntimeException('Should not execute delayed job early');
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
        $worker->assign(Deliveries::to($worker, $job));

        usleep(150_000);
        posix_kill($worker->getPid(), SIGKILL);
        $pool->poll(null);

        $countAfterFirst = (int) file_get_contents($counterFile);
        $this->assertSame(1, $countAfterFirst);

        $pool->replaceDeadWorkers();
        $recovered = $pool->getAvailableWorker();
        $this->assertNotNull($recovered);
        $recovered->assign(Deliveries::to($recovered, $job));
        $pool->poll(null);

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
        $monitor->track($popped, 1);

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
        $w1->assign(Deliveries::to($w1, $job1));

        $w2 = $pool->getAvailableWorker();
        $this->assertNotNull($w2);
        $w2->assign(Deliveries::to($w2, $job2));

        usleep(10_000);

        posix_kill($w1->getPid(), SIGKILL);
        posix_kill($w2->getPid(), SIGKILL);
        usleep(100_000);

        while ($pool->busyCount() > 0) {
            $pool->poll(null);
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
        $worker->assign(Deliveries::to($worker, $job3));
        $result = $pool->poll(null);
        $this->assertNotNull($result);
        $this->assertTrue($result->getResult()?->isSuccess());

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
            $worker->assign(Deliveries::to($worker, $job));

            usleep(20_000);
            posix_kill($worker->getPid(), SIGKILL);
            $pool->poll(null);

            $pool->replaceDeadWorkers();
        }

        // All 3 jobs are still in the queue, recoverable
        $this->assertSame(3, $queue->size());

        // Process them all successfully
        $pool2 = new WorkerPool(1, Handlers::succeeds());
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
            throw new RuntimeException('fail');
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
            file_put_contents($completedFile, "done\n", FILE_APPEND | LOCK_EX);
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
        $this->assertSame(2, substr_count((string) file_get_contents($completedFile), 'done'));

        $pool->shutdown();
        @unlink($completedFile);
    }

    /**
     * Waits until the process is actually gone. posix_kill only delivers
     * the signal; without this the test races the kernel.
     */
    private function waitForExit(int $pid): void
    {
        for ($i = 0; $i < 200 && posix_kill($pid, 0); $i++) {
            usleep(5_000);
        }
    }
}
