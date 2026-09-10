<?php

declare(strict_types=1);

namespace App\Tests\Master;

use App\Dispatcher\JobDispatcher;
use App\Job\Job;
use App\Job\JobState;
use App\Master\QueueRuntime;
use App\Persistence\FileStorage;
use App\Queue\InMemoryQueue;
use App\Tests\Support\FakeClock;
use App\Tests\Support\Handlers;
use App\Worker\WorkerPool;
use Closure;
use PHPUnit\Framework\TestCase;

final class QueueRuntimeTest extends TestCase
{
    private FakeClock $clock;

    private InMemoryQueue $queue;

    protected function setUp(): void
    {
        $this->clock = new FakeClock(1000.0);
        $this->queue = new InMemoryQueue($this->clock);
    }

    public function testTicksMoveJobsThroughToCompletion(): void
    {
        $jobs = [
            Job::create(type: 'a', clock: $this->clock),
            Job::create(type: 'b', clock: $this->clock),
            Job::create(type: 'c', clock: $this->clock),
        ];
        foreach ($jobs as $job) {
            $this->queue->push($job);
        }

        $dispatcher = $this->dispatcher(Handlers::succeeds(), workers: 2);
        $runtime = new QueueRuntime($dispatcher, $this->clock, maxWait: 0.01);
        $dispatcher->start();

        $this->tickUntilQuiet($runtime, $dispatcher);

        foreach ($jobs as $job) {
            $this->assertSame(JobState::COMPLETED, $job->getState());
        }
        $this->assertSame(0, $this->queue->size());

        $dispatcher->shutdown(1.0);
    }

    /**
     * The reason a runtime exists at all rather than just drain(): a job
     * that is not due yet is work that does not exist yet, and the loop has
     * to still be there when it does.
     */
    public function testADelayedJobIsPickedUpOnceItComesDue(): void
    {
        $job = Job::create(type: 'later', clock: $this->clock);
        $this->queue->push($job, delay: 60);

        $dispatcher = $this->dispatcher(Handlers::succeeds());
        $runtime = new QueueRuntime($dispatcher, $this->clock, maxWait: 0.001);
        $dispatcher->start();

        for ($i = 0; $i < 10; $i++) {
            $this->assertSame(0, $runtime->tick());
        }
        $this->assertSame(JobState::DELAYED, $job->getState());

        $this->clock->advance(60.0);
        $this->tickUntilQuiet($runtime, $dispatcher);

        $this->assertSame(JobState::COMPLETED, $job->getState());

        $dispatcher->shutdown(1.0);
    }

    /**
     * A stop requested before the loop starts is honoured rather than
     * cleared - otherwise a SIGTERM landing during startup would be lost.
     * The runtime still starts its workers and still shuts them down; it
     * just never ticks.
     */
    public function testAStopRequestedBeforeRunIsNotLost(): void
    {
        $job = Job::create(type: 'never-runs', clock: $this->clock);
        $this->queue->push($job);

        $dispatcher = $this->dispatcher(Handlers::succeeds());
        $runtime = new QueueRuntime($dispatcher, $this->clock, maxWait: 0.001, shutdownGrace: 1.0);

        $this->assertFalse($runtime->isRunning());

        $runtime->stop();
        $runtime->run();

        $this->assertFalse($runtime->isRunning());
        $this->assertFalse($dispatcher->isAccepting());
        $this->assertSame(JobState::READY, $job->getState());
        $this->assertSame(1, $this->queue->readySize());
    }

    /**
     * PLAN.md Phase 15, with a real signal: SIGTERM arrives while a worker
     * is mid-job, and the job still finishes.
     *
     * The handler signals its own parent - which is this process, running
     * the loop - so the signal lands at a known point: after the job was
     * dispatched and before it was answered.
     */
    public function testSigtermLetsAnInFlightJobFinish(): void
    {
        $job = Job::create(type: 'slow-but-finishes', clock: $this->clock);
        $this->queue->push($job);

        $dispatcher = $this->dispatcher(static function (Job $j): void {
            posix_kill(posix_getppid(), SIGTERM);
            usleep(200_000);
        });
        $runtime = new QueueRuntime($dispatcher, $this->clock, maxWait: 0.01, shutdownGrace: 5.0);

        $runtime->run();

        $this->assertSame(JobState::COMPLETED, $job->getState());
        $this->assertFalse($runtime->isRunning());
    }

    /**
     * PLAN.md Phase 15's last step: the grace period runs out and the
     * worker is killed mid-job. What this proves is that the job is left
     * RECOVERABLE - still PROCESSING, and still holding its lease, so
     * nothing has decided its fate.
     *
     * Note what it does NOT prove. The requeueExpired() call at the end is
     * this test reaching into a dispatcher that is still in memory because
     * the test is holding it. A real process is gone by now, and with it
     * the monitor - so the visibility timeout is not what recovers a job
     * killed by a shutdown. The persistence log is, on the next start:
     * see testAGracePeriodKilledJobIsRestoredFromTheLog().
     */
    public function testAJobKilledByTheGracePeriodKeepsItsLease(): void
    {
        $job = Job::create(type: 'never-finishes', clock: $this->clock);
        $this->queue->push($job);

        $dispatcher = $this->dispatcher(static function (Job $j): void {
            posix_kill(posix_getppid(), SIGTERM);
            sleep(30);
        }, visibilityTimeout: 30);
        $runtime = new QueueRuntime($dispatcher, $this->clock, maxWait: 0.01, shutdownGrace: 0.2);

        $startedAt = microtime(true);
        $runtime->run();
        $elapsed = microtime(true) - $startedAt;

        $this->assertLessThan(5.0, $elapsed, 'the grace period bounded the shutdown');
        $this->assertSame(JobState::PROCESSING, $job->getState(), 'never acknowledged');
        $this->assertTrue($dispatcher->isProcessing($job));

        // With the dispatcher still in memory, the lease is intact and the
        // deadline can still reclaim it. See the docblock for why that is
        // not the same as a real process recovering.
        $this->clock->advance(31.0);

        $this->assertSame(1, $dispatcher->requeueExpired());
        $this->assertSame(JobState::READY, $job->getState());
        $this->assertSame(1, $this->queue->readySize());
    }

    /**
     * The recovery path a real deployment actually takes for a job killed
     * by the shutdown grace period: the log, on the next start.
     *
     * The runtime that would have expired the lease is the one shutting
     * down, so nothing in this process will ever reclaim the job. What
     * survives is the last thing written about it - PROCESSING - and
     * restoreFromStorage() turns that back into READY.
     *
     * Which also states the limit: with no storage attached, this job is
     * lost. That is a property of the configuration, not of the shutdown.
     */
    public function testAGracePeriodKilledJobIsRestoredFromTheLog(): void
    {
        $log = sys_get_temp_dir() . '/php-job-queue-shutdown-' . uniqid('', true) . '.log';

        try {
            $storage = new FileStorage($log);
            $queue = new InMemoryQueue($this->clock, $storage);
            $job = Job::create(type: 'never-finishes', clock: $this->clock);
            $queue->push($job);

            $dispatcher = new JobDispatcher(
                $queue,
                new WorkerPool(1, static function (Job $j): void {
                    posix_kill(posix_getppid(), SIGTERM);
                    sleep(30);
                }),
                clock: $this->clock,
                visibilityTimeout: 30,
                storage: $storage,
            );

            (new QueueRuntime($dispatcher, $this->clock, maxWait: 0.01, shutdownGrace: 0.2))->run();

            // The process is done. In memory the job is PROCESSING and
            // nothing is left running that could expire its lease.
            $this->assertSame(JobState::PROCESSING, $job->getState());

            // A new process, reading the same log.
            $restored = InMemoryQueue::restoreFromStorage($storage, new FakeClock(2000.0));

            $this->assertSame(1, $restored->readySize(), 'the job came back');

            $recovered = $restored->pop();
            $this->assertNotNull($recovered);
            $this->assertSame('never-finishes', $recovered->getType());
            $this->assertSame(JobState::READY, $recovered->getState(), 'available again');
            // The delivery that was killed still counted - an attempt is a
            // delivery, so the next one is attempt 2 of 3.
            $this->assertSame(1, $recovered->getAttempts());
        } finally {
            if (file_exists($log)) {
                unlink($log);
            }
        }
    }

    public function testShutdownStopsAcceptingNewWork(): void
    {
        $dispatcher = $this->dispatcher(Handlers::succeeds());
        $dispatcher->start();

        $this->assertTrue($dispatcher->isAccepting());

        $dispatcher->shutdown(1.0);

        $this->assertFalse($dispatcher->isAccepting());

        $this->queue->push(Job::create(type: 'too-late', clock: $this->clock));
        $this->assertSame(0, $dispatcher->dispatchPending());
        $this->assertSame(1, $this->queue->size());
    }

    private function dispatcher(Closure $handler, int $workers = 1, ?int $visibilityTimeout = null): JobDispatcher
    {
        return new JobDispatcher(
            $this->queue,
            new WorkerPool($workers, $handler),
            clock: $this->clock,
            visibilityTimeout: $visibilityTimeout,
        );
    }

    /** Ticks until the queue is empty and nothing is in flight. */
    private function tickUntilQuiet(QueueRuntime $runtime, JobDispatcher $dispatcher): void
    {
        for ($i = 0; $i < 2_000; $i++) {
            $runtime->tick();

            if ($this->queue->size() === 0 && !$dispatcher->hasWorkInFlight()) {
                return;
            }
        }

        $this->fail('the runtime never went quiet');
    }
}
