<?php

declare(strict_types=1);

namespace App\Tests\Master;

use App\Dispatcher\JobDispatcher;
use App\Job\Job;
use App\Job\JobState;
use App\Master\QueueRuntime;
use App\Queue\InMemoryQueue;
use App\Tests\Support\FakeClock;
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

        $dispatcher = $this->dispatcher(static function (Job $job): void {}, workers: 2);
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

        $dispatcher = $this->dispatcher(static function (Job $j): void {});
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

        $dispatcher = $this->dispatcher(static function (Job $j): void {});
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
     * PLAN.md Phase 15's last step: the grace period runs out, the worker
     * is killed mid-job, and the job is NOT lost - it was never
     * acknowledged, so the visibility timeout brings it back.
     *
     * This is the trade the bounded shutdown depends on. Without the
     * visibility timeout, killing the worker here would lose the job
     * outright.
     */
    public function testAJobKilledByTheGracePeriodComesBackThroughTheTimeout(): void
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

        // The deadline passes, and the job the killed worker was holding
        // becomes available again.
        $this->clock->advance(31.0);

        $this->assertSame(1, $dispatcher->requeueExpired());
        $this->assertSame(JobState::READY, $job->getState());
        $this->assertSame(1, $this->queue->readySize());
    }

    public function testShutdownStopsAcceptingNewWork(): void
    {
        $dispatcher = $this->dispatcher(static function (Job $job): void {});
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
