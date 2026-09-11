<?php

declare(strict_types=1);

namespace App\Tests\Stress;

use App\Dispatcher\JobDispatcher;
use App\Job\Job;
use App\Job\JobPriority;
use App\Metrics\MetricsCollector;
use App\Queue\InMemoryQueue;
use App\Queue\PriorityQueue;
use App\Tests\Support\FakeClock;
use App\Tests\Support\Handlers;
use App\Worker\WorkerPool;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class StressTest extends TestCase
{
    public function testThousandJobsCompleteThroughDispatcher(): void
    {
        $metrics = new MetricsCollector();
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(8, Handlers::succeeds());
        $pool->start();

        for ($i = 0; $i < 1000; $i++) {
            $queue->push(Job::create(type: "job-$i", clock: $clock));
        }

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, metrics: $metrics);
        $dispatched = $dispatcher->drain();

        $this->assertSame(1000, $dispatched);
        $this->assertSame(1000, $metrics->getCounter('completed'));
        $this->assertSame(0, $queue->size());
    }

    /**
     * PLAN.md Phase 16 asks for 10,000 as well. It runs in about half a
     * second, so it stays in the suite; 100,000 is bin/bench.php's job -
     * it takes eight seconds and 96MB, which is a measurement, not a test.
     *
     * What it actually guards is that nothing in the pipeline is O(n^2) in
     * queue depth. The delayed set is a heap and the ready set is an array
     * used as a FIFO; a regression to a sorted-on-every-push list would
     * show up here as a timeout rather than as a wrong answer.
     */
    public function testTenThousandJobsCompleteWithoutDegrading(): void
    {
        $metrics = new MetricsCollector();
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(8, Handlers::succeeds());
        $pool->start();

        for ($i = 0; $i < 10_000; $i++) {
            $queue->push(Job::create(type: 'bulk', clock: $clock));
        }

        $startedAt = microtime(true);
        $dispatched = (new JobDispatcher($queue, $pool, clock: $clock, metrics: $metrics))->drain();
        $elapsed = microtime(true) - $startedAt;

        $this->assertSame(10_000, $dispatched);
        $this->assertSame(10_000, $metrics->getCounter(MetricsCollector::JOBS_COMPLETED));
        $this->assertSame(0, $queue->size());
        $this->assertLessThan(10.0, $elapsed, 'throughput collapsed');
    }

    /**
     * The same depth, all of it delayed and released at once: the heap's
     * insert path under load, and the ordering it promises at the far end
     * of it.
     */
    public function testTenThousandDelayedJobsComeBackInDeadlineOrder(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);

        // Pushed in reverse deadline order, so nothing about the result can
        // be explained by insertion order.
        for ($i = 10_000; $i > 0; $i--) {
            $queue->push(Job::create(type: "job-$i", clock: $clock), delay: $i);
        }

        $this->assertSame(10_000, $queue->delayedSize());
        $this->assertSame(1_001.0, $queue->nextDeadline());

        $clock->advance(10_001.0);

        for ($i = 1; $i <= 10_000; $i++) {
            $job = $queue->pop();
            $this->assertNotNull($job);
            $this->assertSame("job-$i", $job->getType(), 'out of deadline order');
        }

        $this->assertSame(0, $queue->size());
    }

    /**
     * pop() must not get slower as the queue gets deeper.
     *
     * It used to. The ready set was a plain array and pop() used
     * array_shift(), which reindexes it - O(n) per pop, so draining n jobs
     * was O(n^2). Measured then, popping a full queue with no workers
     * involved: 25k took 0.328s, 50k took 1.191s, 100k took 4.815s, 200k
     * took 19.602s. Doubling the depth quadrupled the time.
     *
     * With SplQueue the same 200k takes 0.116s and the rate is flat. This
     * asserts a budget rather than a ratio because a ratio over sub-second
     * timings is mostly noise: 50,000 pops take about 0.03s linear and
     * about 1.2s quadratic, so half a second sits an order of magnitude
     * above one and comfortably below the other.
     *
     * The end-to-end effect was large - 1,000,000 jobs through the
     * dispatcher went from 541s to 30s.
     */
    public function testPoppingDoesNotGetSlowerAsTheQueueGetsDeeper(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $depth = 50_000;

        for ($i = 0; $i < $depth; $i++) {
            $queue->push(Job::create(type: 'x', clock: $clock));
        }

        $startedAt = microtime(true);
        $popped = 0;

        while ($queue->pop() !== null) {
            $popped++;
        }

        $elapsed = microtime(true) - $startedAt;

        $this->assertSame($depth, $popped);
        $this->assertLessThan(0.5, $elapsed, sprintf(
            '%d pops took %.3fs - pop() looks super-linear again',
            $depth,
            $elapsed,
        ));
    }

    /** The same property for the priority lanes, which had the same array_shift. */
    public function testPoppingALaneDoesNotGetSlowerAsItGetsDeeper(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new PriorityQueue($clock);
        $depth = 50_000;

        for ($i = 0; $i < $depth; $i++) {
            $queue->push(Job::create(type: 'x', clock: $clock, priority: JobPriority::NORMAL));
        }

        $startedAt = microtime(true);
        $popped = 0;

        while ($queue->pop() !== null) {
            $popped++;
        }

        $elapsed = microtime(true) - $startedAt;

        $this->assertSame($depth, $popped);
        $this->assertLessThan(1.0, $elapsed, sprintf(
            '%d pops took %.3fs - the lanes look super-linear again',
            $depth,
            $elapsed,
        ));
    }

    public function testThousandJobsWithMetricsStayConsistent(): void
    {
        $metrics = new MetricsCollector();
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(8, Handlers::succeeds());
        $pool->start();

        for ($i = 0; $i < 1000; $i++) {
            $queue->push(Job::create(type: "job-$i", clock: $clock));
        }

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, metrics: $metrics);
        $dispatcher->drain();

        $this->assertSame(1000, $metrics->getCounter('completed'));
        $this->assertSame(0, $metrics->getCounter('failed'));
        $execution = $metrics->getLatencyStats('execution');
        $this->assertNotNull($execution);
        $this->assertSame(1000, $execution['count']);
    }

    public function testMixedOutcomesBalanceMetrics(): void
    {
        $metrics = new MetricsCollector();
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(8, static function (Job $job): void {
            if ($job->getType() === 'fail') {
                throw new RuntimeException('boom');
            }
        });
        $pool->start();

        for ($i = 0; $i < 500; $i++) {
            $queue->push(Job::create(type: 'ok', clock: $clock));
        }
        for ($i = 0; $i < 500; $i++) {
            $queue->push(Job::create(type: 'fail', maxAttempts: 1, clock: $clock));
        }

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock, metrics: $metrics);
        $dispatcher->drain();

        $this->assertSame(500, $metrics->getCounter('completed'));
        $this->assertSame(500, $metrics->getCounter('failed'));
        $this->assertSame(0, $queue->size());
    }

    public function testForkedWorkersProcessJobsInParallel(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(2, static function (Job $job): void {
            usleep(300_000);
        });
        $pool->start();

        $queue->push(Job::create(type: 'a', clock: $clock));
        $queue->push(Job::create(type: 'b', clock: $clock));

        $dispatcher = new JobDispatcher($queue, $pool, clock: $clock);

        $started = microtime(true);
        $dispatcher->drain();
        $elapsed = microtime(true) - $started;

        // Two 300ms jobs on two workers finish in ~300ms, not ~600ms
        $this->assertLessThan(0.55, $elapsed);
        $this->assertSame(0, $queue->size());
    }
}
