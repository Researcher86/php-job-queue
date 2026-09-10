<?php

declare(strict_types=1);

namespace App\Tests\Stress;

use App\Dispatcher\JobDispatcher;
use App\Job\Job;
use App\Metrics\MetricsCollector;
use App\Queue\InMemoryQueue;
use App\Tests\Support\FakeClock;
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
        $pool = new WorkerPool(8, static function (Job $job): void {});
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

    public function testThousandJobsWithMetricsStayConsistent(): void
    {
        $metrics = new MetricsCollector();
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $pool = new WorkerPool(8, static function (Job $job): void {});
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
