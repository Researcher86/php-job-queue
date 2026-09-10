<?php

declare(strict_types=1);

use App\Dispatcher\JobDispatcher;
use App\Job\Job;
use App\Metrics\MetricsCollector;
use App\Producer\JobFactory;
use App\Producer\Producer;
use App\Queue\InMemoryQueue;
use App\Support\SystemClock;
use App\Worker\WorkerPool;

require __DIR__ . '/../vendor/autoload.php';

/**
 * Throughput and latency under load - PLAN.md Phase 16's stress side.
 *
 *   make bench                              1,000 jobs, 4 workers
 *   make bench ARGS="10000 8"              10,000 jobs, 8 workers
 *   make bench ARGS="100000 8 0"          100,000 no-op jobs
 *
 * Arguments: <jobs> <workers> <work-microseconds>. The third one is how
 * long each handler pretends to work; leave it at 0 to measure the queue
 * itself rather than the handler.
 *
 * What to read in the output:
 *
 *  - Queue wait against execution. With more jobs than workers, queue wait
 *    grows and execution does not: the jobs are not slower, they are
 *    waiting. That is the whole reason the two are measured apart.
 *  - Throughput against worker count. Doubling the workers on no-op jobs
 *    does not double throughput - the dispatcher is one process writing to
 *    one socket at a time, and at some point it, not the workers, is the
 *    limit.
 */

$jobCount = (int) ($argv[1] ?? 1_000);
$workerCount = (int) ($argv[2] ?? 4);
$workMicroseconds = (int) ($argv[3] ?? 0);

$clock = new SystemClock();
$metrics = new MetricsCollector();
$queue = new InMemoryQueue($clock);
$producer = new Producer($queue, new JobFactory($clock, $metrics));

printf(
    "%d jobs, %d workers, %dus of work each\n",
    $jobCount,
    $workerCount,
    $workMicroseconds,
);

$queuedAt = microtime(true);
for ($i = 0; $i < $jobCount; $i++) {
    $producer->dispatch('bench', ['n' => $i]);
}
$queuedIn = microtime(true) - $queuedAt;

$pool = new WorkerPool($workerCount, static function (Job $job) use ($workMicroseconds): void {
    if ($workMicroseconds > 0) {
        usleep($workMicroseconds);
    }
}, $metrics);

$dispatcher = new JobDispatcher($queue, $pool, clock: $clock, metrics: $metrics);

$startedAt = microtime(true);
$dispatched = $dispatcher->drain();
$elapsed = microtime(true) - $startedAt;

printf("\nqueued   %d jobs in %.3fs (%s/s)\n", $jobCount, $queuedIn, number_format($jobCount / $queuedIn));
printf("drained  %d jobs in %.3fs (%s/s)\n", $dispatched, $elapsed, number_format($dispatched / $elapsed));
printf("memory   %.1f MB peak\n\n", memory_get_peak_usage(true) / 1_048_576);

foreach ([
    MetricsCollector::LATENCY_QUEUE_WAIT => 'queue wait',
    MetricsCollector::LATENCY_EXECUTION => 'execution',
    MetricsCollector::LATENCY_END_TO_END => 'end to end',
] as $metric => $label) {
    $stats = $metrics->getLatencyStats($metric);

    if ($stats === null) {
        continue;
    }

    printf(
        "%-11s avg %7.2fms   min %7.2fms   max %7.2fms   n=%d\n",
        $label,
        $stats['avg'] * 1_000,
        $stats['min'] * 1_000,
        $stats['max'] * 1_000,
        $stats['count'],
    );
}

printf("\ncounters %s\n", json_encode($metrics->getCounters()));
