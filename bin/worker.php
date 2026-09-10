<?php

declare(strict_types=1);

use App\Dispatcher\JobDispatcher;
use App\Job\Job;
use App\Master\QueueRuntime;
use App\Metrics\MetricsCollector;
use App\Producer\JobFactory;
use App\Producer\Producer;
use App\Queue\InMemoryQueue;
use App\Retry\ExponentialBackoffRetry;
use App\Support\SystemClock;
use App\Worker\WorkerPool;

require __DIR__ . '/../vendor/autoload.php';

/**
 * The long-running queue process - PLAN.md Phase 15, runnable.
 *
 *   make docker-run-worker
 *
 * Then, from another shell, watch a graceful shutdown happen:
 *
 *   docker compose exec php pkill -TERM -f bin/worker.php
 *
 * The job in flight finishes, its result is applied, and only then does the
 * process exit. Send it again during the 15-second job and compare the
 * output; kill -9 the same process instead and the job comes back through
 * the visibility timeout rather than completing.
 *
 * The producer here is in-process, because the point of the script is the
 * runtime, not the transport. A real deployment would have the producer
 * somewhere else and the queue in shared storage.
 */

$clock = new SystemClock();
$metrics = new MetricsCollector();
$queue = new InMemoryQueue($clock);

$producer = new Producer($queue, new JobFactory($clock, $metrics));

foreach (['send_email', 'generate_report', 'resize_image'] as $type) {
    $producer->dispatch($type, ['at' => date('H:i:s')]);
}

// One job slow enough to still be running when you send the signal.
$producer->dispatch('slow_report', ['seconds' => 15]);

// And one that is not due for a while, so the loop has a reason to stay up
// after the queue looks empty.
$producer->dispatch('delayed_email', [], delay: 20);

$pool = new WorkerPool(3, static function (Job $job): void {
    $seconds = (int) ($job->getPayload()['seconds'] ?? 0);

    printf("  [worker %d] %s started\n", getmypid(), $job->getType());
    sleep($seconds);
    printf("  [worker %d] %s done\n", getmypid(), $job->getType());
}, $metrics);

$dispatcher = new JobDispatcher(
    $queue,
    $pool,
    new ExponentialBackoffRetry(),
    $clock,
    visibilityTimeout: 30,
    metrics: $metrics,
);

$runtime = new QueueRuntime($dispatcher, $clock, shutdownGrace: 20.0);

printf("queue runtime up, pid %d - SIGTERM to stop gracefully\n", getmypid());
printf("gauges: %s\n", json_encode($dispatcher->observe()->toArray()));

$runtime->run();

printf("stopped. gauges: %s\n", json_encode($dispatcher->observe()->toArray()));
printf("counters: %s\n", json_encode($metrics->getCounters()));
