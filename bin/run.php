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
 * `make run` - the whole lifecycle once, in one process, in a few lines:
 *
 *   Producer -> Queue -> Dispatcher -> Worker -> Handler -> ACK -> COMPLETED
 *
 * Deliberately the boring path. One mechanism at a time lives in
 * examples/ (`make example EXAMPLE=worker-crash`), the long-running
 * process is bin/worker.php, and load is bin/bench.php.
 */

$clock = new SystemClock();
$metrics = new MetricsCollector();
$queue = new InMemoryQueue($clock);
$producer = new Producer($queue, new JobFactory($clock, $metrics));

foreach (['send_email', 'generate_report', 'resize_image'] as $type) {
    $producer->dispatch($type, ['queued_at' => date('H:i:s')]);
}

$pool = new WorkerPool(3, static function (Job $job): void {
    printf("  [worker %d] %s\n", getmypid(), $job->getType());
}, $metrics);

$dispatcher = new JobDispatcher($queue, $pool, clock: $clock, metrics: $metrics);

printf("%d job(s) queued, %d worker(s)\n\n", $queue->size(), 3);

$dispatched = $dispatcher->drain();

printf("\ndispatched %d, queue now holds %d\n", $dispatched, $queue->size());
printf("counters %s\n", json_encode($metrics->getCounters()));
