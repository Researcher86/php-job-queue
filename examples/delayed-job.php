<?php

declare(strict_types=1);

use App\Dispatcher\JobDispatcher;
use App\Job\Job;
use App\Master\QueueRuntime;
use App\Producer\JobFactory;
use App\Producer\Producer;
use App\Queue\InMemoryQueue;
use App\Support\SystemClock;
use App\Worker\WorkerPool;

require __DIR__ . '/../vendor/autoload.php';

/**
 * A job that must not run yet:
 *
 *   CREATED -> DELAYED -> (deadline) -> READY -> PROCESSING -> COMPLETED
 *
 *   make example EXAMPLE=delayed-job
 *
 * This one needs the runtime rather than drain(), and that is the lesson:
 * a job due in two seconds is work that does not exist yet, so something
 * has to still be running when it does.
 *
 * The three jobs are dispatched in the wrong order on purpose - 3s, 1s, 2s -
 * to show that what comes back out is deadline order, not push order. That
 * ordering is the min-heap in Scheduler/DelayedJobScheduler.
 */

$clock = new SystemClock();
$queue = new InMemoryQueue($clock);
$producer = new Producer($queue, new JobFactory($clock));

$producer->dispatch('third', [], delay: 3);
$producer->dispatch('first', [], delay: 1);
$producer->dispatch('second', [], delay: 2);
$producer->dispatch('immediate');

printf("queued: %d ready, %d waiting on a deadline\n\n", $queue->readySize(), $queue->delayedSize());

$startedAt = microtime(true);
$pool = new WorkerPool(2, static function (Job $job) use ($startedAt): void {
    printf("  [+%.1fs] %s ran\n", microtime(true) - $startedAt, $job->getType());
});

$dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
$runtime = new QueueRuntime($dispatcher, $clock);

// A runtime normally stops on SIGTERM. Here it is driven tick by tick so
// the script ends on its own once the last deadline has passed.
$dispatcher->start();
while ($queue->size() > 0 || $dispatcher->hasWorkInFlight()) {
    $runtime->tick();
}
$dispatcher->shutdown(1.0);

printf("\nall four done in %.1fs\n", microtime(true) - $startedAt);
