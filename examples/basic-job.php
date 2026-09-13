<?php

declare(strict_types=1);

use PhpJobQueue\Dispatcher\JobDispatcher;
use PhpJobQueue\Job\Job;
use PhpJobQueue\Producer\JobFactory;
use PhpJobQueue\Producer\Producer;
use PhpJobQueue\Queue\InMemoryQueue;
use PhpJobQueue\Support\SystemClock;
use PhpJobQueue\Worker\WorkerPool;

require __DIR__ . '/../vendor/autoload.php';

/**
 * The minimal milestone, on its own:
 *
 *   Producer -> Queue -> Worker -> Handler -> ACK -> COMPLETED
 *
 *   make example EXAMPLE=basic-job
 *
 * Nothing here retries, delays, or recovers from anything. Every other
 * example is this plus one mechanism, so it is worth reading first.
 */

$clock = new SystemClock();
$queue = new InMemoryQueue($clock);
$producer = new Producer($queue, new JobFactory($clock));

$job = $producer->dispatch('send_email', ['to' => 'user@example.com']);

printf("dispatched %s as %s\n", $job->getType(), $job->getState()->name);
printf("queue holds %d job(s)\n\n", $queue->size());

$pool = new WorkerPool(1, static function (Job $received): void {
    printf(
        "  [worker %d] handling %s for %s\n",
        getmypid(),
        $received->getType(),
        $received->getPayload()['to'],
    );
});

$dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
$dispatcher->drain();

// The same Job object the producer returned. The worker ran in another
// process against a copy of it; what moved this one to COMPLETED was the
// ACK coming back.
printf("\njob is now %s after %d attempt(s)\n", $job->getState()->name, $job->getAttempts());
printf("queue holds %d job(s)\n", $queue->size());
