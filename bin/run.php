<?php

declare(strict_types=1);

use App\Dispatcher\JobDispatcher;
use App\Job\Job;
use App\Queue\InMemoryQueue;
use App\Queue\PriorityQueue;
use App\Support\SystemClock;
use App\Worker\WorkerPool;

require __DIR__ . '/../vendor/autoload.php';

$clock = new SystemClock();

$handler = static function (Job $job): void {
    printf("  [worker] processed %s (%s)\n", $job->getType(), $job->getPriority()->name);
};

echo "Queueing jobs...\n";
$queue = new InMemoryQueue($clock);
$queue->push(Job::create(type: 'send_email', clock: $clock));
$queue->push(Job::create(type: 'generate_report', clock: $clock));
$queue->push(Job::create(type: 'resize_image', clock: $clock));
$queue->push(Job::create(type: 'delayed_email', clock: $clock), delay: 1);

echo "Processing...\n";
$pool = new WorkerPool(3, $handler);
$pool->start();
$dispatcher = new JobDispatcher($queue, $pool, clock: $clock);
$dispatched = $dispatcher->drain();

printf("Dispatched %d job(s)\n", $dispatched);

echo "Priority queue demo...\n";
$priorityQueue = new PriorityQueue($clock);
$priorityQueue->push(Job::create(type: 'low', clock: $clock));
$priorityQueue->push(Job::create(type: 'high', clock: $clock));
$priorityPool = new WorkerPool(1, $handler);
$priorityPool->start();
$priorityDispatcher = new JobDispatcher($priorityQueue, $priorityPool, clock: $clock);
$priorityDispatcher->drain();