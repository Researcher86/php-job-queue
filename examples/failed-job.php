<?php

declare(strict_types=1);

use App\Dispatcher\JobDispatcher;
use App\DLQ\DeadLetterQueue;
use App\Job\Job;
use App\Metrics\MetricsCollector;
use App\Producer\JobFactory;
use App\Producer\Producer;
use App\Queue\InMemoryQueue;
use App\Retry\ExponentialBackoffRetry;
use App\Support\SystemClock;
use App\Worker\WorkerPool;

require __DIR__ . '/../vendor/autoload.php';

/**
 * A handler that always throws, followed all the way down:
 *
 *   attempt 1 -> NACK -> retry -> attempt 2 -> NACK -> retry
 *   -> attempt 3 -> NACK -> attempts exhausted -> DLQ
 *
 *   make example EXAMPLE=failed-job
 *
 * The retry delays here are the real exponential backoff (1s, 2s), so the
 * script takes a few seconds - that wait IS the mechanism, and skipping it
 * would be skipping the point.
 *
 * Watch what does NOT happen: the job never disappears, and it never
 * retries forever. A job that cannot succeed ends up somewhere a human can
 * find it.
 */

$clock = new SystemClock();
$metrics = new MetricsCollector();
$queue = new InMemoryQueue($clock);
$dlq = new DeadLetterQueue($clock);
$producer = new Producer($queue, new JobFactory($clock, $metrics));

$job = $producer->dispatch('charge_card', ['order' => 123], maxAttempts: 3);

$pool = new WorkerPool(1, static function (Job $received): void {
    printf("  [worker] attempt %d of %s\n", $received->getAttempts(), $received->getType());

    throw new RuntimeException('the payment gateway is down');
});

$dispatcher = new JobDispatcher(
    $queue,
    $pool,
    new ExponentialBackoffRetry(),
    $clock,
    dlq: $dlq,
    metrics: $metrics,
);

// drain() returns when the queue is empty, and a retry scheduled a second
// into the future leaves it empty for that second - so the loop runs until
// the job reaches a state it cannot come back from.
$pool->start();
$backingOffFrom = null;
while (!$job->getState()->isTerminal()) {
    $dispatcher->dispatchNext();

    if ($queue->readySize() === 0 && $queue->delayedSize() > 0) {
        if ($backingOffFrom !== $job->getAttempts()) {
            $backingOffFrom = $job->getAttempts();
            printf("  ...backing off before attempt %d\n", $job->getAttempts() + 1);
        }

        usleep(50_000);
    }
}
$dispatcher->shutdown(1.0);

printf("\njob is %s after %d attempt(s)\n", $job->getState()->name, $job->getAttempts());
printf("dead letter queue holds %d record(s)\n", $dlq->size());

foreach ($dlq->all() as $record) {
    printf(
        "  %s failed %d time(s), last error: %s\n",
        $record->getJob()->getType(),
        $record->getAttempts(),
        $record->getException()->getMessage(),
    );
}

// The DLQ is not a graveyard: a record can be put back once whatever was
// broken is fixed.
$requeued = $dlq->retry($job->getId()->toString());
printf("\nafter a manual retry: job is %s, DLQ holds %d\n", $requeued?->getState()->name ?? '-', $dlq->size());
printf("counters %s\n", json_encode($metrics->getCounters()));
