<?php

declare(strict_types=1);

use App\Dispatcher\JobDispatcher;
use App\Job\Job;
use App\Job\JobState;
use App\Master\QueueRuntime;
use App\Metrics\MetricsCollector;
use App\Producer\JobFactory;
use App\Producer\Producer;
use App\Queue\InMemoryQueue;
use App\Support\SystemClock;
use App\Worker\WorkerPool;

require __DIR__ . '/../vendor/autoload.php';

/**
 * kill -9 a worker while it is holding a job, and watch the job survive it.
 *
 *   make example EXAMPLE=worker-crash
 *
 * The distinction the whole architecture is built on:
 *
 *   worker lifecycle  !=  job lifecycle
 *
 * The worker is gone for good - a SIGKILLed process runs no cleanup, sends
 * no NACK, and cannot be asked what happened. The job is still owed an
 * answer, so it goes back to READY and is handed to the replacement.
 *
 * Notice the attempt count afterwards: the job ran twice. Nobody could tell
 * whether the first worker did the work before it died, so the queue had to
 * assume it had not. That is at-least-once delivery, and the reason handlers
 * have to be idempotent - see examples/../src/Idempotency for a handler
 * that survives it.
 */

$clock = new SystemClock();
$metrics = new MetricsCollector();
$queue = new InMemoryQueue($clock);
$producer = new Producer($queue, new JobFactory($clock, $metrics));

$job = $producer->dispatch('resize_image', ['file' => 'cat.jpg'], maxAttempts: 5);

$attemptFile = tempnam(sys_get_temp_dir(), 'crash-');
$pool = new WorkerPool(1, static function (Job $received) use ($attemptFile): void {
    $attempt = (int) file_get_contents($attemptFile) + 1;
    file_put_contents($attemptFile, (string) $attempt);

    printf("  [worker %d] attempt %d, working...\n", getmypid(), $attempt);

    // First time round, die halfway through - after the side effect would
    // have started, before anything is acknowledged. The worst moment, and
    // the one worth testing.
    if ($attempt === 1) {
        usleep(100_000);
        printf("  [worker %d] about to be killed mid-job\n", getmypid());
        posix_kill(posix_getpid(), SIGKILL);
    }

    printf("  [worker %d] finished cleanly\n", getmypid());
}, $metrics);

// A visibility timeout is what makes this recoverable at all: if the crash
// is not detected on the socket, the deadline catches it instead.
$dispatcher = new JobDispatcher($queue, $pool, clock: $clock, visibilityTimeout: 5, metrics: $metrics);
$runtime = new QueueRuntime($dispatcher, $clock);

$dispatcher->start();
printf("pool: %s\n\n", json_encode($dispatcher->observe()->toArray()));

while ($job->getState() !== JobState::COMPLETED) {
    $runtime->tick();
}

$dispatcher->shutdown(1.0);
@unlink($attemptFile);

printf("\njob is %s after %d attempt(s)\n", $job->getState()->name, $job->getAttempts());
printf("counters %s\n", json_encode($metrics->getCounters()));
printf("\nthe worker that died was replaced; the job it held was not lost.\n");
