<?php

declare(strict_types=1);

use App\Dispatcher\JobDispatcher;
use App\Job\Job;
use App\Metrics\MetricsCollector;
use App\Persistence\FileStorage;
use App\Persistence\InMemoryStorage;
use App\Persistence\JobStorage;
use App\Producer\JobFactory;
use App\Producer\Producer;
use App\Queue\InMemoryQueue;
use App\Support\SystemClock;
use App\Worker\WorkerPool;
use InvalidArgumentException;

require __DIR__ . '/../vendor/autoload.php';

/**
 * Throughput and latency under load - PLAN.md Phase 16's stress side.
 *
 *   make bench                                1,000 jobs, 4 workers
 *   make bench ARGS="10000 8"                10,000 jobs, 8 workers
 *   make bench ARGS="100000 8 0"            100,000 no-op jobs
 *   make bench ARGS="10000 8 0 file"        …with an append-only log
 *
 * Arguments: <jobs> <workers> <work-microseconds> <storage>. The third is
 * how long each handler pretends to work; leave it at 0 to measure the
 * queue itself rather than the handler.
 *
 * The fourth is the storage, and there are four of them - three real and
 * one probe:
 *
 *   none     no persistence at all
 *   memory   InMemoryStorage: an array assignment per record
 *   encode   serialises each record and throws it away. Not a usable
 *            backend; it exists to separate the cost of turning a job
 *            into JSON from the cost of writing the JSON down
 *   file     FileStorage: json_encode plus an append
 *
 * Whatever the storage, the run reports how much of the wall clock was
 * spent inside it, which is the number that says whether durability is
 * costing CPU or costing I/O.
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
 *  - Throughput with and without storage. Every job that succeeds first
 *    time costs three appends (READY, PROCESSING, COMPLETED), and the
 *    middle one is what makes its attempt survive a crash. This is where
 *    you find out what that is worth.
 */

/**
 * Wraps a JobStorage and adds up the time spent in it.
 *
 * Part of the benchmark harness, not of the system: the point is to be
 * able to say whether a slower run is slower because of the storage or in
 * spite of it, without guessing from the throughput alone.
 */
final class TimedStorage implements JobStorage
{
    public float $seconds = 0.0;

    public int $writes = 0;

    public function __construct(
        private readonly JobStorage $inner,
    ) {
    }

    public function store(string $key, array $data): void
    {
        $startedAt = microtime(true);
        $this->inner->store($key, $data);
        $this->seconds += microtime(true) - $startedAt;
        $this->writes++;
    }

    public function load(): array
    {
        return $this->inner->load();
    }
}

/**
 * Serialises each record exactly as FileStorage would, then drops it.
 *
 * A measurement probe, not a backend: the difference between this and
 * `file` is what the filesystem costs, and the difference between this and
 * `memory` is what json_encode costs.
 */
final class EncodeOnlyStorage implements JobStorage
{
    public function store(string $key, array $data): void
    {
        json_encode(['key' => $key, 'data' => $data], JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
    }

    public function load(): array
    {
        return [];
    }
}

$jobCount = (int) ($argv[1] ?? 1_000);
$workerCount = (int) ($argv[2] ?? 4);
$workMicroseconds = (int) ($argv[3] ?? 0);
$storageKind = (string) ($argv[4] ?? 'none');

$logPath = sys_get_temp_dir() . '/php-job-queue-bench.log';
@unlink($logPath);

$backend = match ($storageKind) {
    'none' => null,
    'memory' => new InMemoryStorage(),
    'encode' => new EncodeOnlyStorage(),
    'file' => new FileStorage($logPath),
    default => throw new InvalidArgumentException(
        "Unknown storage: {$storageKind} (none|memory|encode|file)",
    ),
};

$storage = $backend === null ? null : new TimedStorage($backend);

$clock = new SystemClock();
$metrics = new MetricsCollector();
$queue = new InMemoryQueue($clock, $storage);
$producer = new Producer($queue, new JobFactory($clock, $metrics));

printf(
    "%d jobs, %d workers, %dus of work each, storage: %s\n",
    $jobCount,
    $workerCount,
    $workMicroseconds,
    $storageKind,
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

$dispatcher = new JobDispatcher($queue, $pool, clock: $clock, storage: $storage, metrics: $metrics);

$startedAt = microtime(true);
$dispatched = $dispatcher->drain();
$elapsed = microtime(true) - $startedAt;

printf("\nqueued   %d jobs in %.3fs (%s/s)\n", $jobCount, $queuedIn, number_format($jobCount / $queuedIn));
printf("drained  %d jobs in %.3fs (%s/s)\n", $dispatched, $elapsed, number_format($dispatched / $elapsed));
printf("memory   %.1f MB peak\n", memory_get_peak_usage(true) / 1_048_576);

if ($storage !== null) {
    printf(
        "storage  %.3fs in %s writes (%.1f%% of the drain, %.1fus each)\n",
        $storage->seconds,
        number_format($storage->writes),
        $storage->seconds / $elapsed * 100,
        $storage->writes === 0 ? 0.0 : $storage->seconds / $storage->writes * 1_000_000,
    );
}

if ($storageKind === 'file' && file_exists($logPath)) {
    $records = count(file($logPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: []);
    printf(
        "log      %s records (%.1f per job), %.1f MB\n",
        number_format($records),
        $records / $jobCount,
        filesize($logPath) / 1_048_576,
    );
}

echo "\n";

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
