<?php

declare(strict_types=1);

namespace App\Dispatcher;

use App\DLQ\DeadLetterQueue;
use App\Job\Job;
use App\Metrics\MetricsCollector;
use App\Metrics\QueueMetrics;
use App\Persistence\JobStorage;
use App\Queue\Queue;
use App\Retry\RetryPolicy;
use App\Support\Clock;
use App\Support\SystemClock;
use App\Timeout\VisibilityMonitor;
use App\Worker\Worker;
use App\Worker\WorkerDiedException;
use App\Worker\WorkerOutcome;
use App\Worker\WorkerPool;
use Throwable;

final class JobDispatcher
{
    private VisibilityMonitor $monitor;

    private bool $accepting = true;

    /** @var array<string, float> */
    private array $startedAt = [];

    public function __construct(
        private Queue $queue,
        private WorkerPool $workerPool,
        private ?RetryPolicy $retryPolicy = null,
        private Clock $clock = new SystemClock(),
        ?int $visibilityTimeout = null,
        private ?DeadLetterQueue $dlq = null,
        private ?JobStorage $storage = null,
        private ?MetricsCollector $metrics = null,
    ) {
        $this->monitor = new VisibilityMonitor($visibilityTimeout, $clock);
    }

    public function dispatchNext(): bool
    {
        if (!$this->accepting) {
            return false;
        }

        $this->workerPool->maintain();

        $worker = $this->workerPool->getAvailableWorker();
        if ($worker === null) {
            return false;
        }

        $job = $this->queue->pop();
        if ($job === null) {
            return false;
        }

        if (!$this->dispatch($worker, $job)) {
            return false;
        }

        $result = $this->workerPool->poll(true);
        if ($result !== null) {
            $this->applyResult($result->getWorker(), $result->getOutcome());
        }

        return true;
    }

    public function drain(): int
    {
        $dispatched = 0;
        $this->workerPool->start();

        try {
            while (true) {
                $this->workerPool->maintain();

                while (($worker = $this->workerPool->getAvailableWorker()) !== null) {
                    $job = $this->queue->pop();
                    if ($job === null) {
                        break;
                    }

                    if ($this->dispatch($worker, $job)) {
                        $dispatched++;
                    }
                }

                $result = $this->workerPool->poll();
                if ($result === null) {
                    if ($this->workerPool->busyCount() === 0) {
                        break;
                    }
                    $result = $this->workerPool->poll(true);
                    if ($result === null) {
                        break;
                    }
                }

                $this->applyResult($result->getWorker(), $result->getOutcome());
            }
        } finally {
            $this->workerPool->shutdown();
        }

        return $dispatched;
    }

    public function requeueExpired(): int
    {
        $count = 0;
        foreach ($this->monitor->requeueExpired() as $expired) {
            $this->queue->push($expired);
            $count++;
        }

        return $count;
    }

    public function isProcessing(Job $job): bool
    {
        return $this->monitor->isProcessing($job);
    }

    /**
     * A gauge reading of the whole system, right now - PLAN.md Phase 14.
     *
     * Read from the live objects rather than accumulated as events, because
     * a gauge that is maintained by hand drifts: every push, pop, crash and
     * requeue would have to remember to adjust it, and the one path that
     * forgets is invisible.
     */
    public function observe(): QueueMetrics
    {
        return new QueueMetrics(
            ready: $this->queue->readySize(),
            delayed: $this->queue->delayedSize(),
            processing: $this->monitor->size(),
            workers: $this->workerPool->count(),
            busyWorkers: $this->workerPool->busyCount(),
            deadLettered: $this->dlq?->size() ?? 0,
        );
    }

    public function isAccepting(): bool
    {
        return $this->accepting;
    }

    public function shutdown(): void
    {
        $this->accepting = false;
        $this->workerPool->drain();

        // Wait for in-flight jobs to finish before tearing the pool down.
        while ($this->workerPool->busyCount() > 0) {
            $result = $this->workerPool->poll(true);
            if ($result !== null) {
                $this->applyResult($result->getWorker(), $result->getOutcome());
            }
        }

        $this->workerPool->shutdown();
    }

    /**
     * Hands one job to one worker, and returns whether it got there.
     *
     * The order matters and is the invariant of PLAN.md Phase 5: the job is
     * marked PROCESSING and registered with the visibility monitor BEFORE
     * it is written to the socket, so there is no instant at which the job
     * is neither in the queue nor accounted for as in flight. A job must
     * not be able to fall between the two.
     *
     * If the worker turns out to be dead (killed while idle, too recently
     * for maintain() to have noticed), the job comes straight back to the
     * queue and this returns false. The attempt it consumed is not given
     * back: an attempt in this system counts a DELIVERY, not a successful
     * run, which is the same accounting the visibility timeout uses when it
     * requeues a job whose worker vanished. Real queues count receipts too.
     */
    private function dispatch(Worker $worker, Job $job): bool
    {
        // Read before markProcessing(), which clears it: the job is no
        // longer waiting, so it no longer has a time at which it became
        // available. What it waited FOR is measured from that instant, not
        // from creation - a job deliberately delayed by an hour did not
        // spend an hour queued behind a busy pool.
        $availableAt = $job->getAvailableAt();

        $job->markProcessing();
        $this->monitor->track($job);
        $this->recordStart($job);

        if ($availableAt !== null) {
            $this->metrics?->recordLatency(
                MetricsCollector::LATENCY_QUEUE_WAIT,
                max(0.0, $this->clock->now() - $availableAt),
            );
        }

        try {
            $worker->assign($job);
        } catch (WorkerDiedException) {
            $this->monitor->release($job);
            $this->takeStart($job, $this->clock->now());
            $job->markRetry($this->clock->now());
            $this->queue->push($job);
            $this->workerPool->maintain();

            return false;
        }

        return true;
    }

    private function applyResult(Worker $worker, WorkerOutcome $outcome): void
    {
        $job = $outcome->getJob();
        $result = $outcome->getResult();
        $this->monitor->release($job);

        if ($result === null) {
            $job->markRetry($this->clock->now());
            $this->queue->push($job);
            $this->workerPool->replaceDeadWorkers();
            return;
        }

        $this->metrics?->recordLatency(
            MetricsCollector::LATENCY_EXECUTION,
            $this->clock->now() - $this->takeStart($job, $outcome->getStartedAt()),
        );

        if ($result->isSuccess()) {
            $job->markCompleted();
            $this->persist($job);
            $this->metrics?->increment(MetricsCollector::JOBS_COMPLETED);
            $this->metrics?->recordLatency(
                MetricsCollector::LATENCY_END_TO_END,
                $this->clock->now() - $job->getCreatedAt(),
            );
            return;
        }

        $this->handleFailure($job, $result->getException());
    }

    private function handleFailure(Job $job, ?Throwable $exception): void
    {
        if ($job->getAttempts() < $job->getMaxAttempts()) {
            $delay = $this->retryPolicy?->nextDelay($job) ?? 0;
            $job->markRetry($this->clock->now() + $delay);
            $this->queue->push($job);
            $this->persist($job);
            $this->metrics?->increment(MetricsCollector::JOBS_RETRIED);
            return;
        }

        $job->markFailed();
        $this->persist($job);
        $this->metrics?->increment(MetricsCollector::JOBS_FAILED);
        if ($this->dlq !== null && $exception !== null) {
            $this->dlq->add($job, $exception);
            $this->metrics?->increment(MetricsCollector::JOBS_DEAD_LETTERED);
        }
    }

    private function persist(Job $job): void
    {
        $this->storage?->store($job->getId()->toString(), $job->toArray());
    }

    private function recordStart(Job $job): void
    {
        $this->startedAt[$job->getId()->toString()] = $this->clock->now();
    }

    private function takeStart(Job $job, float $fallback): float
    {
        $id = $job->getId()->toString();
        $startedAt = $this->startedAt[$id] ?? $fallback;
        unset($this->startedAt[$id]);

        return $startedAt;
    }
}
