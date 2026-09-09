<?php

declare(strict_types=1);

namespace App\Dispatcher;

use App\DLQ\DeadLetterQueue;
use App\Job\Job;
use App\Metrics\MetricsCollector;
use App\Persistence\JobStorage;
use App\Queue\Queue;
use App\Retry\RetryPolicy;
use App\Support\Clock;
use App\Support\SystemClock;
use App\Timeout\VisibilityMonitor;
use App\Worker\Worker;
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
        $this->monitor = new VisibilityMonitor($visibilityTimeout ?? 0, $clock);
    }

    public function dispatchNext(): bool
    {
        if (!$this->accepting) {
            return false;
        }

        $worker = $this->workerPool->getAvailableWorker();
        if ($worker === null) {
            return false;
        }

        $job = $this->queue->pop();
        if ($job === null) {
            return false;
        }

        $job->markProcessing();
        $this->monitor->track($job);
        $this->recordStart($job);
        $worker->assign($job);

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
                while (($worker = $this->workerPool->getAvailableWorker()) !== null) {
                    $job = $this->queue->pop();
                    if ($job === null) {
                        break;
                    }

                    $job->markProcessing();
                    $this->monitor->track($job);
                    $this->recordStart($job);
                    $worker->assign($job);
                    $dispatched++;
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

    public function isAccepting(): bool
    {
        return $this->accepting;
    }

    public function shutdown(): void
    {
        $this->accepting = false;
        $this->workerPool->drain();
    }

    private function applyResult(Worker $worker, WorkerOutcome $outcome): void
    {
        $job = $outcome->getJob();
        $result = $outcome->getResult();
        $this->monitor->release($job);

        if ($result === null) {
            $job->markRetry($this->clock->now());
            $this->queue->push($job);
            $this->metrics?->increment('worker_crashes');
            $this->workerPool->replaceDeadWorkers();
            return;
        }

        $this->metrics?->recordLatency('execution', $this->clock->now() - $this->takeStart($job, $outcome->getStartedAt()));

        if ($result->isSuccess()) {
            $job->markCompleted();
            $this->persist($job);
            $this->metrics?->increment('completed');
            $this->metrics?->recordLatency('end_to_end', $this->clock->now() - $job->getCreatedAt());
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
            $this->metrics?->increment('retried');
            return;
        }

        $job->markFailed();
        $this->persist($job);
        $this->metrics?->increment('failed');
        if ($this->dlq !== null && $exception !== null) {
            $this->dlq->add($job, $exception);
            $this->metrics?->increment('dlq');
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