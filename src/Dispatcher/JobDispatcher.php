<?php

declare(strict_types=1);

namespace App\Dispatcher;

use App\Job\Job;
use App\Queue\Queue;
use App\Retry\RetryPolicy;
use App\Support\Clock;
use App\Support\SystemClock;
use App\Timeout\VisibilityMonitor;
use App\Worker\WorkerPool;
use App\DLQ\DeadLetterQueue;
use App\Metrics\MetricsCollector;
use App\Persistence\JobStorage;
use Throwable;

final readonly class JobDispatcher
{
    private VisibilityMonitor $monitor;

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
        $this->requeueExpired();

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

        $startedAt = $this->clock->now();
        $result = $worker->process($job);
        $this->metrics?->recordLatency('execution', $this->clock->now() - $startedAt);
        $this->monitor->release($job);

        if ($result->isSuccess()) {
            $job->markCompleted();
            $this->persist($job);
            $this->metrics?->increment('completed');
            $this->metrics?->recordLatency('end_to_end', $this->clock->now() - $job->getCreatedAt());
        } else {
            $this->handleFailure($job, $result->getException());
        }

        return true;
    }

    public function drain(): int
    {
        $dispatched = 0;
        while ($this->dispatchNext()) {
            $dispatched++;
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
}
