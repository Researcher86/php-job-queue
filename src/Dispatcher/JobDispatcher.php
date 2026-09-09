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
        $result = $worker->process($job);
        $this->monitor->release($job);

        if ($result->isSuccess()) {
            $job->markCompleted();
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
            return;
        }

        $job->markFailed();
        if ($this->dlq !== null && $exception !== null) {
            $this->dlq->add($job, $exception);
        }
    }
}
