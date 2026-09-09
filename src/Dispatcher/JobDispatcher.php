<?php

declare(strict_types=1);

namespace App\Dispatcher;

use App\Job\Job;
use App\Queue\Queue;
use App\Retry\RetryPolicy;
use App\Support\Clock;
use App\Support\SystemClock;
use App\Worker\WorkerPool;

final readonly class JobDispatcher
{
    public function __construct(
        private Queue $queue,
        private WorkerPool $workerPool,
        private ?RetryPolicy $retryPolicy = null,
        private Clock $clock = new SystemClock(),
    ) {}

    public function dispatchNext(): bool
    {
        $worker = $this->workerPool->getAvailableWorker();
        if ($worker === null) {
            return false;
        }

        $job = $this->queue->pop();
        if ($job === null) {
            return false;
        }

        $job->markProcessing();
        $result = $worker->process($job);

        if ($result->isSuccess()) {
            $job->markCompleted();
        } else {
            $this->handleFailure($job);
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

    private function handleFailure(Job $job): void
    {
        if ($job->getAttempts() < $job->getMaxAttempts()) {
            $delay = $this->retryPolicy?->nextDelay($job) ?? 0;
            $job->markRetry($this->clock->now() + $delay);
            $this->queue->push($job);
            return;
        }

        $job->markFailed();
    }
}
