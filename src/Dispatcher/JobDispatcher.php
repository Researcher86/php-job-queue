<?php

declare(strict_types=1);

namespace App\Dispatcher;

use App\Queue\Queue;
use App\Worker\WorkerPool;

final readonly class JobDispatcher
{
    public function __construct(
        private Queue $queue,
        private WorkerPool $workerPool,
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

        $worker->process($job);

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
}
