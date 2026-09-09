<?php

declare(strict_types=1);

namespace App\Worker;

final readonly class WorkerResult
{
    public function __construct(
        private Worker $worker,
        private WorkerOutcome $outcome,
    ) {}

    public function getWorker(): Worker
    {
        return $this->worker;
    }

    public function getOutcome(): WorkerOutcome
    {
        return $this->outcome;
    }
}