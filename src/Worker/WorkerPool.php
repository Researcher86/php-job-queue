<?php

declare(strict_types=1);

namespace App\Worker;

use App\Job\Job;
use Closure;
use InvalidArgumentException;

final class WorkerPool
{
    /** @var list<Worker> */
    private array $workers = [];

    /**
     * @param Closure(Job): void $handler
     */
    public function __construct(
        private readonly int $size,
        private readonly Closure $handler,
    ) {
        if ($size < 1) {
            throw new InvalidArgumentException('A worker pool needs at least one worker');
        }
    }

    public function start(): void
    {
        for ($i = 0; $i < $this->size; $i++) {
            $worker = new Worker($i + 1, $this->handler);
            $worker->markReady();
            $this->workers[] = $worker;
        }
    }

    public function getAvailableWorker(): ?Worker
    {
        foreach ($this->workers as $worker) {
            if ($worker->isAvailable()) {
                return $worker;
            }
        }

        return null;
    }

    /** @return list<Worker> */
    public function getWorkers(): array
    {
        return $this->workers;
    }

    public function count(): int
    {
        return count($this->workers);
    }

    public function add(Worker $worker): void
    {
        $this->workers[] = $worker;
    }
}
