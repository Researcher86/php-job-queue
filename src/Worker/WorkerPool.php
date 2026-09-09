<?php

declare(strict_types=1);

namespace App\Worker;

use App\Job\Job;
use App\Metrics\MetricsCollector;
use Closure;
use InvalidArgumentException;

final class WorkerPool
{
    /** @var list<Worker> */
    private array $workers = [];

    /**
     * @param Closure(Job): mixed $handler
     */
    public function __construct(
        private readonly int $size,
        private readonly Closure $handler,
        private readonly ?MetricsCollector $metrics = null,
    ) {
        if ($size < 1) {
            throw new InvalidArgumentException('A worker pool needs at least one worker');
        }
    }

    public function start(): void
    {
        if ($this->workers !== []) {
            return;
        }

        for ($i = 0; $i < $this->size; $i++) {
            $worker = new Worker($i + 1, $this->handler);
            $worker->spawn();
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

    public function busyCount(): int
    {
        $count = 0;
        foreach ($this->workers as $worker) {
            if ($worker->isBusy()) {
                $count++;
            }
        }

        return $count;
    }

    public function poll(bool $block = false): ?WorkerResult
    {
        do {
            foreach ($this->workers as $worker) {
                if (!$worker->isBusy()) {
                    continue;
                }
                $outcome = $worker->collect(false);
                if ($outcome !== null) {
                    return new WorkerResult($worker, $outcome);
                }
            }

            if (!$block || $this->busyCount() === 0) {
                return null;
            }

            usleep(1000);
        } while (true);
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

    /** @return list<Worker> */
    public function getDeadWorkers(): array
    {
        return array_values(array_filter(
            $this->workers,
            static fn (Worker $worker): bool => $worker->isDead(),
        ));
    }

    public function hasDeadWorkers(): bool
    {
        return $this->getDeadWorkers() !== [];
    }

    public function replaceDeadWorkers(): int
    {
        $replaced = 0;
        foreach ($this->workers as $i => $worker) {
            if (!$worker->isDead()) {
                continue;
            }

            $worker->shutdown();
            $replacement = new Worker($worker->getId(), $this->handler);
            $replacement->spawn();
            $this->workers[$i] = $replacement;
            $replaced++;
            $this->metrics?->increment('worker_crashes');
        }

        return $replaced;
    }

    public function drain(): void
    {
        foreach ($this->workers as $worker) {
            $worker->drain();
        }
    }

    public function isDraining(): bool
    {
        foreach ($this->workers as $worker) {
            if (!$worker->isDraining() && !$worker->isDead()) {
                return false;
            }
        }

        return true;
    }

    public function shutdown(): void
    {
        foreach ($this->workers as $worker) {
            $worker->shutdown();
        }
        $this->workers = [];
    }

    public function __destruct()
    {
        $this->shutdown();
    }
}