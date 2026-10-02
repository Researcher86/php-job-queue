<?php

declare(strict_types=1);

namespace PhpJobQueue\Worker;

use Closure;
use InvalidArgumentException;
use PhpJobQueue\Job\Job;
use PhpJobQueue\Metrics\MetricsCollector;

/**
 * The fixed set of worker processes, and everything the Master side needs
 * to know about them: which one is free, which one answered, which one
 * died.
 *
 * Size is fixed on purpose. Autoscaling is a php-worker-pool concern; here
 * the interesting question is what happens to a JOB when a worker
 * disappears, and a constant pool size makes that easier to watch.
 */
final class WorkerPool
{
    /** @var list<Worker> */
    private array $workers = [];

    /**
     * Set once drain() has been called. What stops maintain() from
     * cheerfully forking a replacement for a worker that died while the
     * whole pool was on its way out.
     */
    private bool $draining = false;

    public function __construct(
        private readonly int $size,
        /** @var Closure(Job): mixed */
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
        return array_find($this->workers, static fn (Worker $worker): bool => $worker->isAvailable());
    }

    /**
     * How many workers are holding a job. Counts a worker that was drained
     * mid-job too - it is still working, which is exactly what a shutdown
     * needs to wait for.
     */
    public function busyCount(): int
    {
        return count(array_filter($this->workers, static fn (Worker $worker): bool => $worker->isWorking()));
    }

    /**
     * Collects worker deaths that poll() cannot see, then restores the lost
     * capacity. Returns how many workers were replaced.
     *
     * The two halves answer different questions: reap() notices a worker
     * that died while idle (see Worker::reap() for why only idle),
     * replaceDeadWorkers() forks a new process to take its place. A caller
     * that only wants to know, without replacing, calls reap() itself.
     *
     * Meant to be called once per loop iteration - it costs one non-blocking
     * waitpid per idle worker.
     */
    public function maintain(): int
    {
        $this->reap();

        return $this->replaceDeadWorkers();
    }

    /**
     * Non-blocking check for workers that died while not holding a job.
     * Returns how many this call found.
     */
    public function reap(): int
    {
        $reaped = 0;

        foreach ($this->workers as $worker) {
            if ($worker->reap()) {
                $reaped++;
            }
        }

        return $reaped;
    }

    /**
     * Waits for one worker to answer, and reports which worker answered
     * with what.
     *
     * Only BUSY workers are selected on: an idle worker's socket has
     * nothing coming. That is also why a crash while busy is detected here
     * (the socket reaches EOF, and collect() returns an outcome with a null
     * result, naming the job that was lost) and a crash while idle is not -
     * see maintain().
     *
     * $timeout is in seconds: 0.0 polls, null waits indefinitely, anything
     * else waits at most that long. With no busy worker to wait on it
     * returns null at once whatever the timeout says - there is nothing
     * that could arrive, so waiting for it would be a deadlock in the
     * null case and a wasted sleep in the others. A caller that wants to
     * idle for a while does its own sleeping.
     */
    public function poll(?float $timeout = 0.0): ?WorkerOutcome
    {
        $this->reap();

        do {
            $read = [];
            $streamWorkers = [];
            foreach ($this->workers as $worker) {
                // isWorking(), not isBusy(): a worker drained mid-job is
                // DRAINING, and its last answer still has to be read.
                if (!$worker->isWorking()) {
                    continue;
                }
                $stream = $worker->getStream();
                if (!is_resource($stream)) {
                    continue;
                }
                $read[] = $stream;
                $streamWorkers[(int) $stream] = $worker;
            }

            if ($read === []) {
                return null;
            }

            $write = null;
            $except = null;
            $ready = @stream_select($read, $write, $except, ...Worker::selectTimeout($timeout));

            if ($ready === false || $ready === 0) {
                // Nothing yet. Only an indefinite wait goes round again;
                // a bounded one has spent its budget.
                if ($timeout === null) {
                    continue;
                }

                return null;
            }

            foreach ($read as $stream) {
                // Already selected as readable, so this does not wait.
                $outcome = $streamWorkers[(int) $stream]->collect(0.0);

                if ($outcome !== null) {
                    return $outcome;
                }
            }

            if ($timeout !== null) {
                return null;
            }
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

    /**
     * Forks a replacement for every DEAD worker, in place, keeping the pool
     * at its configured size. Returns how many were replaced.
     *
     * Each replacement counts as a worker crash: the counter is what tells
     * "the workers keep dying" apart from "the jobs keep failing", which
     * are different problems with different fixes.
     *
     * A pool that is draining replaces nothing - the workers are supposed
     * to be leaving.
     */
    public function replaceDeadWorkers(): int
    {
        if ($this->draining) {
            return 0;
        }

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
            $this->metrics?->increment(MetricsCollector::WORKER_CRASHES);
        }

        return $replaced;
    }

    public function drain(): void
    {
        $this->draining = true;

        foreach ($this->workers as $worker) {
            $worker->drain();
        }
    }

    public function isDraining(): bool
    {
        return array_all($this->workers, static fn (Worker $worker): bool => $worker->isDraining() || $worker->isDead());
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
