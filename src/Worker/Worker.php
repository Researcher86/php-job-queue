<?php

declare(strict_types=1);

namespace App\Worker;

use App\Job\Job;
use App\Job\JobResult;
use Closure;
use LogicException;
use Throwable;

final class Worker
{
    private const array TRANSITIONS = [
        'ready' => [
            'STARTING' => WorkerState::IDLE,
        ],
        'work' => [
            'IDLE' => WorkerState::BUSY,
        ],
        'finish' => [
            'BUSY' => WorkerState::IDLE,
        ],
        'drain' => [
            'IDLE' => WorkerState::STOPPING,
        ],
        'die' => [
            'STARTING' => WorkerState::DEAD,
            'IDLE' => WorkerState::DEAD,
            'BUSY' => WorkerState::DEAD,
            'STOPPING' => WorkerState::DEAD,
        ],
    ];

    private WorkerState $state;

    private ?Job $currentJob = null;

    public function __construct(
        private readonly int $id,
        private readonly Closure $handler,
        WorkerState $state = WorkerState::STARTING,
    ) {
        $this->state = $state;
    }

    public function getId(): int
    {
        return $this->id;
    }

    public function getState(): WorkerState
    {
        return $this->state;
    }

    public function getCurrentJob(): ?Job
    {
        return $this->currentJob;
    }

    public function markReady(): void
    {
        $this->apply('ready');
    }

    public function isAvailable(): bool
    {
        return $this->state === WorkerState::IDLE;
    }

    public function isBusy(): bool
    {
        return $this->state === WorkerState::BUSY;
    }

    public function process(Job $job): JobResult
    {
        $this->currentJob = $job;
        $this->apply('work');

        try {
            $result = ($this->handler)($job);

            return $result instanceof JobResult ? $result : JobResult::success();
        } catch (Throwable $e) {
            return JobResult::failure($e);
        } finally {
            $this->currentJob = null;
            $this->apply('finish');
        }
    }

    public function drain(): void
    {
        $this->apply('drain');
    }

    public function markDead(): void
    {
        $this->apply('die');
    }

    private function apply(string $event): void
    {
        $next = self::TRANSITIONS[$event][$this->state->name] ?? null;
        if ($next === null) {
            throw new LogicException(sprintf(
                'Illegal transition: cannot %s a worker in state %s',
                $event,
                $this->state->name,
            ));
        }
        $this->state = $next;
    }
}
