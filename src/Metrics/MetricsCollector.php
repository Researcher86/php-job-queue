<?php

declare(strict_types=1);

namespace App\Metrics;

use InvalidArgumentException;

/**
 * Counters and latency samples for the whole runtime - PLAN.md Phase 14.
 *
 * Deliberately dumb: names are strings, counters only go up, and latencies
 * are kept as raw samples rather than pre-aggregated buckets. Nothing here
 * needs to scale to a million series; it needs to make one run explainable.
 *
 * The names are constants rather than literals at the call sites so that
 * the vocabulary of the system is readable in one place, and so that a
 * typo is a fatal error instead of a counter that silently stays at zero.
 *
 * ## Why three latencies and not one
 *
 * A job that took 300ms is not evidence of a slow handler. Splitting the
 * time tells you which problem you have:
 *
 *   LATENCY_QUEUE_WAIT   how long the job sat available with no free
 *                        worker  -> add workers
 *   LATENCY_EXECUTION    how long the handler itself ran
 *                        -> fix the handler
 *   LATENCY_END_TO_END   creation to completion, which also covers time a
 *                        DELAYED job spent deliberately waiting, and every
 *                        failed attempt before the one that worked
 *
 * They are not three views of one number, and end-to-end is not the sum of
 * the other two - a retried job passes through queue wait and execution
 * once per attempt, inside a single end-to-end span.
 */
final class MetricsCollector
{
    public const string JOBS_CREATED = 'created';
    public const string JOBS_COMPLETED = 'completed';
    public const string JOBS_FAILED = 'failed';
    public const string JOBS_RETRIED = 'retried';
    public const string JOBS_DEAD_LETTERED = 'dlq';
    public const string WORKER_CRASHES = 'worker_crashes';

    public const string LATENCY_QUEUE_WAIT = 'queue_wait';
    public const string LATENCY_EXECUTION = 'execution';
    public const string LATENCY_END_TO_END = 'end_to_end';

    /** @var array<string, int> */
    private array $counters = [];

    /** @var array<string, list<float>> */
    private array $latencies = [];

    public function increment(string $name, int $by = 1): void
    {
        $this->counters[$name] = ($this->counters[$name] ?? 0) + $by;
    }

    public function recordLatency(string $name, float $seconds): void
    {
        if ($seconds < 0) {
            throw new InvalidArgumentException('Latency cannot be negative');
        }
        $this->latencies[$name][] = $seconds;
    }

    public function getCounter(string $name): int
    {
        return $this->counters[$name] ?? 0;
    }

    /** @return array<string, int> */
    public function getCounters(): array
    {
        return $this->counters;
    }

    /** @return array{count: int, min: float, max: float, avg: float}|null */
    public function getLatencyStats(string $name): ?array
    {
        $samples = $this->latencies[$name] ?? [];
        if ($samples === []) {
            return null;
        }

        return [
            'count' => count($samples),
            'min' => min($samples),
            'max' => max($samples),
            'avg' => array_sum($samples) / count($samples),
        ];
    }

    /** @return array<string, mixed> */
    public function snapshot(): array
    {
        $latencyStats = [];
        foreach (array_keys($this->latencies) as $name) {
            $latencyStats[$name] = $this->getLatencyStats($name);
        }

        return [
            'counters' => $this->counters,
            'latencies' => $latencyStats,
        ];
    }
}
