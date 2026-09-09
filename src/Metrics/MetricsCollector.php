<?php

declare(strict_types=1);

namespace App\Metrics;

use InvalidArgumentException;

final class MetricsCollector
{
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