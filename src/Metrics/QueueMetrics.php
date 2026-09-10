<?php

declare(strict_types=1);

namespace App\Metrics;

/**
 * A gauge reading: what the system looks like at one instant - PLAN.md
 * Phase 14.
 *
 * Separate from MetricsCollector because the two answer different
 * questions and age differently. A counter is cumulative and only ever
 * grows ("2,318 jobs completed since start"); a gauge is a snapshot that is
 * already stale when you read it ("7 jobs are being processed right now").
 * Mixing them in one bag makes it easy to read a gauge as a total, which is
 * how "queue size: 40,000" ends up in a dashboard as a number that never
 * goes down.
 *
 * Read from the live objects by JobDispatcher::observe(). Immutable, so a
 * reading can be kept and compared against a later one.
 */
final readonly class QueueMetrics
{
    public function __construct(
        /** Jobs waiting and available right now. */
        public int $ready,
        /** Jobs waiting for a deadline - a delay, or a retry under backoff. */
        public int $delayed,
        /** Jobs in a worker's hands, unacknowledged. */
        public int $processing,
        public int $workers,
        public int $busyWorkers,
        /** Jobs that exhausted their attempts and are waiting for a human. */
        public int $deadLettered,
    ) {}

    /**
     * Workers with nothing to do. Together with $ready this is the reading
     * that matters most: idle workers AND a non-empty ready queue at the
     * same time means the dispatcher is not keeping up, which is a
     * different problem from either being high on its own.
     */
    public function idleWorkers(): int
    {
        return $this->workers - $this->busyWorkers;
    }

    /** @return array<string, int> */
    public function toArray(): array
    {
        return [
            'ready' => $this->ready,
            'delayed' => $this->delayed,
            'processing' => $this->processing,
            'workers' => $this->workers,
            'busy_workers' => $this->busyWorkers,
            'idle_workers' => $this->idleWorkers(),
            'dead_lettered' => $this->deadLettered,
        ];
    }
}
