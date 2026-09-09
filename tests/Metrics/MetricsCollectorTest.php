<?php

declare(strict_types=1);

namespace App\Tests\Metrics;

use App\Metrics\MetricsCollector;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class MetricsCollectorTest extends TestCase
{
    public function testCounterStartsAtZero(): void
    {
        $metrics = new MetricsCollector();

        $this->assertSame(0, $metrics->getCounter('completed'));
    }

    public function testIncrementIncreasesCounter(): void
    {
        $metrics = new MetricsCollector();

        $metrics->increment('completed');
        $metrics->increment('completed');

        $this->assertSame(2, $metrics->getCounter('completed'));
    }

    public function testIncrementByAmount(): void
    {
        $metrics = new MetricsCollector();

        $metrics->increment('failed', 5);

        $this->assertSame(5, $metrics->getCounter('failed'));
    }

    public function testRecordLatencyComputesStats(): void
    {
        $metrics = new MetricsCollector();

        $metrics->recordLatency('execution', 1.0);
        $metrics->recordLatency('execution', 3.0);

        $stats = $metrics->getLatencyStats('execution');

        $this->assertNotNull($stats);
        $this->assertSame(2, $stats['count']);
        $this->assertSame(1.0, $stats['min']);
        $this->assertSame(3.0, $stats['max']);
        $this->assertSame(2.0, $stats['avg']);
    }

    public function testLatencyStatsReturnNullWithoutSamples(): void
    {
        $metrics = new MetricsCollector();

        $this->assertNull($metrics->getLatencyStats('execution'));
    }

    public function testNegativeLatencyIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $metrics = new MetricsCollector();
        $metrics->recordLatency('execution', -1.0);
    }

    public function testSnapshotReturnsCountersAndLatencies(): void
    {
        $metrics = new MetricsCollector();
        $metrics->increment('completed');
        $metrics->recordLatency('execution', 2.0);

        $snapshot = $metrics->snapshot();

        $this->assertSame(1, $snapshot['counters']['completed']);
        $this->assertSame(2.0, $snapshot['latencies']['execution']['min']);
    }
}