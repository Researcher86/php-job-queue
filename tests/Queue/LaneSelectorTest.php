<?php

declare(strict_types=1);

namespace App\Tests\Queue;

use App\Job\JobPriority;
use App\Queue\StrictPriority;
use App\Queue\WeightedRoundRobin;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class LaneSelectorTest extends TestCase
{
    public function testStrictPriorityTakesTheHighestNonEmptyLane(): void
    {
        $selector = new StrictPriority();

        $this->assertSame(JobPriority::HIGH, $selector->next($this->depth(high: 1, normal: 1, low: 1)));
        $this->assertSame(JobPriority::NORMAL, $selector->next($this->depth(normal: 1, low: 1)));
        $this->assertSame(JobPriority::LOW, $selector->next($this->depth(low: 1)));
    }

    public function testStrictPriorityReportsAnEmptyQueue(): void
    {
        $this->assertNull((new StrictPriority())->next($this->depth()));
    }

    public function testWeightedRoundRobinReportsAnEmptyQueue(): void
    {
        $this->assertNull((new WeightedRoundRobin())->next($this->depth()));
    }

    /**
     * The credits are per round, not per call: once HIGH has had its five
     * turns it is skipped even though it still has jobs, which is what lets
     * the other lanes move at all.
     */
    public function testWeightedRoundRobinSpendsCreditsThenRefills(): void
    {
        $selector = new WeightedRoundRobin(high: 2, normal: 1, low: 1);
        $depth = $this->depth(high: 100, normal: 100, low: 100);

        $order = [];
        for ($i = 0; $i < 8; $i++) {
            $order[] = $selector->next($depth)?->name;
        }

        $this->assertSame([
            'HIGH', 'HIGH', 'NORMAL', 'LOW',
            'HIGH', 'HIGH', 'NORMAL', 'LOW',
        ], $order);
    }

    /** A lane that is empty when its turn comes forfeits it rather than blocking. */
    public function testWeightedRoundRobinSkipsEmptyLanes(): void
    {
        $selector = new WeightedRoundRobin(high: 1, normal: 1, low: 1);

        $this->assertSame(JobPriority::HIGH, $selector->next($this->depth(high: 1, low: 1)));
        $this->assertSame(JobPriority::LOW, $selector->next($this->depth(low: 1)));
    }

    /**
     * A lane whose jobs only arrive after its credits are gone still gets
     * served: the round ends when no funded lane has work, and the refill
     * happens inside the same call.
     */
    public function testWeightedRoundRobinRefillsWithinOneCall(): void
    {
        $selector = new WeightedRoundRobin(high: 1, normal: 1, low: 1);
        $selector->next($this->depth(high: 5));

        $this->assertSame(JobPriority::HIGH, $selector->next($this->depth(high: 5)));
    }

    public function testAWeightOfZeroWouldStarveALaneAndIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new WeightedRoundRobin(low: 0);
    }

    /** @return array<string, int> */
    private function depth(int $high = 0, int $normal = 0, int $low = 0): array
    {
        return [
            JobPriority::HIGH->name => $high,
            JobPriority::NORMAL->name => $normal,
            JobPriority::LOW->name => $low,
        ];
    }
}
