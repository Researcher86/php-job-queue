<?php

declare(strict_types=1);

namespace App\Tests\Queue;

use App\Job\Job;
use App\Job\JobPriority;
use App\Job\JobState;
use App\Queue\PriorityQueue;
use App\Queue\WeightedRoundRobin;
use App\Tests\Support\FakeClock;
use PHPUnit\Framework\TestCase;

final class PriorityQueueTest extends TestCase
{
    private PriorityQueue $queue;

    private FakeClock $clock;

    protected function setUp(): void
    {
        $this->clock = new FakeClock(1000.0);
        $this->queue = new PriorityQueue($this->clock);
    }

    public function testHighPriorityJobIsPoppedFirst(): void
    {
        $low = Job::create(type: 'low', priority: JobPriority::LOW);
        $high = Job::create(type: 'high', priority: JobPriority::HIGH);

        $this->queue->push($low);
        $this->queue->push($high);

        $this->assertSame('high', $this->queue->pop()->getType());
        $this->assertSame('low', $this->queue->pop()?->getType());
    }

    public function testNormalPriorityComesBeforeLow(): void
    {
        $low = Job::create(type: 'low', priority: JobPriority::LOW);
        $normal = Job::create(type: 'normal', priority: JobPriority::NORMAL);

        $this->queue->push($low);
        $this->queue->push($normal);

        $this->assertSame('normal', $this->queue->pop()->getType());
        $this->assertSame('low', $this->queue->pop()?->getType());
    }

    public function testThreeWayPriorityOrdering(): void
    {
        $low = Job::create(type: 'low', priority: JobPriority::LOW);
        $normal = Job::create(type: 'normal', priority: JobPriority::NORMAL);
        $high = Job::create(type: 'high', priority: JobPriority::HIGH);

        $this->queue->push($low);
        $this->queue->push($normal);
        $this->queue->push($high);

        $this->assertSame('high', $this->queue->pop()->getType());
        $this->assertSame('normal', $this->queue->pop()?->getType());
        $this->assertSame('low', $this->queue->pop()?->getType());
    }

    public function testFifoOrderWithinSamePriority(): void
    {
        $a = Job::create(type: 'a', priority: JobPriority::HIGH);
        $b = Job::create(type: 'b', priority: JobPriority::HIGH);

        $this->queue->push($a);
        $this->queue->push($b);

        $this->assertSame('a', $this->queue->pop()->getType());
        $this->assertSame('b', $this->queue->pop()?->getType());
    }

    public function testEmptyQueueReturnsNull(): void
    {
        $this->assertNull($this->queue->pop());
    }

    public function testSizeCountsAllPriorities(): void
    {
        $this->queue->push(Job::create(type: 'a', priority: JobPriority::HIGH));
        $this->queue->push(Job::create(type: 'b', priority: JobPriority::LOW));

        $this->assertSame(2, $this->queue->size());
    }

    public function testStrictPriorityServesLowOnlyAfterEverythingElse(): void
    {
        $this->queue->push(Job::create(type: 'low', priority: JobPriority::LOW));

        for ($i = 0; $i < 5; $i++) {
            $this->queue->push(Job::create(type: "high-$i", priority: JobPriority::HIGH));
        }

        $popped = $this->drain();

        $this->assertSame(['high-0', 'high-1', 'high-2', 'high-3', 'high-4', 'low'], $popped);
    }

    /**
     * Starvation, actually demonstrated: not "LOW goes last in a queue that
     * empties", but "LOW never goes at all". A steady stream of HIGH work -
     * one new HIGH job arriving for every job served, which is what a busy
     * system looks like - and the LOW job pushed first is still sitting
     * there fifty pops later.
     *
     * This is the failure WeightedRoundRobin exists to fix; the next test
     * is the same scenario with it fitted.
     */
    public function testStrictPriorityStarvesLowForever(): void
    {
        $this->queue->push(Job::create(type: 'low', priority: JobPriority::LOW));
        $this->queue->push(Job::create(type: 'high', priority: JobPriority::HIGH));

        $served = [];
        for ($i = 0; $i < 50; $i++) {
            $job = $this->queue->pop();
            $this->assertNotNull($job);
            $served[] = $job->getType();

            // The stream of urgent work never lets up.
            $this->queue->push(Job::create(type: 'high', priority: JobPriority::HIGH));
        }

        $this->assertSame(['high'], array_values(array_unique($served)));
        // One LOW plus the one HIGH that arrived after the last pop.
        $this->assertSame(2, $this->queue->size());
    }

    public function testWeightedRoundRobinRescuesLowFromStarvation(): void
    {
        $queue = new PriorityQueue($this->clock, new WeightedRoundRobin());
        $queue->push(Job::create(type: 'low', priority: JobPriority::LOW));
        $queue->push(Job::create(type: 'high', priority: JobPriority::HIGH));

        $served = [];
        for ($i = 0; $i < 50; $i++) {
            $job = $queue->pop();
            $this->assertNotNull($job);
            $served[] = $job->getType();
            $queue->push(Job::create(type: 'high', priority: JobPriority::HIGH));
        }

        $this->assertContains('low', $served, 'the LOW job got a turn');
        // Within the first round: 5 HIGH, then no NORMAL waiting, then LOW.
        $this->assertSame(5, array_search('low', $served, true));
    }

    /** PLAN.md Phase 13's example order: 5 HIGH per 3 NORMAL per 1 LOW. */
    public function testWeightedRoundRobinFollowsItsWeights(): void
    {
        $queue = new PriorityQueue($this->clock, new WeightedRoundRobin());

        for ($i = 0; $i < 12; $i++) {
            $queue->push(Job::create(type: 'high', priority: JobPriority::HIGH));
            $queue->push(Job::create(type: 'normal', priority: JobPriority::NORMAL));
            $queue->push(Job::create(type: 'low', priority: JobPriority::LOW));
        }

        $served = [];
        for ($i = 0; $i < 18; $i++) {
            $served[] = $queue->pop()?->getType();
        }

        $this->assertSame([
            'high', 'high', 'high', 'high', 'high', 'normal', 'normal', 'normal', 'low',
            'high', 'high', 'high', 'high', 'high', 'normal', 'normal', 'normal', 'low',
        ], $served);
    }

    /**
     * Fairness is only paid for when there is something to be fair to: with
     * only HIGH work waiting, every turn goes to HIGH.
     */
    public function testWeightedRoundRobinDoesNotIdleOnEmptyLanes(): void
    {
        $queue = new PriorityQueue($this->clock, new WeightedRoundRobin());
        for ($i = 0; $i < 12; $i++) {
            $queue->push(Job::create(type: "high-$i", priority: JobPriority::HIGH));
        }

        $served = [];
        while (($job = $queue->pop()) !== null) {
            $served[] = $job->getType();
        }

        $this->assertCount(12, $served);
        $this->assertSame('high-0', $served[0]);
        $this->assertSame('high-11', $served[11]);
    }

    public function testDelayedJobPromotesByPriority(): void
    {
        $this->queue->push(Job::create(type: 'high', priority: JobPriority::HIGH), delay: 60);

        $this->assertNull($this->queue->pop());
        $this->clock->advance(60.0);

        $this->assertSame('high', $this->queue->pop()?->getType());
    }

    public function testPushWithDelayMarksJobDelayed(): void
    {
        $job = Job::create(type: 'a', priority: JobPriority::NORMAL);

        $this->queue->push($job, delay: 60);

        $this->assertSame(JobState::DELAYED, $job->getState());
    }

    public function testDelayedJobsAreCountedSeparatelyFromReadyOnes(): void
    {
        $this->queue->push(Job::create(type: 'now', priority: JobPriority::HIGH));
        $this->queue->push(Job::create(type: 'later', priority: JobPriority::LOW), delay: 60);

        $this->assertSame(2, $this->queue->size());
        $this->assertSame(1, $this->queue->delayedSize());

        $this->clock->advance(60.0);
        $this->queue->pop();

        $this->assertSame(0, $this->queue->delayedSize());
    }

    public function testDelayedJobBecomesReadyWhenPromoted(): void
    {
        $job = Job::create(type: 'a', priority: JobPriority::LOW);
        $this->queue->push($job, delay: 60);

        $this->clock->advance(60.0);
        $popped = $this->queue->pop();

        $this->assertNotNull($popped);
        $this->assertSame(JobState::READY, $job->getState());
    }

    /** @return list<string> */
    private function drain(): array
    {
        $types = [];

        while (($job = $this->queue->pop()) !== null) {
            $types[] = $job->getType();
        }

        return $types;
    }
}
