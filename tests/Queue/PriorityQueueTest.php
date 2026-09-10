<?php

declare(strict_types=1);

namespace App\Tests\Queue;

use App\Job\Job;
use App\Job\JobPriority;
use App\Job\JobState;
use App\Queue\PriorityQueue;
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

    public function testHighPriorityJobsStarveLowPriority(): void
    {
        $this->queue->push(Job::create(type: 'low', priority: JobPriority::LOW));

        for ($i = 0; $i < 5; $i++) {
            $this->queue->push(Job::create(type: "high-$i", priority: JobPriority::HIGH));
        }

        $popped = [];
        while (($job = $this->queue->pop()) !== null) {
            $popped[] = $job->getType();
        }

        // Strict priority means LOW is only served after all HIGH jobs
        $this->assertSame('high-0', $popped[0]);
        $this->assertSame('high-4', $popped[4]);
        $this->assertSame('low', $popped[5]);
        $this->assertCount(6, $popped);
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

    public function testDelayedJobBecomesReadyWhenPromoted(): void
    {
        $job = Job::create(type: 'a', priority: JobPriority::LOW);
        $this->queue->push($job, delay: 60);

        $this->clock->advance(60.0);
        $popped = $this->queue->pop();

        $this->assertNotNull($popped);
        $this->assertSame(JobState::READY, $job->getState());
    }
}
