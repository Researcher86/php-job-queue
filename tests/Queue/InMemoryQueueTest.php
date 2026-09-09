<?php

declare(strict_types=1);

namespace App\Tests\Queue;

use App\Job\Job;
use App\Job\JobState;
use App\Queue\InMemoryQueue;
use App\Tests\Support\FakeClock;
use PHPUnit\Framework\TestCase;

final class InMemoryQueueTest extends TestCase
{
    private InMemoryQueue $queue;

    private FakeClock $clock;

    protected function setUp(): void
    {
        $this->clock = new FakeClock(1000.0);
        $this->queue = new InMemoryQueue($this->clock);
    }

    public function testPopReturnsJobsInFifoOrder(): void
    {
        $a = Job::create(type: 'a');
        $b = Job::create(type: 'b');
        $c = Job::create(type: 'c');

        $this->queue->push($a);
        $this->queue->push($b);
        $this->queue->push($c);

        $this->assertSame('a', $this->queue->pop()?->getType());
        $this->assertSame('b', $this->queue->pop()?->getType());
        $this->assertSame('c', $this->queue->pop()?->getType());
    }

    public function testEmptyQueueReturnsNull(): void
    {
        $this->assertNull($this->queue->pop());
    }

    public function testQueueSizeIsCorrect(): void
    {
        $this->assertSame(0, $this->queue->size());

        $this->queue->push(Job::create(type: 'a'));
        $this->queue->push(Job::create(type: 'b'));

        $this->assertSame(2, $this->queue->size());
    }

    public function testQueueSizeDecreasesAfterPop(): void
    {
        $this->queue->push(Job::create(type: 'a'));
        $this->queue->push(Job::create(type: 'b'));

        $this->queue->pop();

        $this->assertSame(1, $this->queue->size());
    }

    public function testMultipleJobsWorkCorrectly(): void
    {
        $jobs = [];
        for ($i = 0; $i < 100; $i++) {
            $job = Job::create(type: "job-$i");
            $this->queue->push($job);
            $jobs[] = $job;
        }

        $this->assertSame(100, $this->queue->size());

        foreach ($jobs as $job) {
            $popped = $this->queue->pop();
            $this->assertNotNull($popped);
            $this->assertSame($job->getId()->toString(), $popped->getId()->toString());
        }

        $this->assertSame(0, $this->queue->size());
    }

    public function testPushMarksCreatedJobAsReady(): void
    {
        $job = Job::create(type: 'a');

        $this->queue->push($job);

        $this->assertSame(JobState::READY, $job->getState());
    }

    public function testReadyJobKeepsReadyStateOnPush(): void
    {
        $job = Job::create(type: 'a');
        $this->queue->push($job);

        $this->queue->push($job);

        $this->assertSame(JobState::READY, $job->getState());
    }
}
