<?php

declare(strict_types=1);

namespace App\Tests\Queue;

use App\Job\Job;
use App\Job\JobState;
use App\Queue\InMemoryQueue;
use App\Persistence\InMemoryStorage;
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

        $a = $this->queue->pop();
        $this->assertNotNull($a);
        $this->assertSame('a', $a->getType());
        $b = $this->queue->pop();
        $this->assertNotNull($b);
        $this->assertSame('b', $b->getType());
        $c = $this->queue->pop();
        $this->assertNotNull($c);
        $this->assertSame('c', $c->getType());
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

    public function testDelayedJobIsNotImmediatelyAvailable(): void
    {
        $job = Job::create(type: 'a');

        $this->queue->push($job, delay: 60);

        $this->assertSame(JobState::DELAYED, $job->getState());
        $this->assertNull($this->queue->pop());
        $this->assertSame(1, $this->queue->size());
    }

    public function testDelayedJobBecomesAvailableAtCorrectTime(): void
    {
        $job = Job::create(type: 'a');
        $this->queue->push($job, delay: 60);

        $this->assertNull($this->queue->pop());

        $this->clock->advance(60.0);
        $popped = $this->queue->pop();

        $this->assertInstanceOf(Job::class, $popped);
        $this->assertSame($job->getId()->toString(), $popped->getId()->toString());
        $this->assertSame(JobState::READY, $job->getState());
    }

    public function testDelayedJobNotAvailableBeforeDeadline(): void
    {
        $job = Job::create(type: 'a');
        $this->queue->push($job, delay: 60);

        $this->clock->advance(59.0);

        $this->assertNull($this->queue->pop());
    }

    public function testMultipleDelayedJobsPreserveSchedule(): void
    {
        $short = Job::create(type: 'short');
        $long = Job::create(type: 'long');

        $this->queue->push($short, delay: 10);
        $this->queue->push($long, delay: 20);

        $this->clock->advance(10.0);
        $short = $this->queue->pop();
        $this->assertNotNull($short);
        $this->assertSame('short', $short->getType());

        $this->assertNull($this->queue->pop());

        $this->clock->advance(10.0);
        $long = $this->queue->pop();
        $this->assertNotNull($long);
        $this->assertSame('long', $long->getType());
    }

    public function testDelayedAndReadyJobsMixOnPop(): void
    {
        $this->queue->push(Job::create(type: 'ready_first'), delay: 0);
        $this->queue->push(Job::create(type: 'delayed'), delay: 60);

        $readyFirst = $this->queue->pop();
        $this->assertNotNull($readyFirst);
        $this->assertSame('ready_first', $readyFirst->getType());

        $this->clock->advance(60.0);
        $delayed = $this->queue->pop();
        $this->assertNotNull($delayed);
        $this->assertSame('delayed', $delayed->getType());
    }

    public function testReadyJobsSurviveRestart(): void
    {
        $storage = new InMemoryStorage();
        $queue = new InMemoryQueue($this->clock, $storage);
        $queue->push(Job::create(type: 'a'));
        $queue->push(Job::create(type: 'b'));

        $restored = InMemoryQueue::restoreFromStorage($storage, new FakeClock(1000.0));

        $this->assertSame(2, $restored->size());
        $first = $restored->pop();
        $this->assertNotNull($first);
        $this->assertSame('a', $first->getType());
        $second = $restored->pop();
        $this->assertNotNull($second);
        $this->assertSame('b', $second->getType());
    }

    public function testDelayedJobsSurviveRestart(): void
    {
        $storage = new InMemoryStorage();
        $queue = new InMemoryQueue($this->clock, $storage);
        $queue->push(Job::create(type: 'later'), delay: 60);

        $restored = InMemoryQueue::restoreFromStorage($storage, new FakeClock(1000.0));
        $this->assertNull($restored->pop());

        $clock = new FakeClock(1000.0);
        $clock->advance(60.0);
        $this->assertSame('later', $restored->pop($clock->now())?->getType());
    }

    public function testProcessingJobsReturnToReadyAfterRestart(): void
    {
        $storage = new InMemoryStorage();
        $queue = new InMemoryQueue($this->clock, $storage);
        $job = Job::create(type: 'a');
        $queue->push($job);
        $job->markProcessing();
        $storage->store($job->getId()->toString(), $job->toArray());

        $restored = InMemoryQueue::restoreFromStorage($storage, new FakeClock(1000.0));

        $this->assertSame(1, $restored->size());
        $popped = $restored->pop();
        $this->assertNotNull($popped);
        $this->assertSame(JobState::READY, $popped->getState());
    }

    public function testCompletedJobsAreNotRestored(): void
    {
        $storage = new InMemoryStorage();
        $queue = new InMemoryQueue($this->clock, $storage);
        $job = Job::create(type: 'done');
        $queue->push($job);
        $job->markProcessing();
        $job->markCompleted();
        $storage->store($job->getId()->toString(), $job->toArray());

        $restored = InMemoryQueue::restoreFromStorage($storage, new FakeClock(1000.0));

        $this->assertSame(0, $restored->size());
    }
}
