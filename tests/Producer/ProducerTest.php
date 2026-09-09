<?php

declare(strict_types=1);

namespace App\Tests\Producer;

use App\Job\JobId;
use App\Job\JobPriority;
use App\Job\JobState;
use App\Producer\JobFactory;
use App\Producer\Producer;
use App\Queue\InMemoryQueue;
use App\Tests\Support\FakeClock;
use PHPUnit\Framework\TestCase;

final class ProducerTest extends TestCase
{
    private Producer $producer;

    private InMemoryQueue $queue;

    protected function setUp(): void
    {
        $this->queue = new InMemoryQueue(new FakeClock());
        $this->producer = new Producer(
            $this->queue,
            new JobFactory(new FakeClock()),
        );
    }

    public function testProducerCreatesJob(): void
    {
        $job = $this->producer->dispatch('send_email');

        $this->assertSame(1, $this->queue->size());
        $this->assertInstanceOf(JobId::class, $job->getId());
    }

    public function testJobTypeIsPreserved(): void
    {
        $job = $this->producer->dispatch('send_email');

        $this->assertSame('send_email', $job->getType());
    }

    public function testPayloadIsPreserved(): void
    {
        $payload = ['email' => 'user@example.com', 'subject' => 'Hello'];
        $job = $this->producer->dispatch('send_email', payload: $payload);

        $this->assertSame($payload, $job->getPayload());
    }

    public function testJobEntersReadyState(): void
    {
        $job = $this->producer->dispatch('send_email');

        $this->assertSame(JobState::READY, $job->getState());
    }

    public function testJobEntersTheQueue(): void
    {
        $job = $this->producer->dispatch('send_email');

        $popped = $this->queue->pop();

        $this->assertNotNull($popped);
        $this->assertSame($job->getId()->toString(), $popped->getId()->toString());
    }

    public function testMaxAttemptsIsPropagated(): void
    {
        $job = $this->producer->dispatch('send_email', maxAttempts: 7);

        $this->assertSame(7, $job->getMaxAttempts());
    }

    public function testPriorityIsPropagated(): void
    {
        $job = $this->producer->dispatch('send_email', priority: JobPriority::HIGH);

        $this->assertSame(JobPriority::HIGH, $job->getPriority());
    }

    public function testDelaySchedulesTheJob(): void
    {
        $clock = new FakeClock(1000.0);
        $queue = new InMemoryQueue($clock);
        $producer = new Producer($queue, new JobFactory($clock));

        $job = $producer->dispatch('send_email', delay: 60);

        $this->assertSame(JobState::DELAYED, $job->getState());
        $this->assertNull($queue->pop());
        $this->assertSame(1, $queue->size());
    }
}
