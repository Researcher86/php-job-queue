<?php

declare(strict_types=1);

namespace App\Tests\Job;

use App\Job\Job;
use App\Job\JobState;
use App\Tests\Support\FakeClock;
use LogicException;
use PHPUnit\Framework\TestCase;

final class JobTest extends TestCase
{
    private FakeClock $clock;

    protected function setUp(): void
    {
        $this->clock = new FakeClock(1000.0);
    }

    public function testJobReceivesUniqueId(): void
    {
        $job1 = Job::create(type: 'test');
        $job2 = Job::create(type: 'test');

        $this->assertNotSame(
            $job1->getId()->toString(),
            $job2->getId()->toString(),
        );
    }

    public function testNewJobStartsWithCreatedState(): void
    {
        $job = Job::create(type: 'test');

        $this->assertSame(JobState::CREATED, $job->getState());
    }

    public function testJobBecomesReady(): void
    {
        $job = Job::create(type: 'test');

        $job->markReady($this->clock->now());

        $this->assertSame(JobState::READY, $job->getState());
        $this->assertSame(1000.0, $job->getAvailableAt());
    }

    public function testJobBecomesProcessing(): void
    {
        $job = Job::create(type: 'test');

        $job->markReady($this->clock->now());
        $job->markProcessing();

        $this->assertSame(JobState::PROCESSING, $job->getState());
        $this->assertSame(1, $job->getAttempts());
        $this->assertNull($job->getAvailableAt());
    }

    public function testJobBecomesCompleted(): void
    {
        $job = Job::create(type: 'test');

        $job->markReady($this->clock->now());
        $job->markProcessing();
        $job->markCompleted();

        $this->assertSame(JobState::COMPLETED, $job->getState());
    }

    public function testJobBecomesFailed(): void
    {
        $job = Job::create(type: 'test');

        $job->markReady($this->clock->now());
        $job->markProcessing();
        $job->markFailed();

        $this->assertSame(JobState::FAILED, $job->getState());
    }

    public function testInvalidTransitionIsRejected(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Invalid transition: cannot move job from CREATED to PROCESSING');

        $job = Job::create(type: 'test');
        $job->markProcessing();
    }

    public function testCannotMarkCompletedFromCreated(): void
    {
        $this->expectException(LogicException::class);

        $job = Job::create(type: 'test');
        $job->markCompleted();
    }

    public function testCannotMarkFailedFromCreated(): void
    {
        $this->expectException(LogicException::class);

        $job = Job::create(type: 'test');
        $job->markFailed();
    }

    public function testCannotMarkReadyFromProcessing(): void
    {
        $this->expectException(LogicException::class);

        $job = Job::create(type: 'test');
        $job->markReady($this->clock->now());
        $job->markProcessing();
        $job->markReady($this->clock->now());
    }

    public function testJobCanBeRetriedFromProcessing(): void
    {
        $job = Job::create(type: 'test');
        $job->markReady($this->clock->now());
        $job->markProcessing();
        $job->markRetry(1005.0);

        $this->assertSame(JobState::READY, $job->getState());
        $this->assertSame(1005.0, $job->getAvailableAt());
        $this->assertSame(1, $job->getAttempts());
    }

    public function testCannotMarkRetryFromReady(): void
    {
        $this->expectException(LogicException::class);

        $job = Job::create(type: 'test');
        $job->markReady($this->clock->now());
        $job->markRetry(1005.0);
    }

    public function testJobPreservesType(): void
    {
        $job = Job::create(type: 'send_email');

        $this->assertSame('send_email', $job->getType());
    }

    public function testJobPreservesPayload(): void
    {
        $payload = ['email' => 'user@example.com', 'subject' => 'Hello'];
        $job = Job::create(type: 'send_email', payload: $payload);

        $this->assertSame($payload, $job->getPayload());
    }

    public function testJobStartsWithZeroAttempts(): void
    {
        $job = Job::create(type: 'test');

        $this->assertSame(0, $job->getAttempts());
    }

    public function testAttemptsIncreaseOnMarkProcessing(): void
    {
        $job = Job::create(type: 'test', maxAttempts: 5);

        $job->markReady($this->clock->now());
        $job->markProcessing();

        $this->assertSame(1, $job->getAttempts());
        $this->assertSame(5, $job->getMaxAttempts());
    }

    public function testCreatedAtIsSetFromClock(): void
    {
        $this->clock->advance(500.0);
        $job = Job::create(type: 'test', clock: $this->clock);

        $this->assertSame(1500.0, $job->getCreatedAt());
    }

    public function testAvailableAtIsNullOnCreation(): void
    {
        $job = Job::create(type: 'test');

        $this->assertNull($job->getAvailableAt());
    }

    public function testCompletedStateIsTerminal(): void
    {
        $this->assertTrue(JobState::COMPLETED->isTerminal());
    }

    public function testFailedStateIsTerminal(): void
    {
        $this->assertTrue(JobState::FAILED->isTerminal());
    }

    public function testCreatedStateIsNotTerminal(): void
    {
        $this->assertFalse(JobState::CREATED->isTerminal());
    }

    public function testReadyStateIsNotTerminal(): void
    {
        $this->assertFalse(JobState::READY->isTerminal());
    }

    public function testProcessingStateIsNotTerminal(): void
    {
        $this->assertFalse(JobState::PROCESSING->isTerminal());
    }

    public function testDefaultMaxAttemptsIsThree(): void
    {
        $job = Job::create(type: 'test');

        $this->assertSame(3, $job->getMaxAttempts());
    }
}
