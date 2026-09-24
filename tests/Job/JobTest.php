<?php

declare(strict_types=1);

namespace PhpJobQueue\Tests\Job;

use LogicException;
use PhpJobQueue\Job\Job;
use PhpJobQueue\Job\JobPriority;
use PhpJobQueue\Job\JobState;
use PhpJobQueue\Tests\Support\FakeClock;
use PHPUnit\Framework\TestCase;
use ValueError;

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

    public function testJobBecomesDelayed(): void
    {
        $job = Job::create(type: 'test');

        $job->markDelayed(1060.0);

        $this->assertSame(JobState::DELAYED, $job->getState());
        $this->assertSame(1060.0, $job->getAvailableAt());
    }

    public function testDelayedJobBecomesReady(): void
    {
        $job = Job::create(type: 'test');
        $job->markDelayed(1060.0);

        $job->markReady(1060.0);

        $this->assertSame(JobState::READY, $job->getState());
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
        $this->expectExceptionMessage('Invalid transition: cannot dispatch a job in state CREATED');

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

    public function testMarkProcessingRecordsWhenTheAttemptStarted(): void
    {
        $job = Job::create(type: 'test');
        $job->markReady($this->clock->now());

        $job->markProcessing(1000.0);

        $this->assertSame(1000.0, $job->getStartedAt());
        $this->assertNull($job->getCompletedAt());
    }

    public function testMarkProcessingWithNoTimeLeavesStartedAtNull(): void
    {
        $job = Job::create(type: 'test');
        $job->markReady($this->clock->now());

        $job->markProcessing();

        $this->assertNull($job->getStartedAt());
    }

    public function testMarkCompletedRecordsWhenTheJobFinished(): void
    {
        $job = Job::create(type: 'test');
        $job->markReady($this->clock->now());
        $job->markProcessing(1000.0);

        $job->markCompleted(1000.5);

        $this->assertSame(1000.5, $job->getCompletedAt());
        $this->assertNull($job->getLastError());
    }

    public function testMarkFailedRecordsWhenItFinishedAndWhy(): void
    {
        $job = Job::create(type: 'test');
        $job->markReady($this->clock->now());
        $job->markProcessing(1000.0);

        $job->markFailed(1000.2, 'database connection refused');

        $this->assertSame(1000.2, $job->getCompletedAt());
        $this->assertSame('database connection refused', $job->getLastError());
    }

    public function testMarkFailedWithNoReasonLeavesTheLastErrorUnchanged(): void
    {
        $job = Job::create(type: 'test');
        $job->markReady($this->clock->now());
        $job->markProcessing(1000.0);
        $job->markFailed(1000.1, 'first attempt broke');

        // A human requeues the dead-lettered job for one more try, which
        // then fails with no message at all - the earlier reason should not
        // be silently erased by a blank one.
        $job->markRequeued(1001.0);
        $job->markProcessing(1001.0);
        $job->markFailed(1001.1);

        $this->assertSame('first attempt broke', $job->getLastError());
    }

    public function testMarkRetryRecordsWhyThisAttemptFailed(): void
    {
        $job = Job::create(type: 'test');
        $job->markReady($this->clock->now());
        $job->markProcessing(1000.0);

        $job->markRetry(1005.0, 'timed out talking to the payment gateway');

        $this->assertSame('timed out talking to the payment gateway', $job->getLastError());
        // Retry is not terminal - nothing has "completed" yet.
        $this->assertNull($job->getCompletedAt());
    }

    public function testANewAttemptOverwritesThePreviousStartedAt(): void
    {
        // started_at tracks the CURRENT/most recent attempt - it is
        // deliberately overwritten by the next markProcessing(), the same
        // way attempts and availableAt already behave.
        $job = Job::create(type: 'test');
        $job->markReady($this->clock->now());
        $job->markProcessing(1000.0);
        $job->markRetry(1005.0, 'first try failed');

        // markRetry() already put the job back in READY - the next attempt
        // is dispatched straight from there.
        $job->markProcessing(1005.0);

        $this->assertSame(1005.0, $job->getStartedAt());
    }

    public function testJobStartsWithNoAttemptMetadata(): void
    {
        $job = Job::create(type: 'test');

        $this->assertNull($job->getStartedAt());
        $this->assertNull($job->getCompletedAt());
        $this->assertNull($job->getLastError());
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

    public function testJobRoundTripsThroughArray(): void
    {
        $job = Job::create(type: 'send_email', payload: ['email' => 'user@example.com'], maxAttempts: 5);
        $job->markReady(1000.0);
        $job->markProcessing(1000.0);
        $job->markRetry(1060.0, 'smtp connection refused');

        $restored = Job::fromArray($job->toArray());

        $this->assertSame($job->getId()->toString(), $restored->getId()->toString());
        $this->assertSame($job->getType(), $restored->getType());
        $this->assertSame($job->getPayload(), $restored->getPayload());
        $this->assertSame($job->getState(), $restored->getState());
        $this->assertSame($job->getAttempts(), $restored->getAttempts());
        $this->assertSame($job->getMaxAttempts(), $restored->getMaxAttempts());
        $this->assertSame($job->getCreatedAt(), $restored->getCreatedAt());
        $this->assertSame($job->getAvailableAt(), $restored->getAvailableAt());
        $this->assertSame($job->getStartedAt(), $restored->getStartedAt());
        $this->assertSame($job->getCompletedAt(), $restored->getCompletedAt());
        $this->assertSame($job->getLastError(), $restored->getLastError());
    }

    public function testAJobWithNoAttemptMetadataRoundTripsThatWay(): void
    {
        $job = Job::create(type: 'test');

        $restored = Job::fromArray($job->toArray());

        $this->assertNull($restored->getStartedAt());
        $this->assertNull($restored->getCompletedAt());
        $this->assertNull($restored->getLastError());
    }

    public function testAnUnknownStateNameIsRejected(): void
    {
        $this->expectException(ValueError::class);
        $this->expectExceptionMessage('"TRANSITIONS" is not a valid job state');

        // Not a hypothetical: fromName() used to be constant("self::$name"),
        // which would happily resolve any constant on the class.
        JobState::fromName('TRANSITIONS');
    }

    public function testAnUnknownPriorityNameIsRejected(): void
    {
        $this->expectException(ValueError::class);

        JobPriority::fromName('URGENT');
    }
}
