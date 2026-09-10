<?php

declare(strict_types=1);

namespace App\Tests\DLQ;

use App\DLQ\DeadLetterQueue;
use App\Job\Job;
use App\Job\JobState;
use App\Tests\Support\FakeClock;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class DeadLetterQueueTest extends TestCase
{
    private FakeClock $clock;

    protected function setUp(): void
    {
        $this->clock = new FakeClock(1000.0);
    }

    public function testAddStoresRecord(): void
    {
        $dlq = new DeadLetterQueue($this->clock);
        $job = $this->failedJob(attempts: 3);
        $exception = new RuntimeException('boom');

        $dlq->add($job, $exception);

        $this->assertSame(1, $dlq->size());
        $this->assertTrue($dlq->contains($job));
    }

    public function testRecordPreservesFailureInformation(): void
    {
        $dlq = new DeadLetterQueue($this->clock);
        $job = $this->failedJob(attempts: 3);
        $exception = new RuntimeException('boom');

        $dlq->add($job, $exception);
        $record = $dlq->find($job->getId()->toString());

        $this->assertNotNull($record);
        $this->assertSame($exception, $record->getException());
        $this->assertSame(3, $record->getAttempts());
        $this->assertSame(1000.0, $record->getFailedAt());
        $this->assertSame($job->getId()->toString(), $record->getJob()->getId()->toString());
    }

    public function testListReturnsAllRecords(): void
    {
        $dlq = new DeadLetterQueue($this->clock);
        $dlq->add($this->failedJob(attempts: 1), new RuntimeException('a'));
        $dlq->add($this->failedJob(attempts: 2), new RuntimeException('b'));

        $this->assertCount(2, $dlq->all());
    }

    public function testDeleteRemovesRecord(): void
    {
        $dlq = new DeadLetterQueue($this->clock);
        $job = $this->failedJob(attempts: 1);
        $dlq->add($job, new RuntimeException('a'));

        $dlq->delete($job->getId()->toString());

        $this->assertSame(0, $dlq->size());
        $this->assertNull($dlq->find($job->getId()->toString()));
    }

    public function testRetryRequeuesJobToReady(): void
    {
        $dlq = new DeadLetterQueue($this->clock);
        $job = $this->failedJob(attempts: 3);
        $dlq->add($job, new RuntimeException('boom'));

        $retried = $dlq->retry($job->getId()->toString());

        $this->assertNotNull($retried);
        $this->assertSame(JobState::READY, $job->getState());
        $this->assertSame(0, $dlq->size());
    }

    public function testRetryUnknownIdReturnsNull(): void
    {
        $dlq = new DeadLetterQueue($this->clock);

        $this->assertNull($dlq->retry('missing'));
    }

    private function failedJob(int $attempts): Job
    {
        $job = Job::create(type: 'a');
        $job->markReady(1000.0);
        for ($i = 0; $i < $attempts; $i++) {
            $job->markProcessing();
            if ($i < $attempts - 1) {
                $job->markRetry(1000.0);
            }
        }
        $job->markFailed();

        return $job;
    }
}
