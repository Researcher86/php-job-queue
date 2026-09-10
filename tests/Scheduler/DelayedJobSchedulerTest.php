<?php

declare(strict_types=1);

namespace App\Tests\Scheduler;

use App\Job\Job;
use App\Job\JobState;
use App\Scheduler\DelayedJobScheduler;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class DelayedJobSchedulerTest extends TestCase
{
    public function testEmptySchedulerHasNoDeadline(): void
    {
        $scheduler = new DelayedJobScheduler();

        $this->assertSame(0, $scheduler->size());
        $this->assertNull($scheduler->nextDeadline());
        $this->assertSame([], $scheduler->releaseDue(1000.0));
    }

    public function testNextDeadlineIsTheEarliestOne(): void
    {
        $scheduler = new DelayedJobScheduler();
        $scheduler->schedule($this->delayedJob('late', availableAt: 1300.0));
        $scheduler->schedule($this->delayedJob('early', availableAt: 1100.0));
        $scheduler->schedule($this->delayedJob('middle', availableAt: 1200.0));

        $this->assertSame(1100.0, $scheduler->nextDeadline());
        $this->assertSame(3, $scheduler->size());
    }

    public function testJobIsNotReleasedBeforeItsDeadline(): void
    {
        $scheduler = new DelayedJobScheduler();
        $scheduler->schedule($this->delayedJob('a', availableAt: 1100.0));

        $this->assertSame([], $scheduler->releaseDue(1099.9));
        $this->assertSame(1, $scheduler->size());
    }

    public function testDueJobsComeOutInDeadlineOrder(): void
    {
        $scheduler = new DelayedJobScheduler();
        $scheduler->schedule($this->delayedJob('third', availableAt: 1300.0));
        $scheduler->schedule($this->delayedJob('first', availableAt: 1100.0));
        $scheduler->schedule($this->delayedJob('second', availableAt: 1200.0));

        $released = $scheduler->releaseDue(1300.0);

        $this->assertSame(['first', 'second', 'third'], array_map(
            static fn (Job $job): string => $job->getType(),
            $released,
        ));
        $this->assertSame(0, $scheduler->size());
    }

    public function testOnlyDueJobsAreReleased(): void
    {
        $scheduler = new DelayedJobScheduler();
        $scheduler->schedule($this->delayedJob('due', availableAt: 1100.0));
        $scheduler->schedule($this->delayedJob('not-due', availableAt: 1200.0));

        $released = $scheduler->releaseDue(1150.0);

        $this->assertCount(1, $released);
        $this->assertSame('due', $released[0]->getType());
        $this->assertSame(1200.0, $scheduler->nextDeadline());
    }

    /** Equal deadlines are common - a burst of fixed-delay retries. */
    public function testEqualDeadlinesKeepInsertionOrder(): void
    {
        $scheduler = new DelayedJobScheduler();
        for ($i = 0; $i < 20; $i++) {
            $scheduler->schedule($this->delayedJob("job-$i", availableAt: 1100.0));
        }

        $released = $scheduler->releaseDue(1100.0);

        $expected = array_map(static fn (int $i): string => "job-$i", range(0, 19));
        $this->assertSame($expected, array_map(
            static fn (Job $job): string => $job->getType(),
            $released,
        ));
    }

    public function testReleasedDelayedJobBecomesReady(): void
    {
        $scheduler = new DelayedJobScheduler();
        $job = $this->delayedJob('a', availableAt: 1100.0);
        $scheduler->schedule($job);

        $released = $scheduler->releaseDue(1100.0);

        $this->assertSame(JobState::READY, $released[0]->getState());
        $this->assertSame(1100.0, $released[0]->getAvailableAt());
    }

    /**
     * A retry under backoff is a READY job with a future availableAt. It
     * never left READY, so releasing it must not try to re-enter that state.
     */
    public function testReleasedRetryStaysReadyWithoutATransition(): void
    {
        $scheduler = new DelayedJobScheduler();
        $job = Job::create(type: 'retried');
        $job->markReady(1000.0);
        $job->markProcessing();
        $job->markRetry(1100.0);
        $scheduler->schedule($job);

        $released = $scheduler->releaseDue(1100.0);

        $this->assertCount(1, $released);
        $this->assertSame(JobState::READY, $released[0]->getState());
        $this->assertSame(1100.0, $released[0]->getAvailableAt());
    }

    public function testJobWithoutADeadlineIsRejected(): void
    {
        $scheduler = new DelayedJobScheduler();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('no availableAt');

        $scheduler->schedule(Job::create(type: 'never-scheduled'));
    }

    private function delayedJob(string $type, float $availableAt): Job
    {
        $job = Job::create(type: $type);
        $job->markDelayed($availableAt);

        return $job;
    }
}
