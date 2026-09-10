<?php

declare(strict_types=1);

namespace App\Tests\Timeout;

use App\Job\Job;
use App\Job\JobState;
use App\Tests\Support\FakeClock;
use App\Timeout\VisibilityMonitor;
use PHPUnit\Framework\TestCase;

final class VisibilityMonitorTest extends TestCase
{
    private FakeClock $clock;

    protected function setUp(): void
    {
        $this->clock = new FakeClock(1000.0);
    }

    public function testTrackedJobIsProcessing(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $job = Job::create(type: 'a');
        $job->markReady(1000.0);
        $job->markProcessing();

        $monitor->track($job, 1);

        $this->assertTrue($monitor->isProcessing($job));
    }

    /**
     * A null timeout switches off expiry, not tracking: the job is still
     * counted as in flight, it just never becomes overdue. Which keeps
     * size() an honest gauge of unacknowledged work whether or not the
     * runtime is configured to reclaim anything.
     */
    public function testNullTimeoutTracksWithoutEverExpiring(): void
    {
        $monitor = new VisibilityMonitor(null, $this->clock);
        $job = Job::create(type: 'a');
        $job->markReady(1000.0);
        $job->markProcessing();
        $monitor->track($job, 1);

        $this->assertTrue($monitor->isProcessing($job));
        $this->assertSame(1, $monitor->size());

        $this->clock->advance(3600.0);

        $this->assertSame([], $monitor->requeueExpired());
        $this->assertSame(1, $monitor->size());
    }

    public function testSizeCountsUnacknowledgedJobs(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $this->assertSame(0, $monitor->size());

        $firstDelivery = $monitor->track($this->processingJob('a'), 1);
        $monitor->track($this->processingJob('b'), 2);

        $this->assertSame(2, $monitor->size());

        $monitor->release($firstDelivery);

        $this->assertSame(1, $monitor->size());
    }

    public function testReleasedJobIsNoLongerProcessing(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $job = Job::create(type: 'a');
        $job->markReady(1000.0);
        $job->markProcessing();

        $delivery = $monitor->track($job, 1);
        $monitor->release($delivery);

        $this->assertFalse($monitor->isProcessing($job));
    }

    public function testJobIsNotRequeuedBeforeDeadline(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $job = Job::create(type: 'a');
        $job->markReady(1000.0);
        $job->markProcessing();
        $monitor->track($job, 1);

        $this->clock->advance(29.0);

        $this->assertSame([], $monitor->requeueExpired());
        $this->assertTrue($monitor->isProcessing($job));
    }

    public function testExpiredJobReturnsToReady(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $job = Job::create(type: 'a');
        $job->markReady(1000.0);
        $job->markProcessing();
        $monitor->track($job, 1);

        $this->clock->advance(30.0);
        $expired = $monitor->requeueExpired();

        $this->assertCount(1, $expired);
        $this->assertSame($job->getId()->toString(), $expired[0]->getId()->toString());
        $this->assertSame(JobState::READY, $job->getState());
        $this->assertFalse($monitor->isProcessing($job));
    }

    public function testOnlyExpiredJobsAreRequeued(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $soon = Job::create(type: 'soon');
        $later = Job::create(type: 'later');
        $soon->markReady(1000.0);
        $later->markReady(1000.0);
        $soon->markProcessing();
        $monitor->track($soon, 1);

        $this->clock->advance(10.0);
        $later->markProcessing();
        $monitor->track($later, 1);

        $this->clock->advance(20.0);
        $expired = $monitor->requeueExpired();

        $this->assertCount(1, $expired);
        $this->assertSame('soon', $expired[0]->getType());
        $this->assertTrue($monitor->isProcessing($later));
    }

    private function processingJob(string $type): Job
    {
        $job = Job::create(type: $type);
        $job->markReady($this->clock->now());
        $job->markProcessing();

        return $job;
    }

    /**
     * The fence. A delivery whose deadline expired stops being current the
     * moment the job is handed out again, so its late answer is refusable.
     */
    public function testAnExpiredDeliveryIsNoLongerCurrentOnceTheJobIsReissued(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $job = $this->processingJob('slow');

        $first = $monitor->track($job, 1);
        $this->assertTrue($monitor->isCurrent($first));

        $this->clock->advance(31.0);
        $this->assertSame([$job], $monitor->requeueExpired());
        $this->assertFalse($monitor->isCurrent($first), 'the lease was revoked');

        // Handed out again - a new delivery, a new generation.
        $job->markProcessing();
        $second = $monitor->track($job, 2);

        $this->assertTrue($monitor->isCurrent($second));
        $this->assertFalse($monitor->isCurrent($first), 'and the old one stays revoked');
    }

    /**
     * Releasing on a stale answer would drop the LIVE delivery's lease and
     * leave a job in flight with nothing that could reclaim it.
     */
    public function testAStaleDeliveryReleasesNothing(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $job = $this->processingJob('slow');

        $first = $monitor->track($job, 1);
        $this->clock->advance(31.0);
        $monitor->requeueExpired();

        $job->markProcessing();
        $second = $monitor->track($job, 2);

        $monitor->release($first);

        $this->assertTrue($monitor->isCurrent($second), 'the live lease survived');
        $this->assertSame(1, $monitor->size());

        $monitor->release($second);

        $this->assertSame(0, $monitor->size());
    }

    public function testADeliveryForAnUntrackedJobIsNotCurrent(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $delivery = $monitor->track($this->processingJob('a'), 1);

        $monitor->release($delivery);

        $this->assertFalse($monitor->isCurrent($delivery));
    }

    public function testNextDeadlineIsTheEarliestLease(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $this->assertNull($monitor->nextDeadline());

        $monitor->track($this->processingJob('a'), 1);
        $this->clock->advance(5.0);
        $monitor->track($this->processingJob('b'), 2);

        $this->assertSame(1030.0, $monitor->nextDeadline());
    }
}
