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

        $monitor->track($job);

        $this->assertTrue($monitor->isProcessing($job));
    }

    public function testReleasedJobIsNoLongerProcessing(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $job = Job::create(type: 'a');
        $job->markReady(1000.0);
        $job->markProcessing();

        $monitor->track($job);
        $monitor->release($job);

        $this->assertFalse($monitor->isProcessing($job));
    }

    public function testJobIsNotRequeuedBeforeDeadline(): void
    {
        $monitor = new VisibilityMonitor(30, $this->clock);
        $job = Job::create(type: 'a');
        $job->markReady(1000.0);
        $job->markProcessing();
        $monitor->track($job);

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
        $monitor->track($job);

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
        $monitor->track($soon);

        $this->clock->advance(10.0);
        $later->markProcessing();
        $monitor->track($later);

        $this->clock->advance(20.0);
        $expired = $monitor->requeueExpired();

        $this->assertCount(1, $expired);
        $this->assertSame('soon', $expired[0]->getType());
        $this->assertTrue($monitor->isProcessing($later));
    }
}
