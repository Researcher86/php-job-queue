<?php

declare(strict_types=1);

namespace App\Tests\Producer;

use App\Job\JobPriority;
use App\Metrics\MetricsCollector;
use App\Producer\JobFactory;
use App\Tests\Support\FakeClock;
use PHPUnit\Framework\TestCase;

final class JobFactoryTest extends TestCase
{
    public function testCreatesJobWithType(): void
    {
        $factory = new JobFactory(new FakeClock());

        $job = $factory->create(type: 'send_email');

        $this->assertSame('send_email', $job->getType());
    }

    public function testCreatesJobWithPayload(): void
    {
        $factory = new JobFactory(new FakeClock());
        $payload = ['email' => 'user@example.com'];

        $job = $factory->create(type: 'send_email', payload: $payload);

        $this->assertSame($payload, $job->getPayload());
    }

    public function testCreatesJobWithMaxAttempts(): void
    {
        $factory = new JobFactory(new FakeClock());

        $job = $factory->create(type: 'send_email', maxAttempts: 5);

        $this->assertSame(5, $job->getMaxAttempts());
    }

    public function testCreatesJobWithPriority(): void
    {
        $factory = new JobFactory(new FakeClock());

        $job = $factory->create(type: 'send_email', priority: JobPriority::LOW);

        $this->assertSame(JobPriority::LOW, $job->getPriority());
    }

    /**
     * "Jobs created" is counted here rather than in Producer: a job is
     * created exactly once, whichever route it takes to a queue afterwards.
     */
    public function testCreatedJobsAreCounted(): void
    {
        $metrics = new MetricsCollector();
        $factory = new JobFactory(new FakeClock(), $metrics);

        $factory->create('a');
        $factory->create('b');

        $this->assertSame(2, $metrics->getCounter(MetricsCollector::JOBS_CREATED));
    }
}
