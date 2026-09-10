<?php

declare(strict_types=1);

namespace App\Tests\Delivery;

use App\Delivery\Delivery;
use App\Job\Job;
use PHPUnit\Framework\TestCase;

final class DeliveryTest extends TestCase
{
    public function testGenerationComesFromTheAttemptItRepresents(): void
    {
        $job = $this->processing();

        $first = Delivery::of($job, workerId: 1, now: 1000.0);

        $this->assertSame(1, $first->getGeneration());
        $this->assertSame(1, $job->getAttempts());
    }

    /**
     * Two handings-out of the same job differ, and that is the whole
     * mechanism: markProcessing() moves the attempt count on, so the second
     * lease can never be mistaken for the first.
     */
    public function testASecondDeliveryOfTheSameJobIsNotTheFirst(): void
    {
        $job = $this->processing();
        $first = Delivery::of($job, workerId: 1, now: 1000.0);

        $job->markRetry(1010.0);
        $job->markProcessing();
        $second = Delivery::of($job, workerId: 2, now: 1010.0);

        $this->assertSame(2, $second->getGeneration());
        $this->assertFalse($first->is($second));
        $this->assertFalse($second->is($first));
        $this->assertTrue($first->is($first));
    }

    /** The snapshot must not follow the job it points at. */
    public function testAnIssuedDeliveryDoesNotDriftWithTheJob(): void
    {
        $job = $this->processing();
        $first = Delivery::of($job, workerId: 1, now: 1000.0);

        $job->markRetry(1010.0);
        $job->markProcessing();

        $this->assertSame(1, $first->getGeneration(), 'still delivery 1');
    }

    public function testTwoJobsAtTheSameGenerationAreDifferentDeliveries(): void
    {
        $a = Delivery::of($this->processing(), workerId: 1, now: 1000.0);
        $b = Delivery::of($this->processing(), workerId: 1, now: 1000.0);

        $this->assertSame($a->getGeneration(), $b->getGeneration());
        $this->assertFalse($a->is($b));
    }

    public function testADeliveryWithNoDeadlineIsNeverOverdue(): void
    {
        $delivery = Delivery::of($this->processing(), workerId: 1, now: 1000.0);

        $this->assertNull($delivery->getDeadline());
        $this->assertFalse($delivery->isOverdue(1_000_000.0));
    }

    public function testADeadlineIsReachedAtTheInstantItNames(): void
    {
        $delivery = Delivery::of($this->processing(), workerId: 1, now: 1000.0, deadline: 1030.0);

        $this->assertFalse($delivery->isOverdue(1029.9));
        $this->assertTrue($delivery->isOverdue(1030.0));
        $this->assertTrue($delivery->isOverdue(1030.1));
    }

    public function testItDescribesItselfForALog(): void
    {
        $job = $this->processing();
        $delivery = Delivery::of($job, workerId: 7, now: 1000.0);

        $this->assertSame(
            sprintf('job %s, delivery 1, worker 7', $job->getId()),
            $delivery->describe(),
        );
    }

    private function processing(): Job
    {
        $job = Job::create(type: 'a', maxAttempts: 5);
        $job->markReady(1000.0);
        $job->markProcessing();

        return $job;
    }
}
