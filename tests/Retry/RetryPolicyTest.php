<?php

declare(strict_types=1);

namespace App\Tests\Retry;

use App\Job\Job;
use App\Retry\ExponentialBackoffRetry;
use App\Retry\FixedDelayRetry;
use PHPUnit\Framework\TestCase;

final class RetryPolicyTest extends TestCase
{
    private function jobWithAttempts(int $attempts): Job
    {
        $job = Job::create(type: 'test');
        $job->markReady(0.0);
        for ($i = 0; $i < $attempts; $i++) {
            $job->markProcessing();
            if ($i < $attempts - 1) {
                $job->markRetry(0.0);
            }
        }

        return $job;
    }

    public function testFixedDelayRetryReturnsConstantDelay(): void
    {
        $policy = new FixedDelayRetry(5);

        $this->assertSame(5, $policy->nextDelay($this->jobWithAttempts(1)));
        $this->assertSame(5, $policy->nextDelay($this->jobWithAttempts(3)));
    }

    public function testExponentialBackoffGrowsGeometrically(): void
    {
        $policy = new ExponentialBackoffRetry(2, 2.0);

        $this->assertSame(2, $policy->nextDelay($this->jobWithAttempts(1)));
        $this->assertSame(4, $policy->nextDelay($this->jobWithAttempts(2)));
        $this->assertSame(8, $policy->nextDelay($this->jobWithAttempts(3)));
    }

    public function testExponentialBackoffWithCustomFactor(): void
    {
        $policy = new ExponentialBackoffRetry(1, 3.0);

        $this->assertSame(1, $policy->nextDelay($this->jobWithAttempts(1)));
        $this->assertSame(3, $policy->nextDelay($this->jobWithAttempts(2)));
        $this->assertSame(9, $policy->nextDelay($this->jobWithAttempts(3)));
    }
}
