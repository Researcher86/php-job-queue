<?php

declare(strict_types=1);

namespace App\Tests\Job;

use App\Job\JobResult;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class JobResultTest extends TestCase
{
    public function testSuccessResult(): void
    {
        $result = JobResult::success();

        $this->assertTrue($result->isSuccess());
        $this->assertNull($result->getException());
    }

    public function testFailureResult(): void
    {
        $exception = new RuntimeException('boom');
        $result = JobResult::failure($exception);

        $this->assertFalse($result->isSuccess());
        $this->assertSame($exception, $result->getException());
    }
}
