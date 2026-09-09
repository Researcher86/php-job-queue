<?php

declare(strict_types=1);

namespace App\Tests\Producer;

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
}
