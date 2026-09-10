<?php

declare(strict_types=1);

namespace App\Tests\Worker;

use App\Job\Job;
use App\Worker\WorkerPool;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class WorkerPoolTest extends TestCase
{
    public function testPoolCannotBeEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new WorkerPool(0, static function (Job $job): void {});
    }

    public function testStartCreatesRequestedWorkers(): void
    {
        $pool = new WorkerPool(3, static function (Job $job): void {});
        $pool->start();

        $this->assertSame(3, $pool->count());

        $pool->shutdown();
    }

    public function testAllWorkersAreAvailableAfterStart(): void
    {
        $pool = new WorkerPool(3, static function (Job $job): void {});
        $pool->start();

        foreach ($pool->getWorkers() as $worker) {
            $this->assertTrue($worker->isAvailable());
        }

        $pool->shutdown();
    }

    public function testGetAvailableWorkerReturnsFirstWorker(): void
    {
        $pool = new WorkerPool(3, static function (Job $job): void {});
        $pool->start();

        $worker = $pool->getAvailableWorker();

        $this->assertNotNull($worker);
        $this->assertSame(1, $worker->getId());

        $pool->shutdown();
    }

    public function testGetAvailableWorkerReturnsNullBeforeStart(): void
    {
        $pool = new WorkerPool(1, static function (Job $job): void {});

        $this->assertNull($pool->getAvailableWorker());

        $pool->shutdown();
    }

    public function testWorkersReturnToPoolAfterJob(): void
    {
        $pool = new WorkerPool(1, static function (Job $job): void {});
        $pool->start();

        $worker = $pool->getAvailableWorker();
        $this->assertNotNull($worker);
        $worker->assign(Job::create(type: 'test'));

        $result = $pool->poll(true);

        $this->assertNotNull($result);
        $this->assertSame($worker->getId(), $pool->getAvailableWorker()?->getId());

        $pool->shutdown();
    }

    public function testMultipleWorkersProcessJobs(): void
    {
        $pool = new WorkerPool(3, static function (Job $job): void {});
        $pool->start();

        $w1 = $pool->getAvailableWorker();
        $this->assertNotNull($w1);
        $w1->assign(Job::create(type: 'a'));

        $w2 = $pool->getAvailableWorker();
        $this->assertNotNull($w2);
        $this->assertNotSame($w1->getId(), $w2->getId());
        $w2->assign(Job::create(type: 'b'));

        $w3 = $pool->getAvailableWorker();
        $this->assertNotNull($w3);
        $this->assertNotSame($w1->getId(), $w3->getId());
        $w3->assign(Job::create(type: 'c'));

        $completed = 0;
        while ($pool->poll(true) !== null) {
            $completed++;
        }

        $this->assertSame(3, $completed);
        $this->assertSame(0, $pool->busyCount());

        $pool->shutdown();
    }

    public function testReplaceDeadWorkersRestoresCapacity(): void
    {
        $pool = new WorkerPool(1, static function (Job $job): void {
            usleep(100_000);
        });
        $pool->start();

        $worker = $pool->getAvailableWorker();
        $this->assertNotNull($worker);
        $worker->assign(Job::create(type: 'a'));
        posix_kill($worker->getPid(), SIGKILL);

        $result = $pool->poll(true);
        $this->assertNotNull($result);
        $this->assertNull($result->getOutcome()->getResult());
        $this->assertTrue($pool->hasDeadWorkers());

        $this->assertSame(1, $pool->replaceDeadWorkers());
        $this->assertFalse($pool->hasDeadWorkers());
        $this->assertSame(1, $pool->count());
        $this->assertNotNull($pool->getAvailableWorker());

        $pool->shutdown();
    }

    public function testDrainPutsAllWorkersIntoDraining(): void
    {
        $pool = new WorkerPool(2, static function (Job $job): void {});
        $pool->start();

        $pool->drain();

        $this->assertTrue($pool->isDraining());
        foreach ($pool->getWorkers() as $worker) {
            $this->assertTrue($worker->isDraining());
        }
        $this->assertNull($pool->getAvailableWorker());

        $pool->shutdown();
    }

    public function testDrainConsidersBusyWorkerAsDraining(): void
    {
        $pool = new WorkerPool(1, static function (Job $job): void {
            usleep(50_000);
        });
        $pool->start();

        $worker = $pool->getAvailableWorker();
        $this->assertNotNull($worker);
        $worker->assign(Job::create(type: 'a'));
        $pool->drain();

        $this->assertTrue($pool->isDraining());

        $pool->poll(true);
        $this->assertTrue($pool->isDraining());

        $pool->shutdown();
    }
}
