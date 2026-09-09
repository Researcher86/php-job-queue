<?php

declare(strict_types=1);

namespace App\Tests\Worker;

use App\Job\Job;
use App\Worker\WorkerPool;
use App\Worker\WorkerState;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class WorkerPoolTest extends TestCase
{
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
    }
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
    }

    public function testAllWorkersAreAvailableAfterStart(): void
    {
        $pool = new WorkerPool(3, static function (Job $job): void {});
        $pool->start();

        foreach ($pool->getWorkers() as $worker) {
            $this->assertTrue($worker->isAvailable());
        }
    }

    public function testGetAvailableWorkerReturnsFirstWorker(): void
    {
        $pool = new WorkerPool(3, static function (Job $job): void {});
        $pool->start();

        $worker = $pool->getAvailableWorker();

        $this->assertNotNull($worker);
        $this->assertSame(1, $worker->getId());
    }

    public function testAvailableWorkerSelectionSkipsStartingWorker(): void
    {
        $pool = new WorkerPool(2, static function (Job $job): void {});
        $pool->start();

        // A worker report as available only when IDLE; STARTING workers are skipped

        $worker = $pool->getAvailableWorker();
        $this->assertNotNull($worker);
        $this->assertSame(1, $worker->getId());
    }

    public function testGetAvailableWorkerReturnsNullBeforeStart(): void
    {
        $pool = new WorkerPool(1, static function (Job $job): void {});

        $this->assertNull($pool->getAvailableWorker());
    }

    public function testWorkersReturnToPoolAfterJob(): void
    {
        $pool = new WorkerPool(1, static function (Job $job): void {});
        $pool->start();

        $worker = $pool->getAvailableWorker();
        $this->assertNotNull($worker);
        $worker->process(Job::create(type: 'test'));

        // Worker is available again
        $this->assertSame($worker->getId(), $pool->getAvailableWorker()?->getId());
    }

    public function testMultipleWorkersProcessJobs(): void
    {
        $processed = [];
        $pool = new WorkerPool(3, static function (Job $job) use (&$processed): void {
            $processed[] = $job->getType();
        });
        $pool->start();

        // Dispatch all three jobs to the three available workers in one pass
        $job1 = Job::create(type: 'job');
        $job2 = Job::create(type: 'job');
        $job3 = Job::create(type: 'job');

        $w1 = $pool->getAvailableWorker();
        $w1?->process($job1);
        $w2 = $pool->getAvailableWorker();
        $w2?->process($job2);
        $w3 = $pool->getAvailableWorker();
        $w3?->process($job3);

        $this->assertCount(3, $processed);
    }

    public function testReplaceDeadWorkersRestoresCapacity(): void
    {
        $pool = new WorkerPool(2, static function (Job $job): void {});
        $pool->start();

        $workers = $pool->getWorkers();
        $workers[0]->markDead();

        $this->assertTrue($pool->hasDeadWorkers());
        $this->assertSame(1, $pool->replaceDeadWorkers());

        $this->assertFalse($pool->hasDeadWorkers());
        $this->assertSame(2, $pool->count());

        foreach ($pool->getWorkers() as $worker) {
            $this->assertTrue($worker->isAvailable());
        }
    }

    public function testReplaceDeadWorkersKeepsWorkerIds(): void
    {
        $pool = new WorkerPool(3, static function (Job $job): void {});
        $pool->start();

        $workers = $pool->getWorkers();
        $workers[1]->markDead();

        $pool->replaceDeadWorkers();

        $this->assertSame([1, 2, 3], array_map(
            static fn ($worker) => $worker->getId(),
            $pool->getWorkers(),
        ));
    }

    public function testReplaceDeadWorkersReturnsZeroWhenNoneDead(): void
    {
        $pool = new WorkerPool(2, static function (Job $job): void {});
        $pool->start();

        $this->assertSame(0, $pool->replaceDeadWorkers());
    }

    public function testReplaceDeadWorkersReplacesMultiple(): void
    {
        $pool = new WorkerPool(3, static function (Job $job): void {});
        $pool->start();

        $workers = $pool->getWorkers();
        $workers[0]->markDead();
        $workers[2]->markDead();

        $this->assertSame(2, $pool->replaceDeadWorkers());
        $this->assertFalse($pool->hasDeadWorkers());
    }
}
