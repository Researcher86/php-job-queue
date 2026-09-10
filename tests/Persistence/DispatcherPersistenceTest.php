<?php

declare(strict_types=1);

namespace App\Tests\Persistence;

use App\Dispatcher\JobDispatcher;
use App\Job\Job;
use App\Persistence\FileStorage;
use App\Queue\InMemoryQueue;
use App\Retry\FixedDelayRetry;
use App\Tests\Support\FakeClock;
use App\Tests\Support\Handlers;
use App\Worker\WorkerPool;
use Closure;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * What actually reaches the log, per outcome.
 *
 * Two things write to it - Queue::push() for any job it takes in, and the
 * dispatcher for a state change that does not go into a queue - and
 * without a test that counts, they duplicate each other quietly. The log
 * is last-write-wins over identical content, so a redundant record is not
 * a correctness bug; it is write amplification that nothing complains
 * about, in a design whose stated cost is exactly that.
 *
 * These assertions are on the SEQUENCE, not just the count, because the
 * sequence is the durability story: PROCESSING has to be in there, before
 * the outcome, or an attempt is not durable.
 */
final class DispatcherPersistenceTest extends TestCase
{
    private string $path;

    protected function setUp(): void
    {
        $this->path = sys_get_temp_dir() . '/php-job-queue-records-' . uniqid('', true) . '.log';
    }

    protected function tearDown(): void
    {
        if (file_exists($this->path)) {
            unlink($this->path);
        }
    }

    public function testASuccessfulJobIsWrittenThreeTimes(): void
    {
        $this->assertSame(
            ['READY', 'PROCESSING', 'COMPLETED'],
            $this->statesLoggedFor(Handlers::succeeds(), maxAttempts: 3),
        );
    }

    /**
     * The retry path used to write READY twice: push() wrote the job it
     * took in, and the dispatcher wrote it again straight afterwards.
     */
    public function testARetriedJobIsNotWrittenTwiceForTheSameState(): void
    {
        $this->assertSame(
            ['READY', 'PROCESSING', 'READY'],
            $this->statesLoggedFor($this->fails(), maxAttempts: 3),
        );
    }

    public function testAnExhaustedJobEndsWithItsFailure(): void
    {
        $this->assertSame(
            ['READY', 'PROCESSING', 'FAILED'],
            $this->statesLoggedFor($this->fails(), maxAttempts: 1),
        );
    }

    /**
     * The PROCESSING record is the one that makes an attempt durable, so
     * its attempt count is worth asserting on rather than just its state.
     */
    public function testTheProcessingRecordCarriesTheAttempt(): void
    {
        $this->runOneJob(Handlers::succeeds(), maxAttempts: 3);

        $records = $this->records();

        $this->assertSame(0, $records[0]['attempts'], 'queued, not yet delivered');
        $this->assertSame(1, $records[1]['attempts'], 'delivered once');
        $this->assertSame(1, $records[2]['attempts']);
    }

    /** @return list<string> */
    private function statesLoggedFor(Closure $handler, int $maxAttempts): array
    {
        $this->runOneJob($handler, $maxAttempts);

        return array_map(
            static fn (array $record): string => (string) $record['state'],
            $this->records(),
        );
    }

    private function runOneJob(Closure $handler, int $maxAttempts): void
    {
        $clock = new FakeClock(1000.0);
        $storage = new FileStorage($this->path);
        $queue = new InMemoryQueue($clock, $storage);
        $pool = new WorkerPool(1, $handler);
        $pool->start();

        $queue->push(Job::create(type: 'x', maxAttempts: $maxAttempts, clock: $clock));

        $dispatcher = new JobDispatcher($queue, $pool, new FixedDelayRetry(0), $clock, storage: $storage);
        $dispatcher->dispatchPending();
        $dispatcher->collect(null);

        $pool->shutdown();
    }

    /** @return list<array<string, mixed>> */
    private function records(): array
    {
        $lines = file($this->path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [];

        return array_map(
            static fn (string $line): array => (array) json_decode($line, true, flags: JSON_THROW_ON_ERROR)['data'],
            array_values($lines),
        );
    }

    private function fails(): Closure
    {
        return static function (Job $job): void {
            throw new RuntimeException('boom');
        };
    }
}
