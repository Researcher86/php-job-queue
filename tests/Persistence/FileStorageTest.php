<?php

declare(strict_types=1);

namespace App\Tests\Persistence;

use App\Persistence\FileStorage;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class FileStorageTest extends TestCase
{
    private string $path;

    protected function setUp(): void
    {
        $this->path = sys_get_temp_dir() . '/php-job-queue-' . uniqid('', true) . '.log';
    }

    protected function tearDown(): void
    {
        if (file_exists($this->path)) {
            unlink($this->path);
        }
    }

    public function testStoreAndLoadRoundTrip(): void
    {
        $storage = new FileStorage($this->path);
        $storage->store('job-1', ['state' => 'READY', 'attempts' => 1]);

        $loaded = $storage->load();

        $this->assertSame(['job-1' => ['state' => 'READY', 'attempts' => 1]], $loaded);
    }

    public function testAppendOnlyLogLastWriteWins(): void
    {
        $storage = new FileStorage($this->path);
        $storage->store('job-1', ['state' => 'READY']);
        $storage->store('job-1', ['state' => 'COMPLETED']);

        $loaded = $storage->load();

        $this->assertSame(['job-1' => ['state' => 'COMPLETED']], $loaded);
    }

    public function testLoadMissingFileReturnsEmpty(): void
    {
        $storage = new FileStorage('/nonexistent/path/queue.log');

        $this->assertSame([], $storage->load());
    }

    public function testMultipleKeysAreLoaded(): void
    {
        $storage = new FileStorage($this->path);
        $storage->store('job-1', ['state' => 'READY']);
        $storage->store('job-2', ['state' => 'DELAYED']);

        $loaded = $storage->load();

        $this->assertCount(2, $loaded);
        $this->assertArrayHasKey('job-1', $loaded);
        $this->assertArrayHasKey('job-2', $loaded);
    }

    /**
     * The crash we are recovering from can land in the middle of a write.
     * Refusing to start because the last record is half-written would make
     * the log useless exactly when it matters.
     */
    public function testATornFinalRecordIsDropped(): void
    {
        $storage = new FileStorage($this->path);
        $storage->store('job-1', ['state' => 'READY']);
        $storage->store('job-2', ['state' => 'READY']);

        // Simulate the process dying mid-append.
        file_put_contents($this->path, '{"key":"job-3","da', FILE_APPEND);

        $loaded = (new FileStorage($this->path))->load();

        $this->assertSame(['job-1', 'job-2'], array_keys($loaded));
    }

    public function testCorruptionEarlierInTheLogIsNotHiddenAway(): void
    {
        $storage = new FileStorage($this->path);
        $storage->store('job-1', ['state' => 'READY']);
        file_put_contents($this->path, "not json at all\n", FILE_APPEND);
        $storage->store('job-2', ['state' => 'READY']);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Corrupt record on line 1');

        (new FileStorage($this->path))->load();
    }
}
