<?php

declare(strict_types=1);

namespace App\Tests\Persistence;

use App\Persistence\FileStorage;
use PHPUnit\Framework\TestCase;

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
}