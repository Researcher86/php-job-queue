<?php

declare(strict_types=1);

namespace App\Persistence;

use RuntimeException;

final class FileStorage implements JobStorage
{
    private string $path;

    public function __construct(string $path)
    {
        $this->path = $path;
    }

    public function store(string $key, array $data): void
    {
        $line = json_encode([
            'key' => $key,
            'data' => $data,
        ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);

        if (@file_put_contents($this->path, $line . "\n", FILE_APPEND) === false) {
            throw new RuntimeException("Failed to write job log to {$this->path}");
        }
    }

    public function load(): array
    {
        if (!file_exists($this->path)) {
            return [];
        }

        $lines = file($this->path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        if ($lines === false) {
            throw new RuntimeException("Failed to read job log from {$this->path}");
        }

        $rows = [];
        foreach ($lines as $line) {
            $decoded = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
            $rows[$decoded['key']] = $decoded['data'];
        }

        return $rows;
    }
}
