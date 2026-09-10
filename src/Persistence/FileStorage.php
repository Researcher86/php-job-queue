<?php

declare(strict_types=1);

namespace App\Persistence;

use RuntimeException;

/**
 * An append-only log, one JSON object per line - PLAN.md Phase 12, Option A.
 *
 * Append rather than rewrite, because appending is the operation that is
 * hard to half-finish: a crash mid-write leaves a truncated last line and
 * every complete line before it intact, where rewriting a whole file can
 * lose all of it. The cost is that the file grows with every state change
 * and load() replays it to find the last word on each job. A real system
 * pairs this with periodic snapshots (Option B) so the replay stays
 * bounded; this one does not, and says so.
 */
final readonly class FileStorage implements JobStorage
{
    public function __construct(
        private string $path,
    ) {
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

    /**
     * Replays the log, last write per key winning.
     *
     * A malformed FINAL line is tolerated and dropped: that is a write torn
     * by the crash we are recovering from, and refusing to start because
     * the last record is half-written would make the log useless exactly
     * when it is needed. A malformed line anywhere else is a different
     * thing - the file was corrupted or is not a job log - and throws.
     */
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
        $lastIndex = count($lines) - 1;

        foreach ($lines as $index => $line) {
            $decoded = json_decode($line, true);

            if (!is_array($decoded) || !isset($decoded['key'], $decoded['data'])) {
                if ($index === $lastIndex) {
                    break;
                }

                throw new RuntimeException("Corrupt record on line {$index} of {$this->path}");
            }

            /** @var array<string, mixed> $data */
            $data = (array) $decoded['data'];
            $rows[(string) $decoded['key']] = $data;
        }

        return $rows;
    }
}
