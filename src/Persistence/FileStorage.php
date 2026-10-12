<?php

declare(strict_types=1);

namespace PhpJobQueue\Persistence;

use RuntimeException;

/**
 * An append-only log, one JSON object per line - docs/DECISIONS.md §12.
 *
 * Append rather than rewrite, because appending is the operation that is
 * hard to half-finish: a crash mid-write leaves a truncated last line and
 * every complete line before it intact, where rewriting a whole file can
 * lose all of it. The cost is that the file grows with every state change
 * and load() replays it to find the last word on each job. A real system
 * pairs this with periodic snapshots so the replay stays bounded; this one
 * does not, and says so.
 *
 * Several processes may hold the same file - forked workers sharing an
 * IdempotencyGuard's log do. So store() appends under an exclusive flock
 * and load() reads under one too: a half-written line is then only ever
 * one whose writer died, never one still being written, and load() can
 * cut it off without cutting into somebody else's record.
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

        if (@file_put_contents($this->path, $line . "\n", FILE_APPEND | LOCK_EX) === false) {
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
     *
     * Dropped means cut off the FILE, not just skipped in memory. Skipping
     * alone left the torn bytes where they were, with no newline after
     * them, so the next store() glued its record onto the end - one line
     * that was neither record, no longer last. The restart after that one
     * threw "Corrupt record" on a log that had only ever been torn once.
     * So this is the one place load() writes: it truncates the file back to
     * the end of the last whole record, and appends start clean again.
     */
    public function load(): array
    {
        if (!file_exists($this->path)) {
            return [];
        }

        // r+, not r: the torn tail may have to be truncated, and the lock
        // has to be exclusive for that - see the class docblock.
        $handle = @fopen($this->path, 'r+');

        if ($handle === false || !flock($handle, LOCK_EX)) {
            throw new RuntimeException("Failed to read job log from {$this->path}");
        }

        try {
            $contents = stream_get_contents($handle);

            if ($contents === false) {
                throw new RuntimeException("Failed to read job log from {$this->path}");
            }

            return $this->replay($handle, $contents);
        } finally {
            flock($handle, LOCK_UN);
            fclose($handle);
        }
    }

    /**
     * @param resource $handle the locked log, for truncating a torn tail
     *
     * @return array<string, array<string, mixed>>
     */
    private function replay(mixed $handle, string $contents): array
    {
        $rows = [];
        $lines = explode("\n", $contents);
        // The last line with anything on it: the only place a torn write
        // can be, and so the only one that is forgiven.
        $lastIndex = array_key_last(array_filter($lines, static fn (string $line): bool => $line !== ''));
        $offset = 0;

        foreach ($lines as $index => $line) {
            // Byte offset of this line, for the truncation below.
            $start = $offset;
            $offset += strlen($line) + 1;

            if ($line === '') {
                continue;
            }

            $decoded = json_decode($line, true);

            if (!is_array($decoded) || !isset($decoded['key'], $decoded['data'])) {
                if ($index === $lastIndex) {
                    if (!ftruncate($handle, $start)) {
                        throw new RuntimeException("Failed to truncate the torn tail of {$this->path}");
                    }

                    break;
                }

                // +1: explode() counts from zero, a human reading the log does not.
                throw new RuntimeException(sprintf('Corrupt record on line %d of %s', $index + 1, $this->path));
            }

            /** @var array<string, mixed> $data */
            $data = (array) $decoded['data'];
            $rows[(string) $decoded['key']] = $data;
        }

        return $rows;
    }
}
