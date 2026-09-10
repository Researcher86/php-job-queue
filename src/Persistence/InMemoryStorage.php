<?php

declare(strict_types=1);

namespace App\Persistence;

/**
 * Storage that does not survive anything.
 *
 * Which sounds useless and is not: a test can hand the same instance to a
 * second queue and get exactly the restart behaviour of a real log, without
 * a temp file. And running the queue with this is how you see which
 * behaviour comes from persistence and which does not.
 */
final class InMemoryStorage implements JobStorage
{
    /** @var array<string, array<string, mixed>> */
    private array $data = [];

    public function store(string $key, array $data): void
    {
        $this->data[$key] = $data;
    }

    public function load(): array
    {
        return $this->data;
    }
}
