<?php

declare(strict_types=1);

namespace App\Persistence;

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
