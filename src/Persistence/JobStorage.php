<?php

declare(strict_types=1);

namespace App\Persistence;

interface JobStorage
{
    /**
     * @param array<string, mixed> $data
     */
    public function store(string $key, array $data): void;

    /**
     * @return array<string, array<string, mixed>>
     */
    public function load(): array;
}
