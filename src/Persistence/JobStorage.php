<?php

declare(strict_types=1);

namespace App\Persistence;

/**
 * Somewhere a job's last known state can survive the process - PLAN.md
 * Phase 12.
 *
 * Deliberately tiny: store() by key, load() everything. It is an
 * append-only log keyed by job id, where the last write for a key wins, and
 * that is all recovery needs - not the history of a job, just where it had
 * got to.
 */
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
