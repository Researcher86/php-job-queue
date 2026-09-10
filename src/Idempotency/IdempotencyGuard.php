<?php

declare(strict_types=1);

namespace App\Idempotency;

use App\Persistence\JobStorage;

final class IdempotencyGuard
{
    private const string RECORD_KIND = 'idempotency';

    /** @var array<string, true> */
    private array $processed = [];

    public function __construct(private readonly ?JobStorage $storage = null)
    {
        if ($storage === null) {
            return;
        }

        foreach ($storage->load() as $key => $data) {
            if (($data['kind'] ?? null) === self::RECORD_KIND) {
                $this->processed[$key] = true;
            }
        }
    }

    public function isProcessed(string $key): bool
    {
        return isset($this->processed[$key]);
    }

    public function markProcessed(string $key): void
    {
        $this->processed[$key] = true;
        $this->storage?->store($key, ['kind' => self::RECORD_KIND]);
    }

    /** @return list<string> */
    public function getProcessedKeys(): array
    {
        return array_keys($this->processed);
    }
}
