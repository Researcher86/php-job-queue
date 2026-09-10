<?php

declare(strict_types=1);

namespace App\Worker;

use App\Job\Job;
use App\Job\JobResult;
use Closure;
use LogicException;
use RuntimeException;
use Throwable;

final class Worker
{
    private const array TRANSITIONS = [
        'start' => [
            'STARTING' => WorkerState::IDLE,
        ],
        'assign' => [
            'IDLE' => WorkerState::BUSY,
        ],
        'finish' => [
            'BUSY' => WorkerState::IDLE,
        ],
        'die' => [
            'STARTING' => WorkerState::DEAD,
            'IDLE' => WorkerState::DEAD,
            'BUSY' => WorkerState::DEAD,
            'DRAINING' => WorkerState::DEAD,
            'STOPPING' => WorkerState::DEAD,
        ],
    ];

    /** @var list<resource> */
    private static array $allStreams = [];

    private WorkerState $state = WorkerState::STARTING;

    private ?Job $currentJob = null;

    private mixed $stream = null;

    private int $pid = 0;

    private float $assignedAt = 0.0;

    private bool $draining = false;

    public function __construct(
        private readonly int $id,
        private readonly Closure $handler,
    ) {}

    public function spawn(): void
    {
        if ($this->state !== WorkerState::STARTING) {
            throw new LogicException('A worker can only be spawned once');
        }

        $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        if ($pair === false) {
            throw new RuntimeException('Failed to create worker socket pair');
        }
        [$parent, $child] = $pair;
        self::$allStreams[] = $parent;
        self::$allStreams[] = $child;

        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new RuntimeException('Failed to fork worker');
        }

        if ($pid === 0) {
            foreach (self::$allStreams as $resource) {
                if ($resource !== $child && is_resource($resource)) {
                    fclose($resource);
                }
            }
            $this->workerLoop($child);
            exit(0);
        }

        fclose($child);
        $this->stream = $parent;
        $this->pid = $pid;
        $this->apply('start');
    }

    public function assign(Job $job): void
    {
        if ($this->state !== WorkerState::IDLE) {
            throw new LogicException('Cannot assign a job to a worker that is not idle');
        }

        $payload = json_encode($job->toArray(), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
        self::writeAll($this->stream, $payload);

        $this->currentJob = $job;
        $this->assignedAt = microtime(true);
        $this->apply('assign');
    }

    public function collect(bool $block = false): ?WorkerOutcome
    {
        if ($this->state !== WorkerState::BUSY || !is_resource($this->stream)) {
            return null;
        }

        $read = [$this->stream];
        $write = null;
        $except = null;
        $ready = @stream_select($read, $write, $except, $block ? null : 0);
        if ($ready === false || $ready === 0) {
            return null;
        }

        $job = $this->currentJob;
        $startedAt = $this->assignedAt;
        $this->currentJob = null;
        $this->assignedAt = 0.0;

        if ($job === null) {
            throw new LogicException('Worker returned without an assigned job');
        }

        $payload = self::readExact($this->stream, 4);
        if ($payload === false) {
            $this->apply('die');
            return new WorkerOutcome($job, null, $startedAt);
        }

        $unpacked = unpack('N', $payload);
        if ($unpacked === false) {
            $this->apply('die');
            return new WorkerOutcome($job, null, $startedAt);
        }

        $body = self::readExact($this->stream, $unpacked[1]);
        if ($body === false) {
            $this->apply('die');
            return new WorkerOutcome($job, null, $startedAt);
        }

        $data = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
        $outcome = new WorkerOutcome($job, $this->decodeResult($data), $startedAt);

        $this->apply('finish');
        if ($this->draining) {
            $this->state = WorkerState::STOPPING;
        }

        return $outcome;
    }

    public function getId(): int
    {
        return $this->id;
    }

    public function getPid(): int
    {
        return $this->pid;
    }

    public function getState(): WorkerState
    {
        return $this->state;
    }

    public function getCurrentJob(): ?Job
    {
        return $this->currentJob;
    }

    public function isAvailable(): bool
    {
        return $this->state === WorkerState::IDLE;
    }

    public function isBusy(): bool
    {
        return $this->state === WorkerState::BUSY;
    }

    public function isDead(): bool
    {
        return $this->state === WorkerState::DEAD;
    }

    public function isDraining(): bool
    {
        return $this->draining
            || $this->state === WorkerState::DRAINING
            || $this->state === WorkerState::STOPPING;
    }

    public function drain(): void
    {
        if ($this->state === WorkerState::BUSY) {
            $this->draining = true;
            return;
        }

        if ($this->state === WorkerState::IDLE) {
            $this->terminate();
            $this->state = WorkerState::STOPPING;
            return;
        }

        throw new LogicException(sprintf(
            'Cannot drain a worker in state %s',
            $this->state->name,
        ));
    }

    public function markDead(): void
    {
        $this->apply('die');
    }

    public function terminate(): void
    {
        if (is_resource($this->stream)) {
            fclose($this->stream);
            $this->stream = null;
        }
        if ($this->pid > 0) {
            pcntl_waitpid($this->pid, $status, WNOHANG);
        }
    }

    public function shutdown(): void
    {
        if (is_resource($this->stream)) {
            fclose($this->stream);
            $this->stream = null;
        }
        if ($this->pid > 0) {
            pcntl_waitpid($this->pid, $status);
        }
    }

    private function workerLoop(mixed $stream): void
    {
        while (true) {
            $payload = self::readExact($stream, 4);
            if ($payload === false) {
                return;
            }

            $unpacked = unpack('N', $payload);
            if ($unpacked === false || $unpacked[1] === 0) {
                return;
            }

            $body = self::readExact($stream, $unpacked[1]);
            if ($body === false) {
                return;
            }

            try {
                $data = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
                $job = Job::fromArray($data);

                try {
                    $result = ($this->handler)($job);
                    $result = $result instanceof JobResult ? $result : JobResult::success();
                } catch (Throwable $e) {
                    $result = JobResult::failure($e);
                }
            } catch (Throwable $e) {
                fwrite(STDERR, "worker {$this->id} error: {$e->getMessage()}\n");
                $result = JobResult::failure($e);
            }

            $response = json_encode($this->encodeResult($result), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
            self::writeAll($stream, $response);
        }
    }

    /**
     * @param mixed $data
     */
    private function decodeResult(mixed $data): JobResult
    {
        if (!is_array($data) || !isset($data['success'])) {
            return JobResult::success();
        }

        if ($data['success'] === true) {
            return JobResult::success();
        }

        $class = $data['exceptionClass'] ?? RuntimeException::class;
        $message = $data['exceptionMessage'] ?? 'Worker error';
        if (!is_a($class, Throwable::class, true)) {
            $class = RuntimeException::class;
        }

        /** @var class-string<Throwable> $class */
        return JobResult::failure(new $class($message));
    }

    /** @return array{success: bool, exceptionClass: class-string<Throwable>|null, exceptionMessage: ?string} */
    private function encodeResult(JobResult $result): array
    {
        $exception = $result->getException();

        return [
            'success' => $result->isSuccess(),
            'exceptionClass' => $exception !== null ? get_class($exception) : null,
            'exceptionMessage' => $exception?->getMessage(),
        ];
    }

    private function apply(string $event): void
    {
        $next = self::TRANSITIONS[$event][$this->state->name] ?? null;
        if ($next === null) {
            throw new LogicException(sprintf(
                'Illegal transition: cannot %s a worker in state %s',
                $event,
                $this->state->name,
            ));
        }
        $this->state = $next;
    }

    /**
     * Length-prefixed write: guarantees the entire buffer is sent.
     *
     * Frame format: [4-byte big-endian length][payload]
     */
    private static function writeAll(mixed $stream, string $data): void
    {
        $length = strlen($data);
        $header = pack('N', $length);

        $total = 0;
        $buffer = $header . $data;
        $bytes = strlen($buffer);

        while ($total < $bytes) {
            $written = @fwrite($stream, substr($buffer, $total));
            if ($written === false || $written === 0) {
                throw new RuntimeException('Failed to write to worker stream');
            }
            $total += $written;
        }
    }

    /**
     * Length-prefixed read: guarantees exactly N bytes are read.
     *
     * Returns false on premature EOF (worker crash / broken pipe).
     */
    private static function readExact(mixed $stream, int $length): string|false
    {
        $buffer = '';
        $remaining = $length;

        while ($remaining > 0) {
            $chunk = fread($stream, $remaining);
            if ($chunk === false || $chunk === '') {
                return false;
            }
            $buffer .= $chunk;
            $remaining -= strlen($chunk);
        }

        return $buffer;
    }
}