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
    /**
     * The whole state machine, stated once: event => (state it is legal
     * from => state it leads to). Anything absent throws.
     *
     * Keyed by ->name because enum cases cannot be array keys.
     *
     *   FROM        start   assign   finish     drain      die
     *   ───────────────────────────────────────────────────────
     *   STARTING    IDLE    -        -          STOPPING   DEAD
     *   IDLE        -       BUSY     -          STOPPING   DEAD
     *   BUSY        -       -        IDLE       DRAINING   DEAD
     *   DRAINING    -       -        STOPPING   DRAINING   DEAD
     *   STOPPING    -       -        -          STOPPING   DEAD
     *   DEAD        -       -        -          DEAD       DEAD
     *
     * The two entries that carry the design:
     *
     *  - BUSY + drain = DRAINING, not STOPPING. A drained worker that is
     *    holding a job is left completely alone to finish it; only the
     *    dispatch of NEW work stops. That is what makes a graceful
     *    shutdown graceful.
     *  - DRAINING + finish = STOPPING, not IDLE. A worker that answered its
     *    last request must not become available again, or a shutdown could
     *    hand it one more job on the way out.
     *
     * DRAINING and DEAD tolerate drain() as a no-op rather than an error:
     * draining something already on its way out is a step backwards, not a
     * bug.
     *
     * @var array<string, array<string, WorkerState>>
     */
    private const array TRANSITIONS = [
        'start' => [
            'STARTING' => WorkerState::IDLE,
        ],
        'assign' => [
            'IDLE' => WorkerState::BUSY,
        ],
        'finish' => [
            'BUSY' => WorkerState::IDLE,
            'DRAINING' => WorkerState::STOPPING,
        ],
        'drain' => [
            'STARTING' => WorkerState::STOPPING,
            'IDLE' => WorkerState::STOPPING,
            'BUSY' => WorkerState::DRAINING,
            'DRAINING' => WorkerState::DRAINING,
            'STOPPING' => WorkerState::STOPPING,
            'DEAD' => WorkerState::DEAD,
        ],
        'die' => [
            'STARTING' => WorkerState::DEAD,
            'IDLE' => WorkerState::DEAD,
            'BUSY' => WorkerState::DEAD,
            'DRAINING' => WorkerState::DEAD,
            'STOPPING' => WorkerState::DEAD,
            'DEAD' => WorkerState::DEAD,
        ],
    ];

    /**
     * How long shutdown() waits for a worker to notice its socket closed
     * and exit by itself, before reaching for SIGKILL. A worker between
     * jobs takes microseconds; this is only generous because the cost of
     * being wrong is killing a process that was about to leave politely.
     */
    private const float POLITENESS_BUDGET = 0.05;

    private const int EXIT_POLL_INTERVAL_US = 1_000;

    /** @var list<resource> */
    private static array $allStreams = [];

    private WorkerState $state = WorkerState::STARTING;

    private ?Job $currentJob = null;

    private mixed $stream = null;

    private int $pid = 0;

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
            // A fork inherits its parent's signal handlers, and the parent
            // here is the runtime: without this reset, a SIGTERM to the
            // process group would run QueueRuntime::stop() inside every
            // worker. A worker has no shutdown of its own to run - it
            // leaves when its socket closes.
            pcntl_signal(SIGTERM, SIG_DFL);
            pcntl_signal(SIGINT, SIG_DFL);

            // The pipes belonging to workers forked before this one. The
            // child has no business holding them open, and a socket kept
            // alive by a third process never reaches EOF at the other end.
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

    /**
     * Hands the job to the worker process, over the socket, as JSON.
     *
     * Throws WorkerDiedException if the process is already gone - killed
     * while it sat IDLE, and killed recently enough that nothing has
     * reaped it yet. The write to a socket whose peer is dead is where we
     * find out. The worker is marked DEAD here so it stops being handed
     * work, and the caller still owns the job: see
     * JobDispatcher::dispatch(), which puts it back.
     */
    public function assign(Job $job): void
    {
        if ($this->state !== WorkerState::IDLE) {
            throw new LogicException('Cannot assign a job to a worker that is not idle');
        }

        $payload = json_encode($job->toArray(), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);

        try {
            self::writeAll($this->stream, $payload);
        } catch (RuntimeException $e) {
            $this->apply('die');

            throw new WorkerDiedException(
                sprintf('Worker %d (pid %d) died before it could be given a job', $this->id, $this->pid),
                previous: $e,
            );
        }

        $this->currentJob = $job;
        $this->apply('assign');
    }

    /**
     * Notices a worker that died while it was NOT holding a job, without
     * blocking. Returns whether this call is what found it.
     *
     * Only IDLE and STARTING are checked, and that division is the whole
     * design:
     *
     *   - A worker that dies while BUSY is already detected, with its job,
     *     by WorkerPool::poll() - the socket reaches EOF, collect() reports
     *     an outcome with a null result, and the dispatcher requeues. That
     *     path must stay the one that handles it, because it is the only
     *     one that knows which job was lost.
     *   - A worker that dies while idle has no job and no reader waiting on
     *     its socket. Nothing selects on it, so nothing notices - until it
     *     is handed work and the write fails. This is what closes that gap.
     *   - DRAINING and STOPPING are skipped deliberately: those processes
     *     are leaving because WE told them to. Reaping them here would
     *     count a deliberate shutdown as a crash.
     */
    public function reap(): bool
    {
        if ($this->pid <= 0) {
            return false;
        }

        if ($this->state !== WorkerState::IDLE && $this->state !== WorkerState::STARTING) {
            return false;
        }

        // 0 means "still running". A pid means it just exited and we have
        // now collected it; -1 means it is not our child any more, which
        // for a worker we forked ourselves also means it is gone.
        if (pcntl_waitpid($this->pid, $status, WNOHANG) === 0) {
            return false;
        }

        $this->apply('die');

        return true;
    }

    /**
     * Reads the worker's answer, if it has one - PLAN.md Phase 6's ACK and
     * NACK, arriving on the socket.
     *
     * $timeout is in seconds: 0.0 polls, null waits indefinitely, anything
     * else waits at most that long. Returns null if nothing arrived in
     * time, or if this worker is not working on anything.
     *
     * A premature EOF - the socket closing with no answer on it - is a
     * crash: the worker is marked DEAD and the outcome carries a null
     * result, which is what tells the dispatcher this job was never
     * acknowledged and has to go back. Distinguishing "failed" from
     * "vanished" is the whole point of returning an outcome rather than a
     * JobResult here.
     */
    public function collect(?float $timeout = 0.0): ?WorkerOutcome
    {
        // isWorking() rather than a state check: a worker drained while
        // holding a job is DRAINING, and its answer still has to be read.
        if (!$this->isWorking() || !is_resource($this->stream)) {
            return null;
        }

        $read = [$this->stream];
        $write = null;
        $except = null;
        $ready = @stream_select($read, $write, $except, ...self::selectTimeout($timeout));
        if ($ready === false || $ready === 0) {
            return null;
        }

        $job = $this->currentJob;
        $this->currentJob = null;

        if ($job === null) {
            throw new LogicException('Worker returned without an assigned job');
        }

        $frame = $this->readFrame();

        // No frame means EOF: the process died holding this job. The null
        // result is what tells the dispatcher the job was never
        // acknowledged - see WorkerOutcome.
        if ($frame === null) {
            $this->apply('die');

            return new WorkerOutcome($job, null);
        }

        $data = json_decode($frame, true, flags: JSON_THROW_ON_ERROR);

        // BUSY -> IDLE, or DRAINING -> STOPPING for a worker that was
        // retired while it finished this last job.
        $this->apply('finish');

        return new WorkerOutcome($job, $this->decodeResult($data));
    }

    public function getId(): int
    {
        return $this->id;
    }

    public function getStream(): mixed
    {
        return $this->stream;
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

    /** Assigned a job right now. Not the same as isWorking() - see drain(). */
    public function isBusy(): bool
    {
        return $this->state === WorkerState::BUSY;
    }

    /**
     * Holding a job right now, whatever the state says.
     *
     * The distinction that used to need a separate boolean: the STATE says
     * whether new work may be dispatched here, and this says whether work
     * is happening. A worker drained mid-job is DRAINING - not available,
     * still working.
     */
    public function isWorking(): bool
    {
        return $this->currentJob !== null;
    }

    public function isDead(): bool
    {
        return $this->state === WorkerState::DEAD;
    }

    /** On its way out: retired mid-job, or already stopping. */
    public function isDraining(): bool
    {
        return $this->state === WorkerState::DRAINING || $this->state === WorkerState::STOPPING;
    }

    /**
     * Takes the worker out of rotation - PLAN.md Phase 15.
     *
     * A worker holding a job becomes DRAINING and is left completely alone:
     * it finishes, and its answer is applied exactly as it would have been.
     * One with nothing to do has no reason to wait, so its socket is closed
     * straight away, which is the EOF its process exits on.
     *
     * Idempotent, and a no-op for a worker already leaving or already dead.
     */
    public function drain(): void
    {
        $wasIdle = $this->state === WorkerState::IDLE || $this->state === WorkerState::STARTING;

        $this->apply('drain');

        if ($wasIdle) {
            $this->terminate();
        }
    }

    public function markDead(): void
    {
        $this->apply('die');
    }

    /**
     * Closes our end of the socket and collects the process if it has
     * already gone. The polite half of shutdown(): a worker waiting for its
     * next job reads EOF and returns from its loop by itself.
     */
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

    /**
     * Ends the process for good - PLAN.md Phase 15's last step.
     *
     * Closing the socket is the polite request: a worker waiting for its
     * next job reads EOF and returns from its loop on its own, which is
     * how every worker that is between jobs exits. POLITENESS_BUDGET is
     * how long that is given to happen - microseconds, in practice.
     *
     * What is left after that is a process inside a handler that outlasted
     * the grace period, and it gets SIGKILL rather than SIGTERM: it has no
     * handler installed for a signal (see spawn(), which resets them in
     * the child) and nothing to save. Its job was never acknowledged, so
     * the visibility timeout brings it back - which is exactly why killing
     * it is safe.
     *
     * Then waitpid, blocking: after SIGKILL the process is already gone or
     * about to be, and leaving it unreaped would leave a zombie.
     */
    public function shutdown(): void
    {
        if (is_resource($this->stream)) {
            fclose($this->stream);
            $this->stream = null;
        }

        if ($this->pid <= 0) {
            return;
        }

        if (!$this->waitForExit(self::POLITENESS_BUDGET)) {
            posix_kill($this->pid, SIGKILL);
            pcntl_waitpid($this->pid, $status);
        }

        $this->pid = 0;
    }

    /**
     * Polls waitpid until the process is gone or the budget runs out.
     * Returns whether it exited (and was reaped) in time.
     */
    private function waitForExit(float $budget): bool
    {
        $deadline = microtime(true) + $budget;

        while (true) {
            if (pcntl_waitpid($this->pid, $status, WNOHANG) !== 0) {
                return true;
            }

            if (microtime(true) >= $deadline) {
                return false;
            }

            usleep(self::EXIT_POLL_INTERVAL_US);
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
     * One length-prefixed frame from the worker, or null on EOF.
     *
     * Null is not an error here - it is how a crash is detected. See
     * writeAll() for the frame format.
     */
    private function readFrame(): ?string
    {
        $header = self::readExact($this->stream, 4);

        if ($header === false) {
            return null;
        }

        $unpacked = unpack('N', $header);

        if ($unpacked === false) {
            return null;
        }

        $body = self::readExact($this->stream, $unpacked[1]);

        return $body === false ? null : $body;
    }

    /**
     * stream_select()'s timeout, split the way it wants it: whole seconds
     * and microseconds, or [null] to block. Its own signature cannot take
     * a float.
     *
     * @return array{0: ?int, 1?: int}
     */
    private static function selectTimeout(?float $timeout): array
    {
        if ($timeout === null) {
            return [null];
        }

        $seconds = (int) $timeout;

        return [$seconds, (int) (($timeout - $seconds) * 1_000_000)];
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
