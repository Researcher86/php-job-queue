<?php

declare(strict_types=1);

namespace App\Master;

use App\Dispatcher\JobDispatcher;
use App\Support\Clock;
use App\Support\SystemClock;

/**
 * The long-running process: the loop that keeps the queue moving, and the
 * one thing that knows how to stop - PLAN.md Phase 15.
 *
 * Everything it needs already exists; what was missing was something to
 * call it all on a schedule. JobDispatcher::drain() runs until the work
 * runs out, which is right for a script and wrong for a server: a delayed
 * job due in ten minutes, or a job whose visibility timeout has not yet
 * expired, is work that does not exist yet. A runtime waits for it.
 *
 * ## One tick
 *
 *   dispatch every free worker  ->  apply every answer that arrived
 *   ->  reclaim jobs whose ACK went overdue  ->  wait
 *
 * Reaping dead workers happens inside the dispatch step (see
 * WorkerPool::maintain()), which is also where a crashed worker is
 * replaced.
 *
 * ## The wait is the interesting part
 *
 * A loop with no wait in it burns a core; a loop with a fixed sleep in it
 * adds that sleep to the latency of every job. So the wait is whichever
 * comes first of:
 *
 *   - a worker answering, which the poll's stream_select wakes on;
 *   - the next deadline the runtime owns - a delayed job coming due, or a
 *     visibility timeout expiring - which JobDispatcher::nextDeadline()
 *     reports without scanning anything;
 *   - $maxWait, so a signal is never more than that away from being acted
 *     on, and a queue that receives work from another process is noticed.
 *
 * With no worker busy there is nothing to select on, so the wait is a
 * plain sleep of the same length.
 *
 * ## Stopping
 *
 * SIGTERM and SIGINT set a flag; they do not shut anything down from
 * inside the handler. The loop notices the flag on its next pass and
 * leaves through the same shutdown path a direct stop() call takes.
 * Nothing important happens in a signal handler, which is the only way to
 * keep the ordering of the shutdown steps knowable.
 */
final class QueueRuntime
{
    /**
     * Longest the loop will wait for anything. Also the worst-case delay
     * between a SIGTERM arriving and the shutdown starting.
     */
    private const float DEFAULT_MAX_WAIT = 0.05;

    /** How long busy workers get to finish once shutdown starts. */
    private const float DEFAULT_SHUTDOWN_GRACE = 5.0;

    private bool $running = false;

    private bool $stopping = false;

    public function __construct(
        private readonly JobDispatcher $dispatcher,
        private readonly Clock $clock = new SystemClock(),
        private readonly float $maxWait = self::DEFAULT_MAX_WAIT,
        private readonly float $shutdownGrace = self::DEFAULT_SHUTDOWN_GRACE,
    ) {
    }

    /**
     * Forks the workers, installs the signal handlers, and ticks until
     * something asks it to stop. Returns once the shutdown has finished.
     *
     * A runtime is one-shot: run() does not clear a stop that was already
     * requested, so a SIGTERM landing during startup cannot be lost, and a
     * runtime that has stopped stays stopped. Starting again means building
     * another one.
     */
    public function run(): void
    {
        $this->running = true;

        // Workers first, handlers second: a fork inherits the handlers, and
        // a worker that inherited this one would try to run a runtime
        // shutdown of its own. Worker::spawn() resets them in the child as
        // well, which covers the workers forked later as replacements.
        $this->dispatcher->start();
        $this->installSignalHandlers();

        try {
            while (!$this->stopping) {
                $this->tick();
            }
        } finally {
            $this->restoreSignalHandlers();
            $this->running = false;
            $this->dispatcher->shutdown($this->shutdownGrace);
        }
    }

    /**
     * One pass of the loop. Public because it is the honest way to test a
     * runtime: a test drives the ticks itself, with a clock it controls,
     * instead of racing a background process.
     *
     * Returns how many jobs it dispatched.
     */
    public function tick(): int
    {
        $dispatched = $this->dispatcher->dispatchPending();

        $wait = $this->waitTime();

        if ($this->dispatcher->hasWorkInFlight()) {
            // stream_select does the waiting, and returns early the moment
            // a worker answers.
            $this->dispatcher->collect($wait);
        } else {
            $this->dispatcher->collect();
            $this->sleep($wait);
        }

        $this->dispatcher->requeueExpired();

        return $dispatched;
    }

    /**
     * Asks the loop to stop. Idempotent, safe from a signal handler, and
     * does nothing itself - the shutdown runs in run(), on the loop's own
     * thread of control. Calling it before run() means the loop never
     * ticks; see run().
     */
    public function stop(): void
    {
        $this->stopping = true;
    }

    public function isRunning(): bool
    {
        return $this->running;
    }

    public function isStopping(): bool
    {
        return $this->stopping;
    }

    /**
     * How long this tick may wait: until the next deadline the runtime owns,
     * capped at $maxWait so signals stay responsive and work arriving from
     * elsewhere is still noticed.
     */
    private function waitTime(): float
    {
        $deadline = $this->dispatcher->nextDeadline();

        if ($deadline === null) {
            return $this->maxWait;
        }

        return max(0.0, min($this->maxWait, $deadline - $this->clock->now()));
    }

    private function sleep(float $seconds): void
    {
        if ($seconds > 0.0) {
            usleep((int) ($seconds * 1_000_000));
        }
    }

    private function installSignalHandlers(): void
    {
        // Dispatched between statements rather than at the next
        // declare(ticks) point - so the loop does not need one, and a
        // signal arriving mid-sleep is acted on when the sleep returns.
        pcntl_async_signals(true);

        foreach ([SIGTERM, SIGINT] as $signal) {
            pcntl_signal($signal, $this->stop(...));
        }
    }

    private function restoreSignalHandlers(): void
    {
        foreach ([SIGTERM, SIGINT] as $signal) {
            pcntl_signal($signal, SIG_DFL);
        }
    }
}
