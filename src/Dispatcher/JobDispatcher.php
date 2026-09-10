<?php

declare(strict_types=1);

namespace App\Dispatcher;

use App\DLQ\DeadLetterQueue;
use App\Job\Job;
use App\Job\JobState;
use App\Metrics\MetricsCollector;
use App\Metrics\QueueMetrics;
use App\Persistence\JobStorage;
use App\Queue\Queue;
use App\Retry\RetryPolicy;
use App\Support\Clock;
use App\Support\SystemClock;
use App\Timeout\VisibilityMonitor;
use App\Worker\Worker;
use App\Worker\WorkerDiedException;
use App\Worker\WorkerOutcome;
use App\Worker\WorkerPool;
use Throwable;

/**
 * The middle of the system: takes jobs off a queue, gives them to free
 * workers, and decides what each answer means - PLAN.md Phases 5 to 7.
 *
 * Everything else in the project is a piece this one holds together, and
 * all of them are optional. Given only a queue and a pool it dispatches
 * and completes jobs; given a RetryPolicy it retries them, a
 * DeadLetterQueue it retires the hopeless ones, a JobStorage it survives
 * its own death, a MetricsCollector it can be watched, a visibility
 * timeout it recovers jobs whose worker vanished. That is why they are
 * nullable constructor arguments rather than required collaborators: each
 * one is a mechanism you can read on its own, and see the system work
 * without.
 *
 * ## Two halves, deliberately apart
 *
 * dispatchPending() fills every free worker; collect() applies every
 * answer that arrived. Keeping them separate is what lets a pool of N run
 * N jobs at once - a loop that waits for each job before dispatching the
 * next has a pool of one, whatever its size says. drain() and
 * QueueRuntime are both those two halves in a loop; they differ only in
 * when they decide to stop.
 *
 * ## What an answer means
 *
 *   result is success        -> ACK  -> COMPLETED
 *   result is failure, tries -> NACK -> READY, after the retry delay
 *   result is failure, done  -> NACK -> FAILED, and a DLQ record
 *   result is NULL           -> the worker died holding the job, so it
 *                               goes straight back to READY and the worker
 *                               is replaced. Not a failed attempt: nothing
 *                               reported anything.
 *   the job is not PROCESSING -> a late answer for a job already resolved,
 *                               counted as a stale ACK and ignored
 *
 * The last one is the visibility timeout showing through: see
 * applyResult().
 */
final class JobDispatcher
{
    private VisibilityMonitor $monitor;

    private bool $accepting = true;

    /** @var array<string, float> */
    private array $startedAt = [];

    public function __construct(
        private Queue $queue,
        private WorkerPool $workerPool,
        private ?RetryPolicy $retryPolicy = null,
        private Clock $clock = new SystemClock(),
        ?int $visibilityTimeout = null,
        private ?DeadLetterQueue $dlq = null,
        private ?JobStorage $storage = null,
        private ?MetricsCollector $metrics = null,
    ) {
        $this->monitor = new VisibilityMonitor($visibilityTimeout, $clock);
    }

    /**
     * Dispatches one job to one free worker and waits for its answer.
     *
     * The synchronous shape makes it useful for a test or a one-off script
     * that wants a single job to have happened by the time the call
     * returns. QueueRuntime does not use it: a runtime that waits for each
     * job before dispatching the next has a pool of one, whatever its size
     * says.
     *
     * Returns whether a job was dispatched - false if no worker was free,
     * no job was available, or the worker turned out to be dead.
     */
    public function dispatchNext(): bool
    {
        if (!$this->accepting) {
            return false;
        }

        $this->workerPool->maintain();

        $worker = $this->workerPool->getAvailableWorker();
        if ($worker === null) {
            return false;
        }

        $job = $this->queue->pop();
        if ($job === null) {
            return false;
        }

        if (!$this->dispatch($worker, $job)) {
            return false;
        }

        $this->collect(null);

        return true;
    }

    /**
     * Fills every free worker from the queue, without waiting for any of
     * them. Returns how many jobs went out.
     *
     * The half of the loop that moves work forward; collect() is the other
     * half. Keeping them apart is what lets a pool of N run N jobs at once
     * instead of one at a time.
     */
    public function dispatchPending(): int
    {
        if (!$this->accepting) {
            return 0;
        }

        $this->workerPool->maintain();

        $dispatched = 0;

        while (($worker = $this->workerPool->getAvailableWorker()) !== null) {
            $job = $this->queue->pop();

            if ($job === null) {
                break;
            }

            if ($this->dispatch($worker, $job)) {
                $dispatched++;
            }
        }

        return $dispatched;
    }

    /**
     * Applies every answer that has arrived, waiting up to $timeout seconds
     * for the first one. Returns how many were applied.
     *
     * $timeout is in seconds: 0.0 polls, null waits until something
     * arrives, anything else waits at most that long. Once the first answer
     * is in, the rest are drained without waiting - if three workers
     * finished while we were busy, all three results are applied in this
     * call rather than one per loop iteration.
     */
    public function collect(?float $timeout = 0.0): int
    {
        $applied = 0;

        for ($outcome = $this->workerPool->poll($timeout); $outcome !== null; $outcome = $this->workerPool->poll()) {
            $this->applyResult($outcome);
            $applied++;
        }

        return $applied;
    }

    /**
     * Runs the queue until it is empty and every dispatched job has been
     * answered, then tears the pool down. Returns how many jobs were
     * dispatched - which counts redeliveries, so it can exceed the number
     * of distinct jobs.
     *
     * The batch counterpart to QueueRuntime: same two halves, no signals,
     * and it stops when the work runs out instead of waiting for more.
     * Retries scheduled with a delay come back inside this loop, so a
     * FakeClock that never advances will leave them behind - which is what
     * makes it usable in a test at all.
     */
    public function drain(): int
    {
        $dispatched = 0;
        $this->workerPool->start();

        try {
            while (true) {
                $dispatched += $this->dispatchPending();

                if ($this->collect() > 0) {
                    continue;
                }

                // Nothing came back this time round. If nobody is working,
                // nothing is going to.
                if ($this->workerPool->busyCount() === 0) {
                    break;
                }

                if ($this->collect(null) === 0) {
                    break;
                }
            }
        } finally {
            $this->workerPool->shutdown();
        }

        return $dispatched;
    }

    /**
     * Returns every job whose ACK went overdue to the queue, and reports
     * how many - PLAN.md Phase 9.
     *
     * Nothing calls this on a timer by itself: QueueRuntime calls it once
     * per tick, and a test calls it when it wants to ask "what would the
     * monitor reclaim right now?".
     */
    public function requeueExpired(): int
    {
        $count = 0;

        foreach ($this->monitor->requeueExpired() as $expired) {
            $this->queue->push($expired);
            $count++;
        }

        return $count;
    }

    /**
     * The next instant at which the runtime has something to do on its own
     * - the earliest of a delayed job becoming available and a visibility
     * deadline running out - or null if it is only waiting on workers.
     *
     * What lets the loop sleep instead of poll: see QueueRuntime::tick().
     */
    public function nextDeadline(): ?float
    {
        $deadlines = array_filter([$this->queue->nextDeadline(), $this->monitor->nextDeadline()]);

        return $deadlines === [] ? null : min($deadlines);
    }

    /** Whether any worker is holding a job right now. */
    public function hasWorkInFlight(): bool
    {
        return $this->workerPool->busyCount() > 0;
    }

    /** Forks the workers, if they are not running already. */
    public function start(): void
    {
        $this->workerPool->start();
    }

    public function isProcessing(Job $job): bool
    {
        return $this->monitor->isProcessing($job);
    }

    /**
     * A gauge reading of the whole system, right now - PLAN.md Phase 14.
     *
     * Read from the live objects rather than accumulated as events, because
     * a gauge that is maintained by hand drifts: every push, pop, crash and
     * requeue would have to remember to adjust it, and the one path that
     * forgets is invisible.
     */
    public function observe(): QueueMetrics
    {
        return new QueueMetrics(
            ready: $this->queue->readySize(),
            delayed: $this->queue->delayedSize(),
            processing: $this->monitor->size(),
            workers: $this->workerPool->count(),
            busyWorkers: $this->workerPool->busyCount(),
            deadLettered: $this->dlq?->size() ?? 0,
        );
    }

    public function isAccepting(): bool
    {
        return $this->accepting;
    }

    /**
     * Graceful shutdown - PLAN.md Phase 15.
     *
     *   stop accepting  ->  stop dispatching  ->  let workers finish
     *   ->  apply their results  ->  tear the pool down
     *
     * $grace bounds the third step, in seconds; null waits as long as it
     * takes. When it runs out, WorkerPool::shutdown() kills whatever is
     * left, and the jobs those workers were holding stay PROCESSING: they
     * were never acknowledged, so they come back through the visibility
     * timeout (or, across a restart, through the persistence log). Losing
     * the process must not mean losing the job - that is the invariant, and
     * a bounded shutdown is only safe because of it.
     *
     * The grace period is measured against the wall clock rather than the
     * injected Clock, deliberately: it is how long a real forked process
     * gets to finish its work, and no amount of faking time makes a fork
     * run faster. For SystemClock the two are the same thing anyway.
     */
    public function shutdown(?float $grace = null): void
    {
        $this->accepting = false;

        // Idle workers stop now; busy ones are left completely alone to
        // finish what they are doing.
        $this->workerPool->drain();

        $deadline = $grace === null ? null : microtime(true) + $grace;

        while ($this->workerPool->busyCount() > 0) {
            $remaining = $deadline === null ? null : $deadline - microtime(true);

            if ($remaining !== null && $remaining <= 0.0) {
                break;
            }

            // An indefinite wait that comes back empty means every busy
            // worker died without answering - there is nothing left to
            // wait for.
            if ($this->collect($remaining) === 0 && $remaining === null) {
                break;
            }
        }

        $this->workerPool->shutdown();
    }

    /**
     * Hands one job to one worker, and returns whether it got there.
     *
     * The order matters and is the invariant of PLAN.md Phase 5: the job is
     * marked PROCESSING and registered with the visibility monitor BEFORE
     * it is written to the socket, so there is no instant at which the job
     * is neither in the queue nor accounted for as in flight. A job must
     * not be able to fall between the two.
     *
     * If the worker turns out to be dead (killed while idle, too recently
     * for maintain() to have noticed), the job comes straight back to the
     * queue and this returns false. The attempt it consumed is not given
     * back: an attempt in this system counts a DELIVERY, not a successful
     * run, which is the same accounting the visibility timeout uses when it
     * requeues a job whose worker vanished. Real queues count receipts too.
     */
    private function dispatch(Worker $worker, Job $job): bool
    {
        // Read before markProcessing(), which clears it: the job is no
        // longer waiting, so it no longer has a time at which it became
        // available. What it waited FOR is measured from that instant, not
        // from creation - a job deliberately delayed by an hour did not
        // spend an hour queued behind a busy pool.
        $availableAt = $job->getAvailableAt();

        $job->markProcessing();
        $this->monitor->track($job);
        $this->recordStart($job);

        if ($availableAt !== null) {
            $this->metrics?->recordLatency(
                MetricsCollector::LATENCY_QUEUE_WAIT,
                max(0.0, $this->clock->now() - $availableAt),
            );
        }

        try {
            $worker->assign($job);
        } catch (WorkerDiedException) {
            $this->monitor->release($job);
            $this->takeStart($job);
            $job->markRetry($this->clock->now());
            $this->queue->push($job);
            $this->workerPool->maintain();

            return false;
        }

        return true;
    }

    private function applyResult(WorkerOutcome $outcome): void
    {
        $job = $outcome->getJob();

        // A late answer for a job the queue has already resolved. It
        // happens without anything going wrong: the visibility timeout
        // expired while the handler was still running, the job was handed
        // to a second worker, and now the first one has finished and is
        // reporting on a delivery nobody is waiting for any more.
        //
        // Ignored rather than applied, and deliberately without releasing
        // the monitor: what it tracks under this job's id is the LIVE
        // delivery, and releasing here would leave that one untracked -
        // turning a harmless duplicate into a genuinely lost job.
        if ($job->getState() !== JobState::PROCESSING) {
            $this->metrics?->increment(MetricsCollector::STALE_ACKS);

            return;
        }

        $result = $outcome->getResult();
        $this->monitor->release($job);

        if ($result === null) {
            $job->markRetry($this->clock->now());
            $this->queue->push($job);
            $this->workerPool->replaceDeadWorkers();
            return;
        }

        $startedAt = $this->takeStart($job);

        if ($startedAt !== null) {
            $this->metrics?->recordLatency(MetricsCollector::LATENCY_EXECUTION, $this->clock->now() - $startedAt);
        }

        if ($result->isSuccess()) {
            $job->markCompleted();
            $this->persist($job);
            $this->metrics?->increment(MetricsCollector::JOBS_COMPLETED);
            $this->metrics?->recordLatency(
                MetricsCollector::LATENCY_END_TO_END,
                $this->clock->now() - $job->getCreatedAt(),
            );
            return;
        }

        $this->handleFailure($job, $result->getException());
    }

    private function handleFailure(Job $job, ?Throwable $exception): void
    {
        if ($job->getAttempts() < $job->getMaxAttempts()) {
            $delay = $this->retryPolicy?->nextDelay($job) ?? 0;
            $job->markRetry($this->clock->now() + $delay);
            $this->queue->push($job);
            $this->persist($job);
            $this->metrics?->increment(MetricsCollector::JOBS_RETRIED);
            return;
        }

        $job->markFailed();
        $this->persist($job);
        $this->metrics?->increment(MetricsCollector::JOBS_FAILED);
        if ($this->dlq !== null && $exception !== null) {
            $this->dlq->add($job, $exception);
            $this->metrics?->increment(MetricsCollector::JOBS_DEAD_LETTERED);
        }
    }

    private function persist(Job $job): void
    {
        $this->storage?->store($job->getId()->toString(), $job->toArray());
    }

    private function recordStart(Job $job): void
    {
        $this->startedAt[$job->getId()->toString()] = $this->clock->now();
    }

    /**
     * When this job was dispatched, per the injected clock, removing the
     * record. Null for a job dispatched by something other than this
     * dispatcher - a test driving a Worker directly - which simply has no
     * execution latency to report.
     */
    private function takeStart(Job $job): ?float
    {
        $id = $job->getId()->toString();
        $startedAt = $this->startedAt[$id] ?? null;
        unset($this->startedAt[$id]);

        return $startedAt;
    }
}
