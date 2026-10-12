<?php

declare(strict_types=1);

namespace PhpJobQueue\Queue;

use PhpJobQueue\Delivery\DeliveryLostException;
use PhpJobQueue\DLQ\DeadLetterQueue;
use PhpJobQueue\Job\Job;
use PhpJobQueue\Job\JobState;
use PhpJobQueue\Persistence\JobStorage;
use PhpJobQueue\Scheduler\DelayedJobScheduler;
use PhpJobQueue\Support\Clock;
use PhpJobQueue\Support\SystemClock;
use RuntimeException;
use SplQueue;

/**
 * The plain FIFO queue, grown two things since:
 *
 *   - a DelayedJobScheduler for jobs that are not available yet (README § Delayed jobs);
 *   - an optional JobStorage, so the queue can be rebuilt after the queue
 *     process dies (README § Persistence).
 *
 * Ready jobs are an SplQueue, not an array. That is not a style choice:
 * `array_shift()` reindexes the whole array, so it is O(n) per pop and
 * draining a queue of n jobs is O(n^2). Measured before the change -
 * popping everything out of a full queue, no workers involved:
 *
 *    25,000 pops   0.328s
 *    50,000 pops   1.191s
 *   100,000 pops   4.815s
 *   200,000 pops  19.602s
 *
 * Doubling the depth quadrupled the time. SplQueue is a doubly linked
 * list, so dequeue() is O(1) and the drain is linear. php-worker-pool's
 * RequestQueue is an SplQueue for the same reason.
 *
 * There is no ordering decision to make here - that is what PriorityQueue
 * is for.
 */
final class InMemoryQueue implements Queue
{
    /**
     * @param SplQueue<Job> $ready the FIFO of available jobs - O(1) per
     *                             pop, see the class docblock
     */
    public function __construct(
        private readonly Clock $clock = new SystemClock(),
        private readonly ?JobStorage $storage = null,
        // A new one per queue, because the default is evaluated per call.
        // Injectable so a test can look at the schedule directly.
        private readonly DelayedJobScheduler $scheduler = new DelayedJobScheduler(),
        private readonly SplQueue $ready = new SplQueue(),
    ) {
    }

    public function push(Job $job, int $delay = 0): void
    {
        $this->admit($job, $delay);

        // The job's state changed, and the log has to know: recovery reads
        // the LAST thing written about a job.
        $this->persist($job);
    }

    /**
     * Puts a job where it belongs without logging it.
     *
     * The half of push() that restoreFromStorage() wants: replaying the log
     * must not write back every job it just read, or each restart would add
     * a full copy of the log to it.
     */
    private function admit(Job $job, int $delay = 0): void
    {
        // Waiting or not is the scheduler's decision, and the same one for
        // both queues - see DelayedJobScheduler::holdIfNotDue().
        if (!$this->scheduler->holdIfNotDue($job, $delay, $this->clock->now())) {
            $this->ready->enqueue($job);
        }
    }

    public function pop(?float $now = null): ?Job
    {
        $now ??= $this->clock->now();

        foreach ($this->scheduler->releaseDue($now) as $job) {
            $this->ready->enqueue($job);
        }

        return $this->ready->isEmpty() ? null : $this->ready->dequeue();
    }

    public function size(): int
    {
        return $this->readySize() + $this->scheduler->size();
    }

    public function readySize(): int
    {
        return $this->ready->count();
    }

    public function delayedSize(): int
    {
        return $this->scheduler->size();
    }

    public function nextDeadline(): ?float
    {
        return $this->scheduler->nextDeadline();
    }

    /**
     * Rebuilds a queue from what storage remembers - README § Persistence.
     *
     * PROCESSING is the interesting state. A job the log last saw as
     * PROCESSING was in a worker's hands when the process died, and nobody
     * is left to ACK it, so it comes back as READY and runs again. That is
     * the at-least-once bargain restated at the persistence layer: the same
     * job may execute twice, and handlers must be idempotent.
     *
     * Unless that lost delivery was its last. A job whose attempts are
     * already spent is not handed out again - it is marked FAILED, the log
     * is told so, and it goes to $dlq if one is given. This is the restart
     * counterpart of JobDispatcher::redeliverOrDeadLetter(): without it a
     * job that kills the whole process every time came back from every
     * restart, over its budget, forever.
     *
     * COMPLETED and FAILED are terminal and are simply not restored - a
     * finished job is not work.
     *
     * Every record has to be a job. A storage shared with an
     * IdempotencyGuard holds its keys too, and those are refused with an
     * error naming the cause, rather than half-parsed into a job.
     */
    public static function restoreFromStorage(
        JobStorage $storage,
        Clock $clock = new SystemClock(),
        ?DeadLetterQueue $dlq = null,
    ): self {
        $queue = new self($clock, $storage);

        foreach ($storage->load() as $key => $data) {
            if (!isset($data['id'], $data['state'])) {
                throw new RuntimeException(sprintf(
                    'Record "%s" in the job storage is not a job. Is the storage shared with an '
                    . 'IdempotencyGuard? Give the guard a storage of its own.',
                    $key,
                ));
            }

            $job = Job::fromArray($data);

            if ($job->getState() === JobState::PROCESSING) {
                if ($job->getAttempts() >= $job->getMaxAttempts()) {
                    $reason = new DeliveryLostException('The process died while the job was in flight.');
                    $job->markFailed($clock->now(), $reason->getMessage());
                    // A state change, so unlike everything else restored
                    // here it IS logged: the next restart must not find
                    // the job PROCESSING and decide this all over again.
                    $queue->persist($job);
                    $dlq?->add($job, $reason);

                    continue;
                }

                $job->markRetry($clock->now());
            }

            if (!$job->getState()->isTerminal()) {
                // admit(), not push(): see admit(). Nothing is lost by not
                // re-logging - a job restored as READY that dies again
                // before being dispatched is still logged as PROCESSING,
                // and comes back the same way next time.
                $queue->admit($job);
            }
        }

        return $queue;
    }

    private function persist(Job $job): void
    {
        $this->storage?->store($job->getId()->toString(), $job->toArray());
    }
}
