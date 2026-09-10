# Architecture

The [README](../README.md) is the tour: what each mechanism is and why it
exists. This is the layer under it — the invariants, the state machines,
what happens in what order when something breaks, and where each claim is
held by a test.

If you are reading the code, read this alongside
[`JobDispatcher`](../src/Dispatcher/JobDispatcher.php) and
[`QueueRuntime`](../src/Master/QueueRuntime.php); everything else is
something one of those two calls.

---

## Contents

1. [Processes and what lives where](#processes-and-what-lives-where)
2. [The invariants](#the-invariants)
3. [Following one job](#following-one-job)
4. [Following one failure](#following-one-failure)
5. [The IPC protocol](#the-ipc-protocol)
6. [The loop, in detail](#the-loop-in-detail)
7. [Ownership of a job](#ownership-of-a-job)
8. [Component responsibilities](#component-responsibilities)
9. [Where each mechanism is tested](#where-each-mechanism-is-tested)

---

## Processes and what lives where

```text
┌─────────────────────────── one PHP process ────────────────────────────┐
│                                                                        │
│  Producer ──► Queue ──► JobDispatcher ──► WorkerPool                   │
│                 │            │                                         │
│                 │            ├── VisibilityMonitor   (delivery leases) │
│                 │            ├── RetryPolicy                           │
│                 │            ├── DeadLetterQueue                       │
│                 │            ├── JobStorage          (append-only log) │
│                 │            └── MetricsCollector                      │
│                 │                                                      │
│                 └── DelayedJobScheduler              (min-heap)        │
│                                                                        │
│  QueueRuntime drives the loop, and owns the signal handlers            │
│                                                                        │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │  one socket pair per worker
               ┌─────────────────────┼─────────────────────┐
               ▼                     ▼                     ▼
      ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
      │ forked worker    │  │ forked worker    │  │ forked worker    │
      └──────────────────┘  └──────────────────┘  └──────────────────┘
```

Each worker runs `Worker::workerLoop()`: read a frame, run the handler,
write the result, repeat until the socket closes.

Everything that decides anything is in the parent. A worker holds no queue
state, no attempt counters, and no opinion about retries — it receives a
serialized job, runs a callable, and reports success or failure. That is
what makes killing one survivable: there is nothing in it to lose.

The job object a worker sees is a **copy**, rebuilt from JSON. Mutating it
in the worker changes nothing in the parent. What crosses back is a result,
and the ACK is what moves the original.

What the worker holds on the parent side is not the job but the **delivery**
- the lease it was handed the job under. Only the job crosses the wire; the
lease stays behind, because its whole job is to make the eventual answer
attributable to one specific handing-out. See
[invariant 5](#5-only-the-current-delivery-may-acknowledge).

---

## The invariants

Five statements the system is built to keep true. Each one names what would
break it and the test that would catch it.

### 1. A job is never nowhere

At every instant a job is in exactly one of: the ready set, the delayed
heap, the in-flight set, a terminal state, or a DLQ record.

The dangerous moment is the handover, so `dispatch()` orders it
deliberately:

```php
$job->markProcessing();        // 1. it is no longer available
$this->monitor->track($job);   // 2. it is now accounted for as in flight
$worker->assign($job);         // 3. only now does it leave the process
```

Registering with the monitor **before** the write is what closes the window.
If step 3 throws — the worker died between the reaper's check and the write
— the job is already tracked, and the catch puts it straight back on the
queue rather than leaving it to a timeout.

*Would break it:* writing to the socket first and marking PROCESSING after.
A crash between the two would leave a job that a worker is running and the
queue still considers available.

*Held by:* `ChaosTest::testJobSurvivesDispatchToAWorkerKilledWhileIdle`,
`JobDispatcherTest::testDispatchDoesNotPopWhenNoWorkerAvailable`.

### 2. A worker's death is not a job's death

Two detection paths, and they must not overlap:

| worker state | detected by | knows the job? |
|---|---|---|
| BUSY | EOF on its socket, in `collect()` | **yes** — the outcome names it |
| IDLE / STARTING | `waitpid(WNOHANG)` in `maintain()` | no job to know |

`Worker::reap()` therefore checks only IDLE and STARTING. If it reaped busy
workers too, a busy crash would be noticed by whichever path got there
first — and the reaper's path cannot report the job, so the job would be
left to the visibility timeout instead of coming back immediately.

*Held by:* `WorkerTest::testReapIgnoresABusyWorker`,
`ChaosTest::testWorkerCrashDoesNotLoseJob`,
`ChaosTest::testIdleWorkerCrashIsNoticedAndReplaced`.

### 3. An unacknowledged job always comes back

Either the socket tells us (crash while busy) or the deadline does
(everything else, including a worker killed by the shutdown grace period, a
handler that hangs, and a runtime that was SIGKILLed and restarted from the
log).

This is what makes every bounded wait in the system safe. `shutdown($grace)`
can kill a worker mid-job precisely because the job it was holding is still
tracked and still owed an answer.

*Held by:*
`QueueRuntimeTest::testAJobKilledByTheGracePeriodComesBackThroughTheTimeout`,
`VisibilityMonitorTest`, `InMemoryQueueTest::testProcessingJobsReturnToReadyAfterRestart`.

### 4. Only the state machine changes a state

`Job::apply()` is the single gate; there is no other assignment to
`$this->state` after construction. Same for `Worker::apply()`. An illegal
transition is a `LogicException`, not a silent no-op, because a state
machine that tolerates nonsense stops being evidence of anything.

This is also what caught a real bug: a duplicate delivery whose first
answer arrived late tried to complete an already-COMPLETED job and threw.
The fix was to recognise the late answer, not to loosen the table.

*Held by:* `JobTest::testInvalidTransitionIsRejected` and the
`testCannotMark*` family; `WorkerTest::testWorkerCannotAssignTwice`.

### 5. Only the current delivery may acknowledge

A job can be in two workers' hands at once - the deadline passed while a
slow handler was still running - and both will answer. The answer that
counts is the one from the delivery still holding the lease;
`VisibilityMonitor::isCurrent()` is the fence, and a refused answer changes
nothing but the `stale_acks` counter.

*Would break it:* asking whether the JOB is PROCESSING instead. It cannot
work, and it was the bug: by the time the late answer arrives the job is
PROCESSING again, because the worker that replaced the expired delivery put
it there. Measured consequence - the obsolete answer completed a job the
live worker was still running, released the live delivery's lease (leaving a
job in flight with no deadline that could reclaim it), and the live worker's
real answer, a failure, was then discarded as stale.

*Also would break it:* releasing the lease on a stale answer "for tidiness".
The lease under that job's id belongs to the live delivery.

*Held by:*
`ChaosTest::testALateAnswerFromAnExpiredDeliveryIsNotAppliedToTheNewOne`,
`ChaosTest::testOnlyTheLiveDeliveryDecidesTheOutcome`,
`VisibilityMonitorTest::testAStaleDeliveryReleasesNothing`, `DeliveryTest`.

### 6. A failed attempt is not a failed job

`attempts` counts **deliveries**. A NACK with attempts left returns the job
to READY; only exhausting them reaches FAILED, which is the one state a DLQ
record is made from. Nothing else writes to the DLQ, and a job in the DLQ is
FAILED — `DeadLetterQueue::retry()` refuses a record whose job is in any
other state.

*Held by:* `JobDispatcherTest::testFailedJobIsRetriedWhenAttemptsRemain`,
`testFailedJobIsFailedAfterExhaustingAttempts`,
`DeadLetterQueueTest::testRetryRequeuesJobToReady`.

---

## Following one job

The happy path, in the order it actually happens.

```text
Producer::dispatch('send_email', [...])
   │
   ├─ JobFactory::create()          CREATED, attempts 0, metrics: created++
   │
   └─ Queue::push($job, delay: 0)
        │
        ├─ DelayedJobScheduler::holdIfNotDue()
        │     CREATED + no delay  →  markReady($now)  →  returns false
        │
        ├─ ready[] = $job
        └─ JobStorage::store(id, {state: READY, ...})

QueueRuntime::tick()
   │
   ├─ JobDispatcher::dispatchPending()
   │     ├─ WorkerPool::maintain()          reap idle deaths, replace
   │     ├─ WorkerPool::getAvailableWorker()   first IDLE worker
   │     ├─ Queue::pop()                     releases due delayed jobs first
   │     └─ JobDispatcher::dispatch()
   │           ├─ read availableAt      (before markProcessing clears it)
   │           ├─ markProcessing()      PROCESSING, attempts 1
   │           ├─ VisibilityMonitor::track()
   │           │     └─ Delivery: generation 1 (= attempts), worker id,
   │           │        dispatchedAt, deadline = now + timeout
   │           ├─ metrics: queue_wait   now - availableAt
   │           └─ Worker::assign($delivery)
   │                 └─ only the JOB crosses the wire; the worker keeps
   │                    the lease, so its answer is attributable
   │
   └─ JobDispatcher::collect($wait)
         ├─ WorkerPool::poll($wait)
         │     ├─ stream_select over every WORKING worker's socket
         │     └─ Worker::collect(0.0)
         │           ├─ readFrame()   length prefix, then exactly N bytes
         │           └─ apply('finish') BUSY → IDLE
         │
         └─ applyResult($outcome)
               ├─ monitor->isCurrent($delivery)?
               │     no → stale ACK, counted, ignored, lease untouched
               ├─ VisibilityMonitor::release($delivery)
               ├─ metrics: execution   now - $delivery->getDispatchedAt()
               ├─ markCompleted()      COMPLETED
               ├─ JobStorage::store()  so recovery will not restore it
               └─ metrics: completed++, end_to_end
```

Meanwhile, inside the worker:

```text
workerLoop()
   ├─ readFrame()                     blocks until the parent writes
   ├─ Job::fromArray()                a copy, in this process
   ├─ ($handler)($job)                ordinary code, throws or returns
   ├─ JobResult::success() / failure($e)
   ├─ writeAll({success, exceptionClass, exceptionMessage})
   └─ repeat, until the socket closes
```

---

## Following one failure

### A handler throws

```text
handler throws                     in the worker
   └─ JobResult::failure($e)        class + message, not the object
        └─ over the socket
             └─ applyResult()
                  └─ handleFailure()
                       ├─ attempts < maxAttempts?
                       │    yes → RetryPolicy::nextDelay()
                       │          markRetry(now + delay)     READY, future
                       │          Queue::push()  →  the delayed heap
                       │          metrics: retried++
                       │    no  → markFailed()               FAILED
                       │          DeadLetterQueue::add()
                       │          metrics: failed++, dlq++
```

Note where the retry ends up: the **delayed heap**, because a READY job with
a future `availableAt` is exactly what a backoff is. There is no separate
retry queue.

### A worker is killed while busy

```text
kill -9 <worker pid>
   │
   ├─ the socket's peer is gone → our end reaches EOF
   │
   └─ Worker::collect()
        ├─ readFrame() → null
        ├─ apply('die')                     any state → DEAD
        └─ WorkerOutcome($job, result: null)
             │
             └─ applyResult()
                  ├─ result === null
                  ├─ markRetry(now)          READY, available immediately
                  ├─ Queue::push()
                  └─ WorkerPool::replaceDeadWorkers()
                       ├─ fork a replacement in the same slot
                       └─ metrics: worker_crashes++
```

The job does **not** wait for the visibility timeout here — the crash was
detected directly, so it is requeued at once. The timeout is the fallback
for deaths nothing observed.

### A worker is killed while idle

```text
kill -9 <idle worker pid>
   │
   (nothing notices: nothing is selecting on that socket)
   │
   └─ next tick: WorkerPool::maintain()
        ├─ Worker::reap()  →  waitpid(WNOHANG) returns the pid
        ├─ apply('die')                     IDLE → DEAD
        └─ replaceDeadWorkers()             capacity restored
```

And the race that cannot be closed — killed after the reap, before the
write:

```text
Worker::assign()
   ├─ writeAll() fails (EPIPE)
   ├─ apply('die')
   └─ throw WorkerDiedException
        └─ JobDispatcher::dispatch() catches
             ├─ VisibilityMonitor::release($delivery)
             ├─ markRetry(now)  →  Queue::push()
             └─ maintain()      →  replacement forked
```

The attempt that delivery consumed is not refunded. An attempt is a
delivery; the visibility timeout uses the same accounting.

### A handler outlives the visibility timeout

The scenario people do not expect, because nothing is broken:

```text
t=0    dispatch          delivery 1 (generation 1) → worker A
                         PROCESSING, deadline t=30
t=30   requeueExpired()  deadline passed → lease 1 revoked
                         → markRetry() → READY
                         (worker A is still running the handler)
t=31   dispatch          delivery 2 (generation 2) → worker B
                         PROCESSING again, attempts 2
t=45   worker A answers  isCurrent(delivery 1)? the monitor holds
                         generation 2 → NO → stale_acks++, nothing else
t=60   worker B answers  isCurrent(delivery 2)? yes → COMPLETED
```

Two things `applyResult()` must not do with the stale answer, and both were
once done:

- **Apply it.** Reading the job's state instead of the lease accepts it —
  the job really is PROCESSING at t=45, because worker B put it there.
- **Release the lease.** What the monitor holds under that job's id at t=45
  is delivery 2's lease. Releasing it would leave worker B's job in flight
  with no deadline that could ever reclaim it: a harmless duplicate turned
  into a losable job.

*Held by:* `ChaosTest::testAHandlerSlowerThanTheVisibilityTimeoutRunsTwice`
(two runs, two pids, one stale ACK),
`testALateAnswerFromAnExpiredDeliveryIsNotAppliedToTheNewOne` and
`testOnlyTheLiveDeliveryDecidesTheOutcome` (the expired delivery changes
nothing; the live one decides).

### The runtime itself dies

```text
kill -9 <runtime pid>
   │
   ├─ every worker's socket closes → the workers exit on EOF
   └─ nothing is applied, nothing is written
        │
        restart:
        └─ InMemoryQueue::restoreFromStorage()
             ├─ replay the log, last write per job id wins
             ├─ tolerate a torn final line (that write was the crash)
             ├─ PROCESSING  →  markRetry(now)  →  READY
             ├─ READY / DELAYED  →  as they were
             └─ COMPLETED / FAILED  →  not restored
```

Which is the at-least-once bargain at the persistence layer: a job that was
in a worker's hands runs again, side effect and all, so handlers must be
idempotent.

---

## The IPC protocol

One `stream_socket_pair(AF_UNIX, SOCK_STREAM)` per worker, created before
the fork. The parent closes the child's end, the child closes the parent's
end **and every other worker's** — a socket kept open by a third process
never reaches EOF at the far end, which would break crash detection for
every worker forked before it.

Frame format, both directions:

```text
 0        4                        4+N
 ├────────┼─────────────────────────┤
 │ length │ JSON payload            │
 │ uint32 │ N bytes                 │
 │  BE    │                         │
 └────────┴─────────────────────────┘
```

| direction | payload |
|---|---|
| parent → worker | `Job::toArray()` — id, type, payload, state, attempts, maxAttempts, createdAt, availableAt, priority, idempotencyKey |
| worker → parent | `{success: bool, exceptionClass: ?string, exceptionMessage: ?string}` |

`writeAll()` loops until every byte is out; `readExact()` loops until
exactly N bytes are in, and returns `false` on EOF. That `false` is the
crash signal — `readFrame()` turns it into `null`, and `collect()` turns
that into an outcome with a null result.

Three consequences worth stating:

* **Partial delivery is impossible to mistake for a message.** Without the
  length prefix, a stream socket delivering half a payload looks like a
  short message.
* **A frame can be any size.** No line-based escaping, no maximum.
* **This is trusted local IPC.** A parent and its own children; payloads are
  not validated as hostile input. A queue that accepted jobs over a network
  would need to.

The child also resets `SIGTERM` and `SIGINT` to `SIG_DFL` immediately after
the fork. A fork inherits its parent's handlers, and the parent here is the
runtime — without the reset, a SIGTERM to the process group would run a
runtime shutdown inside every worker.

---

## The loop, in detail

`QueueRuntime::tick()`:

```php
$dispatched = $this->dispatcher->dispatchPending();

$wait = $this->waitTime();                     // see below

if ($this->dispatcher->hasWorkInFlight()) {
    $this->dispatcher->collect($wait);         // stream_select does the waiting
} else {
    $this->dispatcher->collect();              // nothing to select on
    $this->sleep($wait);
}

$this->dispatcher->requeueExpired();
```

`waitTime()` is `min($maxWait, nextDeadline - now)`, and `nextDeadline()` is
the earlier of:

* the delayed heap's root — O(1), no scan;
* the earliest visibility deadline — a linear scan, but over a set bounded
  by the pool size, not the queue depth.

The cap (`$maxWait`, 50ms) is what bounds the delay between a SIGTERM
arriving and the shutdown starting. It is also what lets a queue that
receives work from another process be noticed at all.

`WorkerPool::poll($timeout)` returns null immediately when no worker is
working, whatever the timeout says — there is nothing that could arrive, so
waiting would be a deadlock for `null` and a wasted sleep otherwise. That is
why the runtime asks `hasWorkInFlight()` first and does its own sleeping in
the other branch.

### Shutdown, step by step

```text
SIGTERM
   ├─ handler: $stopping = true          and nothing else
   │
   └─ the loop exits after the current tick
        └─ JobDispatcher::shutdown($grace)
             ├─ accepting = false         dispatchPending() now returns 0
             ├─ WorkerPool::drain()
             │    ├─ IDLE workers    → apply('drain') → STOPPING, socket closed
             │    ├─ working workers → apply('drain') → DRAINING, left alone
             │    └─ pool.draining = true   (so nothing gets replaced)
             │
             ├─ while busyCount() > 0 and grace remains:
             │    └─ collect($remaining)   apply what comes back
             │
             └─ WorkerPool::shutdown()
                  └─ per worker: close the socket, wait 50ms for it to
                     exit on EOF, then SIGKILL and reap
```

The grace deadline is wall-clock (`microtime()`), not the injected `Clock`.
Everywhere else time is injected so tests can advance it instantly; here the
question is how long a real forked process gets to finish, and faking time
cannot make it finish sooner.

---

## Ownership of a job

At any instant exactly one thing is responsible for a job. This table is the
whole design in one place.

| job state | where it physically is | who is responsible | what moves it next |
|---|---|---|---|
| CREATED | nowhere yet | the producer | `push()` |
| DELAYED | the delayed heap | `DelayedJobScheduler` | its deadline |
| READY, available | the ready set / a lane | the queue | `pop()` |
| READY, future `availableAt` | the delayed heap | `DelayedJobScheduler` | its deadline |
| PROCESSING | a worker, under a delivery lease | `VisibilityMonitor` | an ACK or NACK **from the current delivery**, or the deadline |
| COMPLETED | nowhere | nobody — it is done | nothing |
| FAILED | a DLQ record, if there is a DLQ | a human | `DeadLetterQueue::retry()` |

Read the PROCESSING row twice. It is the only row where responsibility is
shared with something outside the process - and the only row where the job
can be in two places at once, which is why the lease and not the job id is
what an answer has to match.

---

## Component responsibilities

| component | owns | deliberately does not know |
|---|---|---|
| `Job` | its own lifecycle and the legal transitions | queues, workers, retries |
| `Delivery` | one handing-out: its generation, its worker, its deadline | why it was handed out, what happens next |
| `JobFactory` | creating jobs, counting them | where they go |
| `Producer` | the caller-facing API | job states |
| `Queue` | holding jobs, ready order | why a job failed |
| `DelayedJobScheduler` | what "not yet" means, deadline order | priorities, retries as a concept |
| `LaneSelector` | which priority gets the next turn | what a job is |
| `Worker` | one process, the wire, running the handler | attempts, retries, the queue |
| `WorkerPool` | N processes: which is free, which died | jobs |
| `JobDispatcher` | what an answer means | how a worker talks, how a queue orders |
| `VisibilityMonitor` | which delivery may answer for each job, and when its lease expires | why a job is in flight |
| `RetryPolicy` | how long to wait | whether to retry at all |
| `DeadLetterQueue` | jobs that stopped being retried | when to stop |
| `JobStorage` | a job's last known state, durably | job semantics |
| `MetricsCollector` | counters and latency samples | what any of it means |
| `QueueRuntime` | the loop and the signals | every decision inside a tick |

The two that carry the most weight are `JobDispatcher` — the only thing that
knows what an answer *means* — and `Job` itself, which is the only thing
that can change a state.

---

## Where each mechanism is tested

| mechanism | tests |
|---|---|
| Job state machine | [`tests/Job/JobTest.php`](../tests/Job/JobTest.php) |
| FIFO, size, delayed promotion | [`tests/Queue/InMemoryQueueTest.php`](../tests/Queue/InMemoryQueueTest.php) |
| Delayed ordering, ties, O(1) deadline | [`tests/Scheduler/DelayedJobSchedulerTest.php`](../tests/Scheduler/DelayedJobSchedulerTest.php) |
| Priority, starvation, fair scheduling | [`tests/Queue/PriorityQueueTest.php`](../tests/Queue/PriorityQueueTest.php), [`LaneSelectorTest.php`](../tests/Queue/LaneSelectorTest.php) |
| Producer API | [`tests/Producer/`](../tests/Producer/) |
| Fork, wire, crash detection, reaping | [`tests/Worker/WorkerTest.php`](../tests/Worker/WorkerTest.php) |
| Pool capacity, replacement, draining | [`tests/Worker/WorkerPoolTest.php`](../tests/Worker/WorkerPoolTest.php) |
| ACK/NACK, retries, DLQ, metrics, restart | [`tests/Dispatcher/JobDispatcherTest.php`](../tests/Dispatcher/JobDispatcherTest.php) |
| Visibility timeout, leases, fencing | [`VisibilityMonitorTest.php`](../tests/Timeout/VisibilityMonitorTest.php), [`DeliveryTest.php`](../tests/Delivery/DeliveryTest.php) |
| Dead letter records and manual retry | [`tests/DLQ/DeadLetterQueueTest.php`](../tests/DLQ/DeadLetterQueueTest.php) |
| Append-only log, torn final record | [`tests/Persistence/FileStorageTest.php`](../tests/Persistence/FileStorageTest.php) |
| Deduplication across redelivery, and the crash window it leaves | [`tests/Job/IdempotencyTest.php`](../tests/Job/IdempotencyTest.php) |
| Runtime loop, real SIGTERM, grace period | [`tests/Master/QueueRuntimeTest.php`](../tests/Master/QueueRuntimeTest.php) |
| Crashes, duplicates, slow handlers, restart | [`tests/Chaos/ChaosTest.php`](../tests/Chaos/ChaosTest.php) |
| 1,000 and 10,000 jobs | [`tests/Stress/StressTest.php`](../tests/Stress/StressTest.php) |

Design decisions, the alternatives that were rejected, and the bugs that
changed the code: [DECISIONS.md](DECISIONS.md).
