# PHP Job Queue — the plan it was built from

> **Status: all 16 phases are done.** 234 tests, PHPStan level 8 clean.
>
> This file is kept as the record of what was built, in what order, and what
> each step was for - not as work outstanding. Every `[x]` names a test that
> holds that line; the tests themselves are indexed in
> [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#where-each-mechanism-is-tested).
>
> Where a phase was implemented differently from how it is described below -
> or where building it changed the design - there is an **Implementation**
> section saying so. The reasoning behind those choices, the alternatives
> that were rejected, and the six bugs that building this found are in
> [docs/DECISIONS.md](docs/DECISIONS.md).
>
> Start at [the README](README.md) if you want the finished system rather
> than the plan for it.

---

> Educational implementation of a reliable asynchronous job processing system in PHP.

This project answers one question by building the answer:

> **How does a reliable job queue actually work inside?**

The goal is **not** to build a production replacement for RabbitMQ, Redis, Kafka, Laravel Queue, Sidekiq, or Beanstalkd.

The goal is to build a small, understandable system that can be:

* read as an executable cheat sheet;
* launched locally;
* modified safely;
* broken intentionally;
* debugged;
* used to study queue semantics.

The system should demonstrate how asynchronous work moves through:

```text
Producer
    ↓
Job Queue
    ↓
Scheduler / Dispatcher
    ↓
Worker Pool
    ↓
Job Execution
    ↓
ACK / NACK
    ↓
Completed / Retry / Dead Letter Queue
```

---

# Table of Contents

1. [Project Goals](#1-project-goals)
2. [Core Concepts](#2-core-concepts)
3. [Final Architecture](#3-final-architecture)
4. [Job Lifecycle](#4-job-lifecycle)
5. [Project Structure](#5-project-structure)
6. [Phase 0 — Project Setup](#phase-0--project-setup)
7. [Phase 1 — Basic Job Model](#phase-1--basic-job-model)
8. [Phase 2 — In-Memory Queue](#phase-2--in-memory-queue)
9. [Phase 3 — Producer API](#phase-3--producer-api)
10. [Phase 4 — Worker Pool](#phase-4--worker-pool)
11. [Phase 5 — Job Dispatching](#phase-5--job-dispatching)
12. [Phase 6 — ACK / NACK](#phase-6--ack--nack)
13. [Phase 7 — Retry System](#phase-7--retry-system)
14. [Phase 8 — Delayed Jobs](#phase-8--delayed-jobs)
15. [Phase 9 — Visibility Timeout](#phase-9--visibility-timeout)
16. [Phase 10 — Dead Letter Queue](#phase-10--dead-letter-queue)
17. [Phase 11 — Worker Failure Recovery](#phase-11--worker-failure-recovery)
18. [Phase 12 — Persistence](#phase-12--persistence)
19. [Phase 13 — Priority Queues](#phase-13--priority-queues)
20. [Phase 14 — Metrics](#phase-14--metrics)
21. [Phase 15 — Graceful Shutdown](#phase-15--graceful-shutdown)
22. [Phase 16 — Stress and Chaos Testing](#phase-16--stress-and-chaos-testing)
23. [Important Engineering Questions](#important-engineering-questions)
24. [Suggested Development Order](#suggested-development-order)

---

# 1. Project Goals

The project should demonstrate the fundamental concepts behind reliable asynchronous job processing.

The main topics are:

```text
Job lifecycle
Queueing
Workers
ACK / NACK
Retries
Delayed jobs
Visibility timeout
Worker crashes
At-least-once delivery
Dead Letter Queue
Persistence
Backpressure
Graceful shutdown
Observability
```

The implementation should prioritize:

```text
Clarity
↓
Correctness
↓
Observability
↓
Performance
```

Do not add production complexity unless it helps demonstrate an important concept.

---

# 2. Core Concepts

## Job

A job represents a unit of asynchronous work.

Example:

```text
SendEmail
GenerateReport
ResizeImage
ProcessPayment
```

A job contains:

```text
Job ID
Type
Payload
Attempts
Max attempts
Created time
Available time
```

Example:

```php
Job {
    id: "job-123",

    type: "send_email",

    payload: [
        "email" => "user@example.com",
        "subject" => "Hello"
    ],

    attempts: 0,

    maxAttempts: 3,

    createdAt: ...,

    availableAt: ...
}
```

---

## Producer

A producer creates jobs.

```text
Application
     │
     ▼
Producer
     │
     ▼
Queue
```

Example:

```php
$queue->push(
    new Job(
        type: 'send_email',
        payload: [...]
    )
);
```

---

## Worker

A worker receives jobs and executes them.

```text
Queue
   │
   ▼
Worker
   │
   ▼
Job Handler
```

---

## ACK

ACK means:

> The job completed successfully.

```text
PROCESSING
     │
     ▼
    ACK
     │
     ▼
COMPLETED
```

---

## NACK

NACK means:

> The job failed.

```text
PROCESSING
     │
     ▼
    NACK
     │
     ▼
RETRY
```

---

# 3. Final Architecture

The final system should approximately look like this:

```text
                         ┌──────────────┐
                         │   Producer   │
                         └──────┬───────┘
                                │
                                ▼
                         ┌──────────────┐
                         │     Queue    │
                         │              │
                         │ READY JOBS   │
                         └──────┬───────┘
                                │
                                ▼
                         ┌──────────────┐
                         │  Dispatcher  │
                         └──────┬───────┘
                                │
                    ┌───────────┼───────────┐
                    ▼           ▼           ▼
                 Worker      Worker      Worker
                    │           │           │
                    ▼           ▼           ▼
                 Handler     Handler     Handler
                    │
          ┌─────────┴──────────┐
          │                    │
          ▼                    ▼
         ACK                  NACK
          │                    │
          ▼                    ▼
      COMPLETED              RETRY
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
                  READY                  DLQ
```

Additional systems:

```text
Delayed Job Scheduler
Visibility Timeout Monitor
Worker Supervisor
Persistence Layer
Metrics Collector
```

---

# 4. Job Lifecycle

The central lifecycle should be explicitly modeled.

```text
                 ┌─────────┐
                 │ CREATED │
                 └────┬────┘
                      │
                      ▼
                 ┌─────────┐
                 │  READY  │◄────────────────────┐
                 └────┬────┘                     │
                      │                          │
                      ▼                          │
              ┌──────────────┐                   │
              │ PROCESSING   │                   │
              └──────┬───────┘                   │
                     │                           │
          ┌──────────┴──────────┐                │
          │                     │                │
          ▼                     ▼                │
     ┌──────────┐          ┌──────────┐          │
     │COMPLETED │          │  FAILED  │          │
     └──────────┘          └────┬─────┘          │
                                │                │
                                ▼                │
                         attempts left?          │
                          │             │        │
                         yes            no       │
                          │             │        │
                          ▼             ▼        │
                       RETRY           DLQ       │
                          │                      │
                          └──────────────────────┘
```

For delayed jobs:

```text
CREATED
   ↓
DELAYED
   ↓
READY
```

---

# 5. Project Structure

Suggested final structure:

```text
php-job-queue/
│
├── bin/
│   ├── producer.php
│   ├── worker.php
│   └── queue.php
│
├── config/
│   └── queue.php
│
├── examples/
│   ├── basic-job.php
│   ├── failed-job.php
│   ├── delayed-job.php
│   ├── worker-crash.php
│   └── priority-jobs.php
│
├── src/
│   │
│   ├── Job/
│   │   ├── Job.php
│   │   ├── JobId.php
│   │   ├── JobState.php
│   │   ├── JobPayload.php
│   │   └── JobResult.php
│   │
│   ├── Queue/
│   │   ├── Queue.php
│   │   ├── InMemoryQueue.php
│   │   ├── ReadyQueue.php
│   │   ├── DelayedQueue.php
│   │   └── ProcessingQueue.php
│   │
│   ├── Worker/
│   │   ├── Worker.php
│   │   ├── WorkerPool.php
│   │   ├── WorkerState.php
│   │   └── WorkerSupervisor.php
│   │
│   ├── Dispatcher/
│   │   └── JobDispatcher.php
│   │
│   ├── Handler/
│   │   ├── JobHandler.php
│   │   └── HandlerRegistry.php
│   │
│   ├── Retry/
│   │   ├── RetryPolicy.php
│   │   ├── FixedDelayRetry.php
│   │   └── ExponentialBackoffRetry.php
│   │
│   ├── Timeout/
│   │   └── VisibilityTimeout.php
│   │
│   ├── Scheduler/
│   │   └── DelayedJobScheduler.php
│   │
│   ├── DLQ/
│   │   └── DeadLetterQueue.php
│   │
│   ├── Persistence/
│   │   ├── JobStorage.php
│   │   ├── InMemoryStorage.php
│   │   └── FileStorage.php
│   │
│   ├── Metrics/
│   │   ├── MetricsCollector.php
│   │   └── QueueMetrics.php
│   │
│   └── Master/
│       └── QueueRuntime.php
│
├── tests/
│
├── README.md
├── PLAN.md
├── composer.json
└── Makefile
```

The structure should grow gradually.

Do not create all classes immediately.

Each phase should introduce only the abstractions that are necessary.

> **What it actually became.** Close to the above, with four differences,
> all of them the rule in that last line being followed:
>
> * `Queue/` has no `ReadyQueue`, `DelayedQueue` or `ProcessingQueue`. The
>   ready set is an array inside each queue, delayed jobs are
>   `Scheduler/DelayedJobScheduler`, and the in-flight set belongs to
>   `Timeout/VisibilityMonitor` - which needs the deadlines anyway, so a
>   separate holder of the same jobs would have been a second source of
>   truth.
> * `Handler/JobHandler` and `HandlerRegistry` do not exist. A handler is a
>   `Closure` per pool and a `match` on the job type, which keeps handlers
>   from having to know a queue exists.
> * `Job/JobPayload` does not exist; a payload is an array.
> * `Queue/` gained `LaneSelector` with two implementations, because
>   Phase 13's starvation problem needed an answer and not just a warning.
>
> The current layout is in [the README](README.md#project-layout).

---

# Phase 0 — Project Setup

## Goal

Create a minimal development environment.

### Tasks

* [x] Create repository `php-job-queue`
* [x] Configure Composer
* [x] Configure PSR-4 autoloading
* [x] Add PHPUnit
* [x] Add PHPStan
* [x] Add PHP CS Fixer or another formatter
* [x] Create `Makefile`
* [x] Create initial README
* [x] Create PLAN.md

### Initial commands

```bash
make test
make analyse
make lint
make fix
make run
```

### Success criteria

```text
Tests run
PHPStan runs
Application starts
```

---

# Phase 1 — Basic Job Model

## Goal

Create the fundamental `Job` object.

### Implement

```text
Job
JobId
JobState
```

### Job states

Start simple:

```text
CREATED
READY
PROCESSING
COMPLETED
FAILED
```

### Job fields

```php
$id
$type
$payload

$state

$attempts
$maxAttempts

$createdAt
$availableAt
```

### Important rule

The Job object should represent state clearly.

Avoid:

```php
if ($job->attempts > 3 && ...)
```

scattered everywhere.

Prefer explicit lifecycle methods:

```php
$job->markReady();

$job->markProcessing();

$job->markCompleted();

$job->markFailed();
```

### Tests

* [x] Job receives unique ID
* [x] New job starts as CREATED
* [x] Job can become READY
* [x] Job can become PROCESSING
* [x] Job can become COMPLETED
* [x] Invalid transitions are rejected

---

# Phase 2 — In-Memory Queue

## Goal

Build the smallest possible queue.

### Interface

```php
interface Queue
{
    public function push(Job $job): void;

    public function pop(): ?Job;

    public function size(): int;
}
```

### Implementation

```text
InMemoryQueue
```

Use a simple FIFO structure.

```text
push(A)
push(B)
push(C)

pop()

→ A
```

### Important concept

Start with:

> **FIFO before reliability**

Do not implement retries or persistence yet.

### Tests

* [x] FIFO order
* [x] Empty queue returns null
* [x] Queue size is correct
* [x] Multiple jobs work correctly

---

# Phase 3 — Producer API

## Goal

Create a simple way for applications to submit jobs.

### API

```php
$queue->push(
    new Job(
        type: 'send_email',
        payload: [...]
    )
);
```

Or:

```php
$producer->dispatch(
    'send_email',
    [
        'email' => 'user@example.com'
    ]
);
```

### Implement

```text
Producer
JobFactory
```

### Example

```text
Application
    │
    ▼
Producer
    │
    ▼
JobFactory
    │
    ▼
Queue
```

### Tests

* [x] Producer creates job
* [x] Job type is preserved
* [x] Payload is preserved
* [x] Job enters READY state

---

# Phase 4 — Worker Pool

## Goal

Reuse ideas from `php-worker-pool`.

The system needs workers capable of processing jobs.

Start simple.

```text
Queue
  │
  ▼
Worker
```

Then:

```text
Queue
  │
  ├──── Worker 1
  │
  ├──── Worker 2
  │
  └──── Worker 3
```

### Worker states

```text
STARTING
IDLE
BUSY
STOPPING
DEAD
```

Later:

```text
DRAINING
```

### Worker responsibilities

```text
Receive job
↓
Execute handler
↓
Return result
```

### Do not add yet

```text
Retries
DLQ
Persistence
Autoscaling
```

### Tests

* [x] Worker processes job
* [x] Worker becomes BUSY
* [x] Worker becomes IDLE
* [x] Multiple workers process jobs

---

# Phase 5 — Job Dispatching

## Goal

Separate queue management from worker management.

Introduce:

```text
JobDispatcher
```

Architecture:

```text
Queue
   │
   ▼
Dispatcher
   │
   ▼
Available Worker
```

### Dispatcher loop

Conceptually:

```php
while (true) {
    $worker = $workerPool->getAvailableWorker();

    if ($worker === null) {
        break;
    }

    $job = $queue->pop();

    if ($job === null) {
        break;
    }

    $dispatcher->dispatch(
        $job,
        $worker
    );
}
```

### Important invariant

A job must not disappear between:

```text
Queue
↓
Worker
```

This becomes increasingly important later.

### Tests

* [x] Job goes to available worker
* [x] Busy worker does not receive another job
* [x] Jobs remain queued when no workers exist

---

# Phase 6 — ACK / NACK

## Goal

Introduce reliable completion semantics.

The worker should explicitly report:

```text
ACK
```

or:

```text
NACK
```

### Success

```text
READY
  ↓
PROCESSING
  ↓
ACK
  ↓
COMPLETED
```

### Failure

```text
READY
  ↓
PROCESSING
  ↓
NACK
  ↓
FAILED
```

### Result model

```php
JobResult::success();

JobResult::failure(
    Throwable $exception
);
```

### Important question

What happens if:

```text
Worker completed the job
BUT
dies before ACK reaches Master?
```

The system cannot safely assume success.

This leads directly to:

> **At-least-once delivery**

---

# Phase 7 — Retry System

## Goal

Retry failed jobs.

### Basic flow

```text
PROCESSING
     │
     ▼
    FAIL
     │
     ▼
Attempts < MaxAttempts?
     │
   yes │
       ▼
     RETRY
       │
       ▼
     READY
```

Otherwise:

```text
FAILED
   ↓
DLQ
```

### Retry policy interface

```php
interface RetryPolicy
{
    public function nextDelay(Job $job): int;
}
```

### Implement

#### Fixed delay

```text
1s
1s
1s
```

#### Exponential backoff

```text
1s
2s
4s
8s
```

### Tests

* [x] Failed job retries
* [x] Attempts increase
* [x] Max attempts respected
* [x] Successful retry completes job
* [x] Failed final attempt goes to DLQ

---

# Phase 8 — Delayed Jobs

## Goal

Support jobs scheduled for the future.

Example:

```php
$queue->dispatch(
    job: $job,
    delay: 60
);
```

Lifecycle:

```text
CREATED
   ↓
DELAYED
   ↓
waiting...
   ↓
READY
```

### Architecture

```text
Delayed Queue
      │
      │ time reached
      ▼
Ready Queue
```

### Important implementation concept

Do not repeatedly scan all jobs if possible.

Study:

```text
Priority queue
Min heap
Sorted timestamps
```

Implemented as `Scheduler/DelayedJobScheduler`, a min-heap (`SplHeap`
ordered by `availableAt`, insertion order as the tie-break) shared by
`InMemoryQueue` and `PriorityQueue`. It replaced an array re-sorted on
every push - O(log n) to insert instead of O(n log n), and, the part the
runtime loop needs, `nextDeadline()` in O(1) off the root, so nothing ever
scans the whole set.

A retry under a backoff policy is a READY job with a future `availableAt`,
and goes through the same scheduler. Backoff is not a second waiting
mechanism; it is this one, reused.

### Tests

* [x] Delayed job is not immediately available
* [x] Job becomes available at correct time
* [x] Multiple delayed jobs preserve schedule
* [x] Equal deadlines preserve insertion order
* [x] Next deadline is answered without scanning
* [x] Ready and delayed jobs are counted separately

---

# Phase 9 — Visibility Timeout

## Goal

Solve the problem of workers dying while processing jobs.

Scenario:

```text
READY
  ↓
Worker receives job
  ↓
PROCESSING
  ↓
Worker crashes 💀
```

Without protection:

```text
Job is lost forever
```

### Solution

When a worker receives a job:

```text
READY
  ↓
PROCESSING
  ↓
Invisible for N seconds
```

If ACK arrives:

```text
COMPLETED
```

If no ACK arrives:

```text
Visibility timeout expires
       │
       ▼
Job returns to READY
```

Architecture:

```text
Processing Queue

Job A
└── deadline: 12:00:30
```

Monitor:

```text
now > deadline?
      │
     yes
      │
      ▼
READY
```

### Important concept

This is one reason reliable queues usually provide:

> **At-least-once delivery**

The same job may execute more than once.

### Implementation

`Timeout/VisibilityMonitor` holds a **lease** per in-flight job -
`Delivery/Delivery`: which handing-out of the job this is, the worker
holding it, when it went out, and when its ACK is overdue. Tracking and
expiry are separate: every dispatched job gets a lease, and a null timeout
only means the lease can never go overdue - so the "processing" gauge is
honest either way, the fencing check keeps working, and whether to reclaim
jobs stays a matter of configuration.

Nothing checks the deadlines on a timer of its own.
`QueueRuntime::requeueExpired()` runs once per tick, and `nextDeadline()`
is what lets the tick sleep until there is something to check.

### The lease is why this phase is not just a deadline

The timeout creates a situation the rest of the system has to survive: the
same job in two workers' hands at once. Both will answer, about the same
job id, and only one of them is answering the delivery that is still
current.

Asking whether the JOB is PROCESSING cannot separate them - by the time
the first worker's late answer arrives the job genuinely is PROCESSING,
because the second worker put it there. Measured with that check in place:
the obsolete answer completed a job the live worker was still running,
released the live delivery's lease (leaving a job in flight with no
deadline that could reclaim it), and the live worker's real answer, a
failure, was then discarded as stale.

So `JobDispatcher::applyResult()` fences on
`VisibilityMonitor::isCurrent($delivery)`, and a refused answer changes
nothing but the `stale_acks` counter - in particular it does not release
the lease, which belongs to the live delivery.

The generation needed no new counter: `markProcessing()` already
increments `attempts` exactly once per handing-out, which is why this
project calls an attempt a delivery. See
[docs/DECISIONS.md](docs/DECISIONS.md) entry 15.

### Tests

* [x] A tracked job counts as processing
* [x] A job is not requeued before its deadline
* [x] An expired job returns to READY
* [x] Only expired jobs are requeued
* [x] A released job is no longer processing
* [x] A null timeout tracks without ever expiring
* [x] An expired job is dispatched again end to end
* [x] An expired delivery stops being current once the job is reissued
* [x] A stale delivery releases nothing
* [x] A late answer from an expired delivery is not applied to the new one
* [x] Only the live delivery decides the outcome
* [x] A handler slower than the timeout runs twice, in two workers

---

# Phase 10 — Dead Letter Queue

## Goal

Stop permanently failing jobs from retrying forever.

Flow:

```text
Job
 ↓
Attempt 1 ❌
 ↓
Attempt 2 ❌
 ↓
Attempt 3 ❌
 ↓
DLQ
```

### Dead Letter Queue

```text
Main Queue
     │
     ▼
Retry
     │
     ▼
Dead Letter Queue
```

### Store

```text
Original job
Final exception
Attempts
Failure timestamp
```

### Useful operations

```text
list()
inspect()
retry()
delete()
```

### Tests

* [x] Exhausted job enters DLQ
* [x] Failure information preserved
* [x] DLQ job can be retried manually

---

# Phase 11 — Worker Failure Recovery

## Goal

Handle worker crashes.

Reuse knowledge from:

```text
php-worker-pool
```

Scenario:

```text
Worker
   │
   ▼
Processing Job
   │
   💀 SIGKILL
```

The runtime should:

```text
SIGCHLD
   ↓
Reap worker
   ↓
Mark worker DEAD
   ↓
Restore capacity
```

But the job is still important.

It remains:

```text
PROCESSING
```

until:

```text
ACK
```

or:

```text
Visibility Timeout
```

Then:

```text
PROCESSING
   ↓
timeout
   ↓
READY
```

### Critical invariant

> A worker crash must not permanently lose a job.

### Two crashes, two detection paths

A worker that dies while **busy** is found by the poll loop: its socket
reaches EOF, `Worker::collect()` reports an outcome with a null result, and
that outcome names the job that went down with it. This path has to stay the
one that handles a busy crash, because it is the only one that knows which
job was lost.

A worker that dies while **idle** is invisible to that loop - nothing is
selecting on its socket, because nothing is coming. It used to stay
invisible until the pool handed it a job and the write to a dead socket
threw, which killed the dispatch loop and left the job in limbo.
`WorkerPool::maintain()` closes that gap with a non-blocking `waitpid` per
idle worker, and `Worker::assign()` now reports the race it cannot avoid as
`WorkerDiedException`, which the dispatcher answers by putting the job back.

DRAINING and STOPPING workers are skipped by the reaper on purpose: those
are leaving because we told them to, and counting a deliberate shutdown as
a crash would make the crash counter useless.

### Tests

* [x] Kill worker while idle
* [x] Kill worker while busy
* [x] Worker is replaced
* [x] Job returns to queue
* [x] Job can execute again
* [x] Reaper leaves live and busy workers alone

---

# Phase 12 — Persistence

## Goal

Make jobs survive queue process restart.

Start simple.

Do not immediately build a database.

---

## Option A — Append-only log

Example:

```text
jobs.log

CREATE job-1
PROCESSING job-1
ACK job-1
```

On restart:

```text
Read log
↓
Rebuild state
```

This teaches:

```text
Event log
Write-ahead log
Recovery
Replay
```

---

## Option B — Snapshot

Periodically:

```text
Memory
   ↓
Snapshot
   ↓
jobs.snapshot
```

On restart:

```text
Snapshot
   ↓
Restore queue
```

---

## Recommended approach

Implement:

```text
Append-only log
+
Periodic snapshot
```

Eventually:

```text
Snapshot
+
Recent log
=
Current state
```

This is an excellent educational exercise.

### Tests

* [x] Queue survives restart
* [x] Ready jobs restored
* [x] Delayed jobs restored
* [x] Processing jobs handled correctly after restart
* [x] Completed jobs are not restored - a finished job is not work
* [x] An in-flight job's attempts survive a restart
* [x] A job that keeps killing its worker still runs out of attempts
* [x] An idempotency key survives a restart, so a redelivered job's side
      effect still happens only once

---

# Phase 13 — Priority Queues

## Goal

Support different priorities.

Example:

```text
HIGH
NORMAL
LOW
```

Architecture:

```text
Dispatcher

HIGH queue
    ↓
NORMAL queue
    ↓
LOW queue
```

### Important problem

Naive priority can cause starvation.

Example:

```text
HIGH HIGH HIGH HIGH HIGH...
```

Then:

```text
LOW never executes
```

Study:

```text
Strict priority
Weighted priority
Fair scheduling
Round robin
```

### Example

```text
HIGH   → 5 jobs
NORMAL → 3 jobs
LOW    → 1 job
```

### Implementation

`PriorityQueue` holds one FIFO lane per priority and delegates the policy
question to a `LaneSelector`:

* `StrictPriority` — always the highest non-empty lane. The default, because
  it is what "priority queue" usually means, and because its failure is
  worth being able to watch.
* `WeightedRoundRobin` — the example weights above. Each lane starts a round
  with credits equal to its weight; the highest lane that has both jobs and
  credits is served, and when no lane has both, the round refills. An empty
  lane forfeits its turn instead of blocking, so fairness costs nothing when
  there is nothing to be fair to.

Starvation is not described here, it is demonstrated:
`testStrictPriorityStarvesLowForever` keeps one HIGH job arriving for every
job served - what a busy system looks like - and the LOW job pushed first is
still waiting fifty pops later. The next test fits `WeightedRoundRobin` to
the same scenario and it comes out sixth.

### Tests

* [x] Highest priority is served first
* [x] FIFO within a priority
* [x] Strict priority starves LOW under sustained load
* [x] Weighted round robin gives LOW a turn
* [x] Weighted round robin follows its weights (5 : 3 : 1)
* [x] Empty lanes forfeit their turn rather than blocking
* [x] Delayed jobs return to the lane for their own priority

---

# Phase 14 — Metrics

## Goal

Make the system observable.

Track:

```text
Jobs created
Jobs completed
Jobs failed
Jobs retried
Jobs in DLQ

Queue size

Delayed jobs

Processing jobs

Worker count

Worker crashes
```

### Latency metrics

Separate:

```text
Queue wait time
```

from:

```text
Execution time
```

and:

```text
End-to-end time
```

Example:

```text
Queue wait:    250ms
Execution:      20ms
Total:         270ms
```

Important insight:

> A slow job does not necessarily mean a slow handler.

It may mean:

> The job waited in the queue.

### Implementation

Two objects, because counters and gauges are different things:

* `MetricsCollector` — cumulative counters (`created`, `completed`,
  `failed`, `retried`, `dlq`, `worker_crashes`) and latency samples
  (`queue_wait`, `execution`, `end_to_end`). The names are constants on the
  class, so the vocabulary of the system is readable in one place and a typo
  is a fatal error rather than a counter that silently stays at zero.
* `QueueMetrics` — an immutable gauge reading (`ready`, `delayed`,
  `processing`, `workers`, `busyWorkers`, `deadLettered`), produced by
  `JobDispatcher::observe()` and read from the live objects rather than
  maintained by hand, because a hand-maintained gauge drifts the moment one
  path forgets to adjust it.

The reading that matters most is two of them together: idle workers **and** a
non-empty ready queue at the same time means the dispatcher is not keeping
up, which is a different problem from either number being high alone.

The three latencies are not three views of one number, and end-to-end is not
the sum of the other two: a retried job passes through queue wait and
execution once per attempt, inside one end-to-end span. A deliberate delay
counts towards end-to-end and deliberately does **not** count as queue
wait - the caller did wait, but the queue was not behind.

### Tests

* [x] Counters start at zero and add up
* [x] Jobs created are counted at the factory
* [x] Completed, retried, failed and DLQ counters move
* [x] Worker crashes are counted once, not twice
* [x] Queue wait is measured separately from execution
* [x] A deliberate delay is not counted as queue wait
* [x] `observe()` reports ready, delayed, processing, workers and DLQ

---

# Phase 15 — Graceful Shutdown

## Goal

Stop safely without losing jobs.

Scenario:

```text
SIGTERM
```

The runtime should:

```text
Stop accepting new jobs
       ↓
Stop dispatching new jobs
       ↓
Workers finish current jobs
       ↓
ACK results
       ↓
Exit
```

Workers become:

```text
IDLE
   ↓
DRAINING
```

Busy workers:

```text
BUSY
   ↓
finish job
   ↓
STOPPING
```

After timeout:

```text
SIGTERM
   ↓
grace period
   ↓
SIGKILL
```

Unacknowledged jobs eventually return through visibility timeout.

### Implementation

`Master/QueueRuntime` is the long-running process, and the only thing in
the project that knows how to stop. `JobDispatcher::drain()` runs until the
work runs out, which is right for a script and wrong for a server: a
delayed job due in ten minutes, or a job whose visibility timeout has not
expired yet, is work that does not exist yet. A runtime waits for it.

One tick:

```text
dispatch every free worker
        ↓
apply every answer that arrived
        ↓
reclaim jobs whose ACK went overdue
        ↓
wait
```

The wait is whichever comes first of a worker answering (`stream_select`
wakes on it), the next deadline the runtime owns (`nextDeadline()`, no
scanning), and `maxWait` - so a signal is never more than 50ms from being
acted on. With no worker busy there is nothing to select on, so it is a
plain sleep of the same length.

SIGTERM and SIGINT only set a flag. Nothing is shut down from inside a
signal handler, which is the only way to keep the order of the shutdown
steps knowable. The loop notices on its next pass and leaves through
`JobDispatcher::shutdown($grace)`:

```text
stop accepting  →  stop dispatching  →  busy workers finish
        →  apply their results  →  tear the pool down
```

The grace period bounds the third step. When it runs out, the remaining
workers are killed - `Worker::shutdown()` closes the socket first, which is
how a worker between jobs exits by itself, and reaches for SIGKILL only for
one still inside a handler. The jobs those workers were holding stay
PROCESSING: never acknowledged, and never released from their lease. But
the visibility timeout is not what brings them back - the runtime is on
its way out, so there is no later tick to expire anything on. What carries
them across is the persistence log, restored on the next start. A bounded
shutdown is only as safe as the storage behind it; with none attached, a
job killed by the grace period is lost like any other in-flight job when
the process ends.

Workers reset their inherited signal handlers on fork. Without it, a
SIGTERM to the process group would run a runtime shutdown inside every
worker.

### Tests

* [x] Ticks move jobs through to completion
* [x] A delayed job is picked up once it comes due
* [x] A real SIGTERM lets an in-flight job finish
* [x] The grace period bounds the shutdown
* [x] A job killed by the grace period keeps its lease, unreleased
* [x] A job killed by the grace period is restored to READY on the next
      start, from the log
* [x] Shutdown stops accepting new work
* [x] A stop requested before `run()` is not lost

Runnable by hand - `make run-worker`, then `pkill -TERM -f bin/worker.php`
from another shell.

---

# Phase 16 — Stress and Chaos Testing

## Goal

Prove the system under failure.

---

## Stress tests

```text
1,000 jobs

10,000 jobs

100,000 jobs
```

Measure:

```text
Throughput
Queue latency
Worker utilization
Memory
CPU
```

---

## Chaos tests

Intentionally break the system.

### Kill workers

```bash
kill -9 <pid>
```

Expected:

```text
Worker dies
↓
SIGCHLD
↓
Reap
↓
Replace worker
↓
Unacknowledged job eventually returns
```

---

### Crash queue process

```text
Queue Runtime 💀
```

Restart:

```text
Persistence recovery
↓
Restore state
```

---

### Slow job

```php
sleep(60);
```

Observe:

```text
Visibility timeout
Execution timeout
Worker termination
Retry
```

---

### Always failing job

```text
Attempt 1 ❌
Attempt 2 ❌
Attempt 3 ❌
DLQ
```

---

## What was built

**Stress** — `bin/bench.php <jobs> <workers> <work-microseconds>`, and two
depths kept in the suite (1,000 and 10,000). 100,000 stays in the bench
rather than the tests, because eight seconds and 96MB is a measurement, not
an assertion. Measured on the 8.5-cli image, no-op handlers, 8 workers:

| jobs | drained in | throughput | peak memory | queue wait (avg) | execution (avg) |
|--------:|-----------:|-----------:|------------:|-----------------:|----------------:|
| 1,000 | 0.055s | 18,000/s | 4 MB | 23 ms | 0.08 ms |
| 10,000 | 0.416s | 24,000/s | 12 MB | 215 ms | 0.14 ms |
| 100,000 | 8.368s | 12,000/s | 96 MB | 5,129 ms | 0.30 ms |

And what durability costs, at 10,000 jobs on 8 workers:

| storage | throughput | log |
|---|---:|---|
| none | 23,500/s | - |
| memory | 21,000/s | - |
| file | 18,100/s | 30,000 records, 3.0 per job, 8 MB |

Three records per job - READY, PROCESSING, and the outcome - with the
middle one being what makes the attempt survive a crash. The 8 MB is the
argument for the snapshots this project does not have: the log grows with
every state change and `load()` replays all of it.

Which is Phase 14's insight as a table: at 100,000 jobs the average job took
five seconds and the average handler took a third of a millisecond. The jobs
were not slow. They were queued.

**Chaos** — all four scenarios, as tests:

* [x] Kill a worker while busy → replaced, job returns, job runs again
* [x] Kill a worker while idle → noticed by the reaper, replaced
* [x] Crash the queue process → state rebuilt from the log, PROCESSING jobs
      return to READY
* [x] Slow job → the visibility timeout expires mid-handler and the job runs
      twice, in two different workers, with nothing broken
* [x] Always-failing job → attempts exhausted → DLQ
* [x] Duplicate execution before ACK → the same job charged only once,
      because the handler deduplicates
* [x] A crash between the charge and its record → charged TWICE, which is
      the limit of deduplication without a shared transaction

The slow-job test found two bugs, one after the other. First: the second
worker's answer arrived for a job the first had already completed, and
`markCompleted()` threw out of the dispatch loop. That was fixed by
counting a late answer as `stale_acks` and ignoring it - which was right
for the case the test covered and wrong in general, because it identified
the answer by the job's STATE. The second bug was that check itself; see
Phase 9's implementation notes and
[docs/DECISIONS.md](docs/DECISIONS.md) entry 15.

---

# Important Engineering Questions

The project should explicitly answer these questions.

---

## 1. Why is exactly-once delivery difficult?

Scenario:

```text
Worker executes job
      ↓
Side effect happens
      ↓
Worker crashes before ACK
```

What should the queue do?

```text
Retry?
```

Then the job may execute twice.

```text
Do not retry?
```

Then the job may be lost.

This is why many systems choose:

> **At-least-once delivery**

and require:

> **Idempotent job handlers**

---

## 2. What is the difference between a request timeout and visibility timeout?

```text
Request timeout
```

is about:

> How long someone waits.

```text
Visibility timeout
```

is about:

> How long a job may remain unacknowledged.

These are different problems.

---

## 3. Why does ACK exist?

Without ACK:

```text
Worker received job
```

does not mean:

```text
Job completed successfully
```

ACK creates explicit completion semantics.

---

## 4. Why can jobs execute twice?

Because:

```text
Job executed
↓
ACK lost
↓
Queue assumes failure
↓
Job retries
```

This is expected in an at-least-once system.

---

## 5. Why should handlers be idempotent?

Example:

Bad:

```text
Charge credit card
```

twice.

Better:

```text
Charge order #123
```

with an idempotency key.

---

## 6. What happens when a worker crashes?

The worker may disappear.

The job should not.

That distinction is central to the architecture:

```text
Worker lifecycle
≠
Job lifecycle
```

---

# Suggested Development Order

The recommended order is:

```text
Phase 0
Project setup
    ↓
Phase 1
Job model
    ↓
Phase 2
FIFO queue
    ↓
Phase 3
Producer
    ↓
Phase 4
Worker
    ↓
Phase 5
Dispatcher
    ↓
Phase 6
ACK / NACK
    ↓
Phase 7
Retries
    ↓
Phase 8
Delayed jobs
    ↓
Phase 9
Visibility timeout
    ↓
Phase 10
Dead Letter Queue
    ↓
Phase 11
Worker crash recovery
    ↓
Phase 12
Persistence
    ↓
Phase 13
Priority queues
    ↓
Phase 14
Metrics
    ↓
Phase 15
Graceful shutdown
    ↓
Phase 16
Stress and chaos testing
```

---

# Minimal Milestone

The first important milestone should be:

```text
Producer
   ↓
Queue
   ↓
Worker
   ↓
Handler
   ↓
ACK
   ↓
Completed
```

Only after this works should additional reliability features be added.

---

# Final Goal

The final repository should make this entire lifecycle understandable:

```text
Producer
    │
    ▼
CREATE JOB
    │
    ▼
READY
    │
    ▼
WAITING IN QUEUE
    │
    ▼
DISPATCH
    │
    ▼
PROCESSING
    │
    ├───────────────┐
    │               │
    ▼               ▼
   ACK             NACK
    │               │
    ▼               ▼
COMPLETED         RETRY
                    │
                    ▼
                 DELAYED
                    │
                    ▼
                  READY
                    │
                    ▼
              max attempts?
                    │
                   no
                    │
                    ▼
                   DLQ
```

The repository should be useful as an executable answer to:

> **How does reliable asynchronous job processing work inside?**

The ideal workflow should be:

```text
Read
↓
Run
↓
Experiment
↓
Break something
↓
Observe
↓
Understand
```

---

# Non-Goals

This project should NOT try to become:

```text
RabbitMQ
Kafka
Redis
Temporal
Laravel Queue
Sidekiq
```

Avoid adding complexity just to imitate production systems.

The most important property of the project is:

> **Every important mechanism should be understandable.**

A smaller system that clearly demonstrates:

```text
ACK
Retry
Visibility timeout
Worker crash
DLQ
Persistence
```

is more valuable for learning than a production-scale system with hundreds of abstractions.

---

# Repository Philosophy

```text
Small enough to understand.
Simple enough to modify.
Real enough to fail.
Reliable enough to study.
```

This is an educational engineering playground.

Not a black box.

The user should be able to:

```text
Open the code
↓
Follow one job
↓
Watch it move through the system
↓
Kill a worker
↓
See what happens
↓
Understand why the architecture exists
```

That is the definition of success for this project.
