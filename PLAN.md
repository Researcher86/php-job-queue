# PHP Job Queue

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

---

# Phase 0 — Project Setup

## Goal

Create a minimal development environment.

### Tasks

* [ ] Create repository `php-job-queue`
* [ ] Configure Composer
* [ ] Configure PSR-4 autoloading
* [ ] Add PHPUnit
* [ ] Add PHPStan
* [ ] Add PHP CS Fixer or another formatter
* [ ] Create `Makefile`
* [ ] Create initial README
* [ ] Create PLAN.md

### Initial commands

```bash
make test
make analyse
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

* [ ] Job receives unique ID
* [ ] New job starts as CREATED
* [ ] Job can become READY
* [ ] Job can become PROCESSING
* [ ] Job can become COMPLETED
* [ ] Invalid transitions are rejected

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

* [ ] FIFO order
* [ ] Empty queue returns null
* [ ] Queue size is correct
* [ ] Multiple jobs work correctly

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

* [ ] Producer creates job
* [ ] Job type is preserved
* [ ] Payload is preserved
* [ ] Job enters READY state

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

* [ ] Worker processes job
* [ ] Worker becomes BUSY
* [ ] Worker becomes IDLE
* [ ] Multiple workers process jobs

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

* [ ] Job goes to available worker
* [ ] Busy worker does not receive another job
* [ ] Jobs remain queued when no workers exist

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

* [ ] Failed job retries
* [ ] Attempts increase
* [ ] Max attempts respected
* [ ] Successful retry completes job
* [ ] Failed final attempt goes to DLQ

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

### Tests

* [ ] Delayed job is not immediately available
* [ ] Job becomes available at correct time
* [ ] Multiple delayed jobs preserve schedule

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

* [ ] Exhausted job enters DLQ
* [ ] Failure information preserved
* [ ] DLQ job can be retried manually

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

### Tests

* [ ] Kill worker while idle
* [ ] Kill worker while busy
* [ ] Worker is replaced
* [ ] Job returns to queue
* [ ] Job can execute again

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

* [ ] Queue survives restart
* [ ] Ready jobs restored
* [ ] Delayed jobs restored
* [ ] Processing jobs handled correctly after restart

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
