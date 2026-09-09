# PHP Job Queue

> An educational implementation of a background job queue in PHP.

`php-job-queue` is a small educational project for exploring how background job processing systems work internally.

The goal is not to replace production-ready solutions such as:

* RabbitMQ;
* Redis-based queues;
* Symfony Messenger;
* Laravel Queues;
* Temporal.

The goal is to build a system that is:

* small enough to understand;
* simple enough to modify;
* realistic enough to demonstrate real engineering problems;
* easy to run locally and experiment with.

This repository is designed as an **executable mental model of a background job processing system**.

## Status

All 16 phases from [PLAN.md](PLAN.md) are implemented:

`Job model` · `FIFO / delayed / priority queues` · `Producer` · `Worker pool` · `Dispatcher` · `ACK / NACK` · `Retry (fixed & exponential backoff)` · `Visibility timeout` · `Dead Letter Queue` · `Worker crash recovery` · `Persistence (append-only log)` · `Metrics` · `Graceful shutdown` · `Stress & chaos tests`

```bash
make install        # composer install
make test           # PHPUnit
make analyse        # PHPStan (level 8)
make docker-run     # demo via Docker
```

Run the demo directly:

```bash
docker compose exec php php bin/run.php
```

---

## Why?

Modern applications often need to perform work asynchronously.

For example:

```text
HTTP Request

↓

Send Email

↓

Generate PDF

↓

Resize Image

↓

Call External API

↓

Process Data

↓

Return Response
```

Doing all this work inside an HTTP request can make the application slow.

Instead:

```text
HTTP Request

↓

Create Job

↓

Push Job To Queue

↓

Return Response
```

Later:

```text
Worker

↓

Take Job

↓

Process Job
```

This project explores what actually happens between:

```php
dispatch($job);
```

and:

```text
Job Completed
```

---

# Core Idea

A Job Queue separates:

```text
Producing Work
```

from:

```text
Processing Work
```

The Producer creates work:

```text
Application

↓

Create Job

↓

Queue
```

Workers process work:

```text
Queue

↓

Worker

↓

Job Handler

↓

Result
```

The Queue connects them.

> **Producers create work. Workers process work. The Queue connects them.**

---

# Architecture

```text
                    PRODUCERS
                        │
                        │ dispatch()
                        ▼
                 ┌──────────────┐
                 │    QUEUE     │
                 └──────┬───────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
       Worker A      Worker B      Worker C
          │             │             │
          ▼             ▼             ▼
        Job 1         Job 2         Job 3
          │             │             │
          └─────────────┼─────────────┘
                        ▼
                    HANDLERS
```

The system consists of several main components:

```text
┌─────────────────────────────────────┐
│ Job Creation                        │
│                                     │
│ Job                                 │
│ Dispatcher                          │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ Queue                               │
│                                     │
│ Pending Jobs                        │
│ Reserved Jobs                       │
│ Delayed Jobs                        │
│ Failed Jobs                         │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ Worker                              │
│                                     │
│ Reserve Job                         │
│ Process Job                         │
│ ACK / Retry                         │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ Processing                          │
│                                     │
│ Job Processor                       │
│ Handler Resolver                    │
│ Job Handler                         │
└─────────────────────────────────────┘
```

For a detailed explanation of the internal design, see:

* [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

---

# Job Lifecycle

Every Job moves through a lifecycle.

```text
CREATED
    │
    ▼
PENDING
    │
    │ Worker reserves Job
    ▼
RESERVED
    │
    ▼
RUNNING
    │
    ├─────────────────┐
    │                 │
    ▼                 ▼
SUCCESS             FAILURE
    │                 │
    ▼                 ▼
COMPLETED          RETRYING
                      │
                      ▼
                   DELAYED
                      │
                      ▼
                    PENDING
```

If retries are exhausted:

```text
RUNNING

↓

FAILURE

↓

FAILED
```

Simplified:

```text
PENDING
   │
   ▼
RESERVED
   │
   ▼
RUNNING
   │
   ├── Success ──► COMPLETED
   │
   └── Failure ──► RETRY / FAILED
```

---

# Project Structure

```text
php-job-queue/
│
├── bin/
│   ├── worker.php
│   └── console.php
│
├── src/
│   │
│   ├── Job/
│   │   ├── Job.php
│   │   ├── JobId.php
│   │   ├── JobPayload.php
│   │   └── JobState.php
│   │
│   ├── Queue/
│   │   ├── Queue.php
│   │   ├── InMemoryQueue.php
│   │   ├── JobReservation.php
│   │   └── QueueStats.php
│   │
│   ├── Worker/
│   │   ├── Worker.php
│   │   ├── WorkerState.php
│   │   ├── WorkerRunner.php
│   │   └── WorkerConfig.php
│   │
│   ├── Processing/
│   │   ├── JobProcessor.php
│   │   ├── HandlerResolver.php
│   │   └── JobHandler.php
│   │
│   ├── Retry/
│   │   ├── RetryPolicy.php
│   │   └── BackoffStrategy.php
│   │
│   ├── Delay/
│   │   └── DelayedJobManager.php
│   │
│   ├── Failure/
│   │   └── FailedJobRepository.php
│   │
│   └── Support/
│       └── Clock.php
│
├── tests/
├── examples/
├── benchmarks/
│
├── docs/
│   └── ARCHITECTURE.md
│
├── README.md
├── PLAN.md
├── composer.json
└── phpunit.xml
```

---

# Basic Example

Create a Job:

```php
$job = new SendEmailJob(
    to: 'user@example.com',
    subject: 'Hello',
    message: 'Welcome!',
);
```

Dispatch it:

```php
$queue->dispatch($job);
```

The Job enters the Queue:

```text
SendEmailJob

↓

PENDING
```

A Worker later processes it:

```text
Worker

↓

Reserve Job

↓

Execute Handler

↓

ACK
```

---

# Queue

The Queue stores Jobs waiting for processing.

Conceptually:

```text
Queue

├── Pending
│
├── Reserved
│
├── Delayed
│
└── Failed
```

The Queue is responsible for:

```text
Push Job

Reserve Job

Acknowledge Job

Release Job

Move Delayed Job

Handle Expired Reservations
```

---

# Workers

Workers continuously look for work.

Conceptually:

```text
while (running) {

    job = queue.reserve();

    if no job:
        wait;

    process(job);
}
```

A Worker:

```text
START

↓

WAIT FOR JOB

↓

RESERVE JOB

↓

PROCESS JOB

↓

ACK / RETRY

↓

WAIT FOR NEXT JOB
```

---

# Reservation

A Worker should not simply:

```text
GET JOB

↓

PROCESS JOB

↓

DELETE JOB
```

Imagine:

```text
Worker

↓

Gets Job

↓

Starts Processing

↓

CRASH 💀
```

If the Job was already removed:

```text
Job Lost Forever 💀
```

Instead:

```text
PENDING

↓

RESERVED

↓

PROCESSING

↓

ACK

↓

COMPLETED
```

The Job remains recoverable until successful processing is acknowledged.

---

# Visibility Timeout

A reservation should not live forever.

Example:

```text
Worker A

↓

Reserve Job

↓

Visibility Timeout = 30 seconds
```

The Job becomes temporarily unavailable to other Workers.

```text
PENDING

↓

RESERVED

↓

Invisible To Other Workers
```

If processing succeeds:

```text
ACK

↓

COMPLETED
```

If the Worker crashes:

```text
Worker Crash

↓

No ACK

↓

Visibility Timeout Expires

↓

Job Returns To PENDING
```

Architecture:

```text
PENDING
    │
    ▼
RESERVED
    │
    ├── ACK ─────────────► COMPLETED
    │
    └── Timeout ─────────► PENDING
```

This creates a basic:

> **At-least-once delivery model.**

---

# At-Least-Once Delivery

The Queue attempts to ensure that a Job is eventually processed.

However:

```text
Worker

↓

Executes Job

↓

Action Completed

↓

Worker Crashes Before ACK
```

The Queue may retry the Job.

```text
Same Job

↓

Executed Again
```

Therefore:

> **A Job may execute more than once.**

---

# Idempotency

Because duplicate execution is possible, Job handlers should consider idempotency.

Example:

```text
Process Order #123

↓

Check Current State

↓

Already Processed?

├── Yes → Skip
│
└── No → Process
```

The goal is:

```text
Same Job Executed Multiple Times

↓

Same Logical Result
```

---

# Retries

A failed attempt does not necessarily mean a permanently failed Job.

Example:

```text
External API unavailable
```

The system can retry:

```text
Attempt 1

↓

Failure

↓

Retry
```

A Retry Policy can define:

```text
Maximum Attempts

Retry Delay

Backoff Strategy
```

Example:

```text
Attempt 1

↓

Failure

↓

Wait 1 second

↓

Attempt 2

↓

Failure

↓

Wait 5 seconds

↓

Attempt 3
```

---

# Retry Lifecycle

```text
RUNNING
    │
    ▼
FAILED ATTEMPT
    │
    ▼
Attempts Remaining?
    │
    ├── Yes
    │
    │     ▼
    │   DELAYED
    │
    │     ▼
    │   PENDING
    │
    └── No
          │
          ▼
        FAILED
```

> **Failed Attempt ≠ Failed Job**

---

# Delayed Jobs

Not every Job should execute immediately.

```text
DISPATCH

↓

DELAYED

↓

Scheduled Time Reached

↓

PENDING

↓

Worker
```

Example:

```php
$queue->dispatch(
    new SendEmailJob(...),
    delay: 3600,
);
```

---

# Failed Jobs

When retries are exhausted:

```text
RUNNING

↓

FAILURE

↓

FAILED
```

A Failed Job should remain inspectable.

Example metadata:

```text
Job ID

Job Type

Payload

Attempts

Exception

Failed At
```

This allows:

```text
Inspect

↓

Understand Failure

↓

Fix Problem

↓

Retry Manually
```

---

# Concurrency

Multiple Workers can process Jobs simultaneously.

```text
                    Queue
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
     Worker A      Worker B      Worker C
        │             │             │
        ▼             ▼             ▼
      Job 1         Job 2         Job 3
```

The important invariant:

```text
One Job

↓

Must not be reserved

by

Two Workers
```

Therefore:

```text
reserve()

=

Take Next Available Job

+

Mark As Reserved

atomically
```

---

# Graceful Shutdown

Workers should support graceful shutdown.

```text
RUNNING
    │
    │ Shutdown Requested
    ▼
DRAINING
    │
    │ Current Job Finished
    ▼
STOPPED
```

While draining:

```text
❌ Do not reserve new Jobs

✅ Finish current Job
```

This is important during:

* deployments;
* restarts;
* process shutdown.

---

# Failure Scenarios

One of the main purposes of this project is to experiment with failure.

## Worker Crash

```text
PENDING

↓

RESERVED

↓

Worker Crash 💀

↓

Visibility Timeout

↓

PENDING Again
```

## Job Failure

```text
RUNNING

↓

Exception

↓

Retry Available?

├── Yes → Retry
│
└── No → Failed
```

## Duplicate Execution

```text
Job Executed

↓

Worker Crashes Before ACK

↓

Job Returns To Queue

↓

Job Executed Again
```

## Graceful Shutdown

```text
Worker Processing Job

↓

SIGTERM

↓

DRAINING

↓

Finish Current Job

↓

STOPPED
```

---

# Experiments

The repository is designed to be executed and modified.

Recommended experiments:

### Worker Crash

Kill a Worker while it processes a Job:

```text
Job Processing

↓

kill -9 Worker

↓

Start New Worker

↓

Observe Job Recovery
```

### Retry

Create a Handler that intentionally fails:

```php
throw new RuntimeException('Temporary failure');
```

Observe:

```text
Attempt

↓

Failure

↓

Retry

↓

Backoff

↓

Success / Failed
```

### Delayed Job

Dispatch:

```text
Job

↓

Delay 10 Seconds

↓

Observe

↓

PENDING

↓

Worker Processes Job
```

### Multiple Workers

Start:

```text
Worker A

Worker B

Worker C
```

Dispatch many Jobs and observe how they are distributed.

### Graceful Shutdown

Start processing a long-running Job.

Send:

```text
SIGTERM
```

Observe:

```text
Worker

↓

DRAINING

↓

Current Job Completes

↓

STOPPED
```

---

# Roadmap

The project is implemented incrementally.

## Phase 1 — Basic Job Queue

```text
Job

Queue

Dispatch

Reserve

ACK
```

## Phase 2 — Worker

```text
Worker Loop

Job Processor

Handler Resolver
```

## Phase 3 — Job Lifecycle

```text
PENDING

RESERVED

RUNNING

COMPLETED
```

## Phase 4 — Failures

```text
Exceptions

Retries

Retry Policy

Backoff
```

## Phase 5 — Delayed Jobs

```text
Delayed Queue

Scheduled Execution

Move To Pending
```

## Phase 6 — Worker Crash Recovery

```text
Reservation

Visibility Timeout

Expired Reservation Recovery
```

## Phase 7 — Graceful Shutdown

```text
RUNNING

↓

DRAINING

↓

STOPPED
```

## Phase 8 — Failed Jobs

```text
Failed Job Storage

Inspection

Manual Retry
```

## Phase 9 — Concurrency

```text
Multiple Workers

Atomic Reservation

Job Distribution
```

---

# Related Projects

This project is part of a collection of educational PHP backend and concurrency projects.

## [PHP Concurrency](https://github.com/Researcher86/php-concurrency)

A practical collection of experiments exploring concurrency in PHP.

It focuses on concepts such as:

* processes;
* `pcntl_fork`;
* IPC;
* Fibers;
* event loops;
* asynchronous execution;
* concurrency patterns.

It provides the foundation for understanding how multiple units of work can execute concurrently.

## [PHP Worker Pool](https://github.com/Researcher86/php-worker-pool)

An educational implementation of a reusable Worker Pool.

It explores:

* Worker lifecycle;
* process management;
* Worker states;
* task execution;
* Worker recycling;
* graceful shutdown;
* `DRAINING`.

`php-job-queue` builds on these concepts and focuses on the next layer:

```text
php-concurrency
        ↓
Concurrency fundamentals
        ↓
php-worker-pool
        ↓
Worker lifecycle and process management
        ↓
php-job-queue
        ↓
Reliable background job processing
```

---

# What This Project Is Not

This project is intentionally **not** trying to become:

* RabbitMQ;
* Apache Kafka;
* Redis;
* Symfony Messenger;
* Laravel Queue;
* Laravel Horizon;
* Temporal.

Production systems contain many additional concerns:

```text
Distributed Storage

Network Failures

Replication

High Availability

Authentication

Authorization

Metrics

Tracing

Monitoring

Dead Letter Queues

Priority Queues

Persistence

Horizontal Scaling
```

Those are valuable problems.

But they can hide the fundamental ideas.

This project focuses on the core model first.

---

# Mental Model

The entire system can be reduced to:

```text
                    PRODUCER
                        │
                        ▼
                   CREATE JOB
                        │
                        ▼
                     QUEUE
                        │
                        ▼
                    PENDING
                        │
                        ▼
                   RESERVATION
                        │
                        ▼
                     WORKER
                        │
                        ▼
                    HANDLER
                        │
             ┌──────────┴──────────┐
             ▼                     ▼
          SUCCESS                FAILURE
             │                     │
             ▼                     ▼
            ACK                 RETRY
             │                     │
             ▼                     ▼
         COMPLETED          DELAYED / FAILED
```

---

# Final Principle

The purpose of this project is not to build the most feature-rich queue.

The purpose is to build an:

> **Executable mental model of a background job processing system.**

The project should remain:

> **Small enough to understand.**

> **Real enough to experiment with.**

> **Simple enough to modify.**

> **Complex enough to demonstrate real engineering problems.**

## License

MIT
