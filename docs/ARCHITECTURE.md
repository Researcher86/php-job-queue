# PHP Job Queue Architecture

> Architecture and internal data flow of an educational background job queue written in PHP.

This document describes how the main components of `php-job-queue` work together.

The goal is not to document every implementation detail.

The goal is to provide a **mental model of a background job processing system**.

If you forget how something works, this document should help answer:

```text
What component is responsible for this?

Where does a job come from?

Where does it go next?

What happens when processing fails?

What happens when a Worker crashes?

How does the job lifecycle work?
```

---

# Table of Contents

* [Architecture Overview](#architecture-overview)
* [Core Concepts](#core-concepts)
* [Core Components](#core-components)
* [Job Lifecycle](#job-lifecycle)
* [Producer Flow](#producer-flow)
* [Worker Lifecycle](#worker-lifecycle)
* [Job Processing Flow](#job-processing-flow)
* [Queue Architecture](#queue-architecture)
* [Reservation and Visibility](#reservation-and-visibility)
* [Retries](#retries)
* [Delayed Jobs](#delayed-jobs)
* [Failed Jobs](#failed-jobs)
* [Worker Crashes](#worker-crashes)
* [Concurrency](#concurrency)
* [Graceful Shutdown](#graceful-shutdown)
* [Component Responsibilities](#component-responsibilities)
* [Important Invariants](#important-invariants)
* [Mental Model](#mental-model)

---

# Architecture Overview

A job queue separates:

```text
Producing Work

from

Processing Work
```

Instead of:

```text
HTTP Request

↓

Send Email

↓

Generate Report

↓

Process Image

↓

Return Response
```

the application can do:

```text
HTTP Request

↓

Create Job

↓

Queue

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

The complete architecture:

```text
                        Producers
                            │
                            │ dispatch()
                            ▼
                  ┌───────────────────┐
                  │   Job Dispatcher  │
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │       Queue       │
                  └─────────┬─────────┘
                            │
                 ┌──────────┼──────────┐
                 │          │          │
                 ▼          ▼          ▼
              Worker A   Worker B   Worker C
                 │          │          │
                 └──────────┼──────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │   Job Processor   │
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │    Job Handler    │
                  └───────────────────┘
```

The central idea is:

> **Producers create work. Workers process work. The Queue connects them.**

---

# Core Concepts

The system is built around several simple concepts.

```text
Job
 ↓
A unit of work
```

Example:

```text
SendEmailJob
```

---

```text
Queue
 ↓
Stores jobs waiting for processing
```

---

```text
Worker
 ↓
Takes jobs and executes them
```

---

```text
Handler
 ↓
Contains the actual business logic
```

Example:

```text
SendEmailHandler
```

---

```text
Retry
 ↓
Attempts to process a failed job again
```

---

```text
Failed Job
 ↓
A job that can no longer be processed automatically
```

---

# Core Components

The architecture can be divided into several layers.

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
│ Queue Storage                       │
│                                     │
│ Pending Jobs                        │
│ Reserved Jobs                       │
│ Delayed Jobs                        │
│ Failed Jobs                         │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ Job Processing                      │
│                                     │
│ Worker                              │
│ Job Processor                       │
│ Handler Resolver                    │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ Execution                           │
│                                     │
│ Job Handler                         │
│ Application Logic                   │
└─────────────────────────────────────┘
```

The separation is important.

For example:

```text
Job Handler
```

should not know about:

```text
Queue Storage
```

And:

```text
Queue
```

should not know how:

```text
SendEmailJob
```

actually sends an email.

The desired flow is:

```text
Producer

↓

Queue

↓

Worker

↓

Handler
```

---

# Job Lifecycle

A job has a lifecycle.

The basic lifecycle:

```text
CREATED
    │
    ▼
PENDING
    │
    │ Worker takes job
    ▼
RESERVED
    │
    │ Processing
    ▼
RUNNING
    │
    ├───────────────┐
    │               │
    │ Success       │ Failure
    ▼               ▼
COMPLETED         RETRYING
                    │
                    │
                    ├──────► PENDING
                    │
                    │ retries exhausted
                    ▼
                  FAILED
```

A simplified view:

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

# Producer Flow

A producer creates a job.

Example:

```php
$queue->dispatch(
    new SendEmailJob(
        to: 'user@example.com',
        subject: 'Hello',
    ),
);
```

Internally:

```text
Application

↓

Job

↓

Job Dispatcher

↓

Serialize Job

↓

Queue Storage

↓

PENDING
```

Detailed flow:

```text
1. Application creates Job
        │
        ▼
2. Job Dispatcher receives Job
        │
        ▼
3. Job is validated
        │
        ▼
4. Job metadata is created
        │
        ▼
5. Job is stored in Queue
        │
        ▼
6. Job becomes PENDING
```

The producer does not wait for processing.

That is the main purpose of a background queue.

---

# Queue Architecture

The Queue stores jobs according to their current state.

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

Example:

```text
                 ┌───────────────┐
                 │    PENDING    │
                 └───────┬───────┘
                         │
                         │ Worker takes job
                         ▼
                 ┌───────────────┐
                 │   RESERVED    │
                 └───────┬───────┘
                         │
                         ▼
                 ┌───────────────┐
                 │    RUNNING    │
                 └───────┬───────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
       ┌────────────┐         ┌────────────┐
       │ COMPLETED  │         │   FAILED   │
       └────────────┘         └────────────┘
```

The Queue is responsible for:

```text
Push Job

↓

Reserve Job

↓

Acknowledge Job

↓

Release Job

↓

Move Delayed Job

↓

Move Failed Job
```

---

# Worker Lifecycle

A Worker continuously looks for work.

Conceptually:

```text
STARTING
    │
    ▼
IDLE
    │
    │ Job available
    ▼
RESERVING
    │
    ▼
PROCESSING
    │
    ├───────────────┐
    │               │
    ▼               ▼
SUCCESS          FAILURE
    │               │
    ▼               ▼
IDLE           RETRYING
                    │
                    ▼
                   IDLE
```

The Worker loop:

```text
while (running) {

    job = queue.reserve()

    if no job:
        wait

    process(job)
}
```

---

# Job Processing Flow

When a Worker receives a Job:

```text
Worker

↓

Reserve Job

↓

Job Processor

↓

Resolve Handler

↓

Execute Handler

↓

Success / Failure
```

Detailed lifecycle:

```text
1. Worker asks Queue for Job
        │
        ▼
2. Queue reserves Job
        │
        ▼
3. Worker receives Job
        │
        ▼
4. Job Processor resolves Handler
        │
        ▼
5. Handler executes
        │
        ▼
6. Result is evaluated
        │
        ├── Success
        │
        └── Failure
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

That creates problems.

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

What happens to the job?

If the job was already removed:

```text
Job

↓

Lost 💀
```

Instead:

```text
PENDING

↓

RESERVED

↓

PROCESSING

↓

ACKNOWLEDGED
```

The job is only considered complete after successful processing.

---

# Visibility Timeout

Reservation can have a timeout.

Example:

```text
Worker A

↓

Reserve Job

↓

Visibility Timeout = 30 seconds
```

The job becomes invisible to other Workers:

```text
PENDING

↓

RESERVED

↓

Invisible for 30 seconds
```

If the Worker successfully finishes:

```text
ACK

↓

COMPLETED
```

But if:

```text
Worker crashes
```

and no acknowledgement arrives:

```text
Visibility Timeout expires

↓

Job returns to Queue
```

Architecture:

```text
PENDING
    │
    ▼
RESERVED
    │
    │ ACK received
    ├──────────────► COMPLETED
    │
    │ Timeout
    ▼
PENDING
```

This prevents jobs from being permanently lost after a Worker crash.

---

# Job Processing

A Job represents:

```text
What should be done
```

Example:

```text
SendEmailJob
```

A Handler represents:

```text
How it should be done
```

Example:

```text
SendEmailHandler
```

Architecture:

```text
SendEmailJob

↓

Handler Resolver

↓

SendEmailHandler

↓

execute()
```

This separation allows the queue infrastructure to remain independent from business logic.

---

# Success Flow

A successful job:

```text
PENDING
    │
    ▼
RESERVED
    │
    ▼
RUNNING
    │
    ▼
SUCCESS
    │
    ▼
ACKNOWLEDGED
    │
    ▼
COMPLETED
```

Example:

```text
Worker

↓

SendEmailJob

↓

Email Sent

↓

ACK

↓

Job Removed From Active Queue
```

---

# Failure Flow

A failed job:

```text
PENDING
    │
    ▼
RESERVED
    │
    ▼
RUNNING
    │
    ▼
EXCEPTION
    │
    ▼
Retry Available?
    │
    ├── Yes
    │      │
    │      ▼
    │   RETRYING
    │      │
    │      ▼
    │   PENDING
    │
    └── No
           │
           ▼
         FAILED
```

---

# Retries

A job may fail temporarily.

Example:

```text
Database unavailable
```

or:

```text
External API timeout
```

Immediately marking the job as permanently failed is often unnecessary.

Instead:

```text
Attempt 1

↓

Failure

↓

Retry
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

This introduces:

```text
Retry Policy
```

A retry policy can define:

```text
Maximum Attempts

Retry Delay

Backoff Strategy
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

The important distinction:

```text
Failed Attempt

≠

Failed Job
```

A job can fail multiple times before becoming permanently failed.

---

# Delayed Jobs

Some jobs should not execute immediately.

Example:

```text
Send email in 1 hour
```

The Job is placed into:

```text
DELAYED
```

instead of:

```text
PENDING
```

Architecture:

```text
DISPATCH

↓

DELAYED

↓

Scheduled Time Reached?

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

The system periodically checks:

```text
Delayed Jobs

↓

Ready Jobs

↓

Move To Pending Queue
```

---

# Failed Jobs

When retries are exhausted:

```text
Job

↓

FAILED
```

The failed job should remain inspectable.

Example information:

```text
Job ID

Job Type

Payload

Attempts

Exception

Failed At
```

Conceptually:

```text
Failed Jobs

├── Job #101
│
├── Job #102
│
└── Job #103
```

This allows experiments with:

```text
Inspect Failed Job

↓

Fix Problem

↓

Retry Manually
```

---

# Worker Crash

One of the most important scenarios.

Imagine:

```text
Worker A

↓

Reserve Job

↓

Start Processing

↓

CRASH 💀
```

The system must avoid:

```text
Job Lost Forever
```

The lifecycle should be:

```text
PENDING

↓

RESERVED

↓

Worker Crash

↓

Visibility Timeout

↓

Reservation Expires

↓

PENDING Again
```

This creates a basic:

> **At-least-once delivery model.**

Important consequence:

```text
A Job Can Execute More Than Once
```

Therefore:

> Job handlers should ideally be idempotent.

---

# Idempotency

Because Workers can crash:

```text
Process Job

↓

Action Completed

↓

Worker crashes before ACK
```

The Queue may retry the job.

```text
Same Job

↓

Executed Again
```

Therefore:

```text
Job

↓

Possible Duplicate Execution
```

Example:

```text
Charge Credit Card

❌ Dangerous without protection
```

Better:

```text
Process Order #123

↓

Check if already processed

↓

Execute only once logically
```

The Queue can provide:

```text
At-Least-Once Delivery
```

But exactly-once processing is a much harder distributed systems problem.

---

# Concurrency

Multiple Workers can process jobs simultaneously.

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

The important rule:

```text
One Job

↓

Must not be reserved

by

Two Workers
```

Therefore reservation must be atomic.

Conceptually:

```text
reserve()

=

Take Next Available Job

+

Mark Reserved

atomically
```

---

# Graceful Shutdown

A Worker should not simply die immediately.

Bad:

```text
PROCESSING

↓

SIGTERM

↓

Worker Dies 💀
```

Better:

```text
PROCESSING

↓

SIGTERM

↓

DRAINING

↓

Finish Current Job

↓

STOPPED
```

Worker lifecycle:

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
Worker

❌ Does Not Reserve New Jobs

✅ Finishes Current Job
```

This prevents unnecessary retries during deployment or shutdown.

---

# Component Responsibilities

| Component                  | Responsibility                   |
| -------------------------- | -------------------------------- |
| `Job`                      | Represents a unit of work        |
| `JobDispatcher`            | Sends Jobs to the Queue          |
| `Queue`                    | Stores and manages Job lifecycle |
| `Worker`                   | Reserves and processes Jobs      |
| `JobProcessor`             | Coordinates Job execution        |
| `HandlerResolver`          | Finds the correct Handler        |
| `JobHandler`               | Executes business logic          |
| `RetryPolicy`              | Decides retry behavior           |
| `DelayedJobManager`        | Moves ready Jobs to Pending      |
| `FailedJobRepository`      | Stores failed Jobs               |
| `ReservationManager`       | Manages Job reservation          |
| `VisibilityTimeoutManager` | Returns abandoned Jobs           |
| `WorkerManager`            | Manages Worker lifecycle         |

---

# Important Invariants

The system should maintain several important rules.

---

## A Job Must Not Be Lost

Bad:

```text
Take Job

↓

Remove Job

↓

Worker Crash

↓

Job Lost 💀
```

Correct:

```text
Pending

↓

Reserved

↓

ACK

↓

Completed
```

---

## Reservation Must Be Atomic

Never allow:

```text
Worker A

↓

Sees Job
```

and simultaneously:

```text
Worker B

↓

Sees Same Job
```

The operation must guarantee:

```text
One Job

↓

One Reservation
```

---

## ACK Only After Success

The Job should only be acknowledged after successful execution.

Bad:

```text
ACK

↓

Process Job

↓

Failure 💀
```

Correct:

```text
Process Job

↓

Success

↓

ACK
```

---

## Workers Must Respect DRAINING

When shutdown begins:

```text
DRAINING
```

means:

```text
❌ Do not reserve new Jobs

✅ Finish current Job

↓

STOP
```

---

## Failed Attempt Is Not Failed Job

Never confuse:

```text
Attempt Failed
```

with:

```text
Job Permanently Failed
```

A job can move:

```text
RUNNING

↓

FAILED ATTEMPT

↓

RETRYING

↓

RUNNING
```

multiple times.

---

## Jobs May Execute More Than Once

Because of crashes and visibility timeouts:

```text
At-Least-Once Delivery
```

means:

```text
Duplicate Execution Is Possible
```

Handlers should consider idempotency.

---

# Typical Job Example

Let's trace:

```text
SendEmailJob
```

through the complete system.

```text
1. Application creates Job
        │
        ▼
2. Job Dispatcher receives Job
        │
        ▼
3. Job is stored in Pending Queue
        │
        ▼
4. Worker requests Job
        │
        ▼
5. Queue atomically reserves Job
        │
        ▼
6. Job becomes RESERVED
        │
        ▼
7. Worker starts processing
        │
        ▼
8. Handler is resolved
        │
        ▼
9. SendEmailHandler executes
        │
        ▼
10. Email is sent
        │
        ▼
11. Worker ACKs Job
        │
        ▼
12. Job becomes COMPLETED
```

---

# Failure Example

Now imagine:

```text
SendEmailJob
```

fails.

```text
1. Worker reserves Job
        │
        ▼
2. Worker executes Handler
        │
        ▼
3. SMTP Server unavailable
        │
        ▼
4. Exception
        │
        ▼
5. Retry Policy checks attempts
        │
        ▼
6. Attempts remaining?
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

---

# Mental Model

The complete system can be reduced to:

```text
                    PRODUCERS
                        │
                        ▼
                   DISPATCH JOB
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

# Project Architecture

The repository can be organized like this:

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
│
├── examples/
│
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

# Final Architecture Principle

The project should remain an:

> **Executable mental model of a background job processing system.**

You should be able to open the repository and quickly answer:

```text
How does a Job enter the system?
```

→ `JobDispatcher`

```text
Where does a Job wait?
```

→ `Queue`

```text
How does a Worker get a Job?
```

→ `reserve()`

```text
What happens during processing?
```

→ `JobProcessor`

```text
How is business logic executed?
```

→ `JobHandler`

```text
What happens when processing fails?
```

→ `RetryPolicy`

```text
What happens when retries are exhausted?
```

→ `FailedJobRepository`

```text
What happens when a Worker crashes?
```

→ `Reservation + Visibility Timeout`

```text
How does graceful shutdown work?
```

→ `WorkerState::DRAINING`

---

# Summary

```text
PHP Job Queue

=

Jobs
+
Queue
+
Workers
+
Reservation
+
Handlers
+
Retries
+
Delayed Jobs
+
Visibility Timeout
+
Failed Jobs
+
Graceful Shutdown
```

The project is not intended to replace a production queue system.

Its purpose is simpler:

> **Build a small system that exposes the important engineering ideas behind background job processing.**

> **Small enough to understand.**

> **Real enough to experiment with.**

> **Simple enough to modify.**

> **Complex enough to demonstrate real failure scenarios.**
