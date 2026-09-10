# PHP Job Queue

> A reliable asynchronous job queue, built small enough to read.

This repository answers one question by building the answer:

> **How does a reliable job queue actually work inside?**

It is not a replacement for RabbitMQ, Redis, Kafka, Symfony Messenger,
Laravel Queue or Sidekiq. It is the mechanism those things contain, with
nothing else in the way: 38 classes, no dependencies beyond a UUID library,
and every reliability feature written out where you can put a breakpoint in
it.

The workflow it is built for:

```text
Read  →  Run  →  Experiment  →  Break something  →  Observe  →  Understand
```

---

## Status

All 16 phases of [PLAN.md](PLAN.md) are done, 230 tests, PHPStan level 8
clean.

`Job state machine` · `FIFO / delayed / priority queues` · `Producer` ·
`Forked worker pool` · `Dispatcher` · `ACK / NACK` · `Delivery leases and
fencing` · `Fixed and exponential retry` · `Visibility timeout` · `Dead
letter queue` · `Worker crash recovery` · `Append-only persistence` · `Fair
scheduling` · `Metrics` · `Graceful shutdown` · `Stress and chaos tests`

```bash
make build              # docker image
make docker-install     # composer install
make docker-test        # the whole suite
make docker-analyse     # phpstan, level 8
make docker-lint        # php-cs-fixer, check only

make docker-run                              # the lifecycle once
make docker-run-worker                       # the long-running process
make docker-example EXAMPLE=worker-crash     # one mechanism at a time
make docker-bench ARGS="10000 8"             # throughput and latency
```

Every target has a non-Docker twin (`make test`, `make run`, …) if you have
PHP 8.5 with `pcntl` and `posix` locally.

---

## Table of contents

1. [The shape of it](#the-shape-of-it)
2. [A job's life](#a-jobs-life)
3. [Producing work](#producing-work)
4. [Queues](#queues)
5. [Workers are processes](#workers-are-processes)
6. [The dispatcher](#the-dispatcher)
7. [ACK and NACK](#ack-and-nack)
8. [Retries](#retries)
9. [Delayed jobs](#delayed-jobs)
10. [Visibility timeout](#visibility-timeout)
11. [Deliveries and fencing](#deliveries-and-fencing)
12. [Dead letter queue](#dead-letter-queue)
13. [Worker crashes](#worker-crashes)
14. [Persistence](#persistence)
15. [Priority and starvation](#priority-and-starvation)
16. [Metrics](#metrics)
17. [Graceful shutdown](#graceful-shutdown)
18. [The runtime](#the-runtime)
19. [At-least-once, and what it costs you](#at-least-once-and-what-it-costs-you)
20. [Experiments](#experiments)
21. [Failure matrix](#failure-matrix)
22. [Performance](#performance)
23. [Project layout](#project-layout)
24. [Engineering questions, answered](#engineering-questions-answered)
25. [What this is not](#what-this-is-not)
26. [Related projects](#related-projects)

---

## The shape of it

```text
        ┌──────────────┐
        │   Producer   │   creates jobs
        └───────┬──────┘
                ▼
        ┌──────────────┐        ┌───────────────────────┐
        │    Queue     │◄───────│  DelayedJobScheduler  │  min-heap of
        │  ready jobs  │        │ (delays and backoff)  │  deadlines
        └───────┬──────┘        └───────────────────────┘
                ▼
        ┌──────────────┐        ┌───────────────────────┐
        │  Dispatcher  │───────►│   VisibilityMonitor   │  in-flight jobs
        └───────┬──────┘        │  and their deadlines  │  and their ACK
                │               └───────────────────────┘  deadlines
    ┌───────────┼───────────┐
    ▼           ▼           ▼
  Worker      Worker      Worker     each a forked process
    │           │           │
    ▼           ▼           ▼
  Handler     Handler     Handler
    │
    ├──── ACK ─────► COMPLETED
    │
    └──── NACK ────► attempts left?  ──yes──►  READY (after backoff)
                            │
                            no
                            ▼
                     ┌──────────────┐
                     │ Dead Letter  │
                     └──────────────┘
```

Everything above the workers runs in one process, driven by
[`QueueRuntime`](src/Master/QueueRuntime.php). Everything below the
dispatcher is a separate OS process, which is the point: a handler that
segfaults, blocks forever, or is `kill -9`'d takes its process down and
nothing else — and what happens to its **job** then is the interesting
question.

---

## A job's life

```text
  ┌─────────┐   markDelayed()   ┌─────────┐
  │ CREATED │──────────────────►│ DELAYED │
  └────┬────┘                   └────┬────┘
       │ markReady()                 │ markReady()
       │                             │ (deadline passed)
       ▼                             │
  ┌─────────┐◄───────────────────────┘
  │  READY  │◄──────────────┐◄──────────────┐
  └────┬────┘               │               │
       │ markProcessing()   │ markRetry()   │ markRequeued()
       ▼                    │               │
  ┌──────────────┐          │               │
  │  PROCESSING  │──────────┘               │
  └───┬──────┬───┘   NACK with attempts     │
      │      │       left, or an expired    │
      │      │       visibility deadline    │
      │      │ markFailed()                 │
      │      ▼                              │
      │  ┌────────┐                         │
      │  │ FAILED │─────────────────────────┘
      │  └────────┘   a human retries a DLQ record
      │ markCompleted()
      ▼
  ┌───────────┐
  │ COMPLETED │
  └───────────┘
```

The states are a real state machine, not a label: every transition goes
through one table in [`Job`](src/Job/Job.php), and anything not in it
throws. There is no path by which a COMPLETED job quietly becomes
PROCESSING again.

Two arrows into READY look like one and are not:

| | from | when |
|---|---|---|
| `markRetry()` | PROCESSING | a NACK with attempts left, or an expired visibility timeout |
| `markRequeued()` | FAILED | a human retried a dead-letter record |

**Attempts count deliveries, not runs.** `markProcessing()` is what
increments the counter, so a job handed to a worker that died before
starting has still used an attempt. That is the honest accounting for an
at-least-once system, and what real queues count too.

---

## Producing work

```php
$producer = new Producer($queue, new JobFactory($clock, $metrics));

$job = $producer->dispatch(
    'send_email',
    ['to' => 'user@example.com', 'subject' => 'Hello'],
);
```

That is the whole caller-facing API. Application code never touches a `Job`,
a state, or a queue implementation — the caller's side of an asynchronous
system should be as small as the synchronous call it replaced.

Everything optional is a named argument:

```php
$producer->dispatch(
    'charge_card',
    ['order_id' => 123, 'amount' => 49.90],
    maxAttempts: 5,
    delay: 60,                              // seconds
    priority: JobPriority::HIGH,
    idempotencyKey: 'charge-order-123',
);
```

A job's **type** is a string, and handlers are plain callables — there is no
handler registry, no container, no class resolution. One `Closure` per pool
receives every job:

```php
$pool = new WorkerPool(4, static function (Job $job): void {
    match ($job->getType()) {
        'send_email' => sendEmail($job->getPayload()),
        'charge_card' => charge($job->getPayload()),
    };
});
```

A handler that returns normally succeeded. A handler that throws failed.
Nothing else is required of it, and it never learns that a queue exists.

---

## Queues

```php
interface Queue
{
    public function push(Job $job, int $delay = 0): void;
    public function pop(?float $now = null): ?Job;

    public function size(): int;          // everything waiting
    public function readySize(): int;     // waiting and available
    public function delayedSize(): int;   // waiting on a deadline
    public function nextDeadline(): ?float;
}
```

`readySize()` and `delayedSize()` are both in the contract on purpose: "40
jobs waiting" means something very different when 39 of them are not due for
an hour.

Two implementations:

* [`InMemoryQueue`](src/Queue/InMemoryQueue.php) — one FIFO. Also the only
  one that can restore itself from storage.
* [`PriorityQueue`](src/Queue/PriorityQueue.php) — one FIFO per priority,
  with the choice between them delegated to a
  [`LaneSelector`](src/Queue/LaneSelector.php). See
  [priority and starvation](#priority-and-starvation).

Both hold delayed jobs in the same
[`DelayedJobScheduler`](src/Scheduler/DelayedJobScheduler.php), and both
make the same push decision through it — a queue's job is holding jobs, not
deciding what "not yet" means.

`pop()` returning null does **not** mean the queue is empty. It means
nothing is available; everything in it may be delayed.

---

## Workers are processes

[`Worker`](src/Worker/Worker.php) forks, and both sides of the fork live in
that one class: the parent uses `assign()` / `collect()`, the child runs
`workerLoop()` and exits from it. Keeping the pair together means the wire
format is stated once.

```text
STARTING ──spawn──► IDLE ──assign──► BUSY ──finish──► IDLE
                      │                │
                drain │          drain │
                      ▼                ▼
                  STOPPING         DRAINING ──finish──► STOPPING
                                    (still working here,
                                     but no new work)

any state ──die──► DEAD   (terminal)
```

Two things are tracked separately, and the distinction matters:

* the **state** says whether new work may be dispatched here;
* `isWorking()` says whether work is happening right now.

A worker drained mid-job is DRAINING — not available, still working — which
is exactly what a graceful shutdown has to wait for. And DRAINING + finish
is STOPPING rather than IDLE, so a shutdown cannot hand it one more job on
the way out.

### The wire

A Unix socket pair carrying length-prefixed JSON:

```text
[ 4-byte big-endian length ][ JSON payload ]
```

Parent → worker is the serialized job; worker → parent is
`{success, exceptionClass, exceptionMessage}`. Writes loop until every byte
is out, reads loop until exactly N bytes are in.

The length prefix is not ceremony. A stream socket is free to deliver half a
message, and "read what is available and hope" is how an IPC layer starts
silently truncating payloads under load. With the prefix, a short read is
either completed or an **EOF** — and an EOF mid-frame means the process
died, which is the single most important thing a worker handle has to be
able to tell.

The exception itself cannot cross the socket — it may not be serializable,
and its stack refers to a process that no longer exists — so its class and
message do, and a stand-in is rebuilt on the other side. What survives is
what the DLQ needs to show a human.

---

## The dispatcher

[`JobDispatcher`](src/Dispatcher/JobDispatcher.php) is the middle of the
system, and everything it collaborates with is optional:

```php
$dispatcher = new JobDispatcher(
    $queue,
    $pool,
    $retryPolicy,               // null → retry immediately
    $clock,
    visibilityTimeout: 30,      // null → in-flight jobs never expire
    dlq: $dlq,                  // null → exhausted jobs just end FAILED
    storage: $storage,          // null → nothing survives a restart
    metrics: $metrics,          // null → nothing is counted
);
```

They are nullable rather than required because each one is a mechanism you
can read on its own — and see the system work without.

It has two halves, deliberately apart:

```php
$dispatcher->dispatchPending();   // fill every free worker, don't wait
$dispatcher->collect($timeout);   // apply every answer that arrived
```

Keeping them separate is what lets a pool of N run N jobs at once. A loop
that waits for each job before dispatching the next has a pool of one,
whatever its size says. `drain()` and `QueueRuntime` are both these two
halves in a loop; they differ only in when they decide to stop.

### The invariant

A job must never be *between* the queue and a worker. So `dispatch()` marks
it PROCESSING and registers it with the visibility monitor **before** it
writes to the socket — there is no instant at which the job is neither
queued nor accounted for as in flight.

---

## ACK and NACK

What the dispatcher does with an answer:

| answer | meaning | result |
|---|---|---|
| success | ACK | COMPLETED |
| failure, attempts left | NACK | READY, after the retry delay |
| failure, attempts gone | NACK | FAILED, plus a DLQ record |
| **null result** | the worker died holding the job | READY at once, worker replaced |
| **not the current delivery** | a late answer for a lease that has been revoked | counted as a stale ACK, ignored |

The last two rows are why a worker reports a
[`WorkerOutcome`](src/Worker/WorkerOutcome.php) rather than a
[`JobResult`](src/Job/JobResult.php). "The job failed" and "the worker
vanished" need different handling, and a `JobResult` has no way to say the
second one.

The last row is the visibility timeout showing through, and getting it right
takes more than it looks — see [deliveries and fencing](#deliveries-and-fencing).

---

## Retries

```php
interface RetryPolicy
{
    public function nextDelay(Job $job): int;
}
```

* [`FixedDelayRetry`](src/Retry/FixedDelayRetry.php) — 1s, 1s, 1s. Right
  when failures are independent, wrong when they are not: a dependency that
  is down stays down, and a hundred jobs retrying every second are a hundred
  requests a second against something already failing.
* [`ExponentialBackoffRetry`](src/Retry/ExponentialBackoffRetry.php) — 1s,
  2s, 4s, 8s. The retries stop being part of the problem.

No jitter, deliberately. It would be right in production — a thousand jobs
that failed together retry together, and the pattern repeats at every
doubling — and it would make the delays in an example unreadable.

```text
attempt 1 ❌  →  wait 1s  →  attempt 2 ❌  →  wait 2s  →  attempt 3 ❌  →  DLQ
```

---

## Delayed jobs

```php
$producer->dispatch('reminder', [], delay: 3600);
```

```text
CREATED  →  DELAYED  →  (deadline passes)  →  READY  →  …
```

[`DelayedJobScheduler`](src/Scheduler/DelayedJobScheduler.php) holds them in
a min-heap ordered by deadline. Two different things end up in it, and it
matters that they are the same mechanism:

* a DELAYED job, dispatched with a delay;
* a **READY job with a future `availableAt`** — which is what a retry under
  backoff is.

A retry *is* a delayed job. Backoff is not a second waiting mechanism bolted
onto the first.

The heap replaced an array re-sorted on every push. O(log n) to insert
instead of O(n log n) — but the property that actually matters is
`nextDeadline()` in O(1) off the root, because that is what lets the runtime
loop **sleep until** the next deadline instead of waking up to ask whether
it has arrived.

---

## Visibility timeout

The problem:

```text
READY  →  worker takes the job  →  PROCESSING  →  worker dies 💀
```

Without protection the job is simply gone: not queued, not completed, not
failed. So a dispatched job gets a deadline, and if no ACK arrives before
it, the job returns to READY and goes to someone else.

```php
new JobDispatcher($queue, $pool, visibilityTimeout: 30);
// …
$dispatcher->requeueExpired();   // QueueRuntime calls this every tick
```

In [`VisibilityMonitor`](src/Timeout/VisibilityMonitor.php), tracking and
expiry are separate: every dispatched job is tracked, and a null timeout
only means nothing can become overdue. So the "processing" gauge is honest
either way, and whether to reclaim jobs stays a matter of configuration.

**It is not an execution timeout.** The deadline says how long a job may
stay unacknowledged, not how long a handler may run. A handler slower than
the timeout gets its job handed to a second worker while the first is still
working on it — that is the timeout being too short, and it is a
[real, tested scenario](tests/Chaos/ChaosTest.php).

Which leads straight into the next section, because two workers holding the
same job at once is only survivable if their answers can be told apart.

---

## Deliveries and fencing

A job can be in two workers' hands at the same time. The deadline passes
while a slow handler is still running, the job goes back to READY, a second
worker takes it — and both workers will eventually answer about the same job
id.

So which one is allowed to acknowledge it?

The obvious answer is "whichever reports while the job is PROCESSING", and
it is wrong. By the time the first worker's late answer arrives, the job
really is PROCESSING — the *second* worker put it there. Checking the state
cannot separate them; it is the same object.

That was a real bug here, and the damage was not a harmless duplicate:

```text
after two deliveries: state=PROCESSING  attempts=2  leased=true
after A's late ACK:   state=COMPLETED   leased=false  completed=1
after B's real NACK:  state=COMPLETED   stale_acks=1
```

The obsolete answer completed a job the live worker was still running. It
released the *live* delivery's lease, leaving a job in a worker's hands with
no deadline that could ever bring it back. And the live worker's real
answer — a failure — was discarded as stale. The roles were inverted: the
delivery nobody was waiting for decided the job's fate.

The fix is to give a delivery an identity.
[`Delivery`](src/Delivery/Delivery.php) is the lease: the job, **which
handing-out this is**, the worker holding it, when it went out, and when its
ACK is overdue.

```text
                    JOB #8f2a
                        │
            ┌───────────┴───────────┐
            ▼                       ▼
       Delivery 1              Delivery 2
        worker A                worker B
         expired                 current     ◄── holds the lease
            │                       │
        late ACK                   ACK
            │                       │
            ▼                       ▼
     STALE, ignored             COMPLETED
     lease untouched
```

[`VisibilityMonitor`](src/Timeout/VisibilityMonitor.php) holds the delivery
currently entitled to answer for each job, and the dispatcher fences on it:

```php
if (!$this->monitor->isCurrent($delivery)) {
    $this->metrics?->increment(MetricsCollector::STALE_ACKS);

    return;   // and pointedly WITHOUT releasing the lease
}
```

That comment is load-bearing. The lease under this job's id belongs to the
live delivery, and releasing it on a stale answer is what turned a duplicate
into a losable job.

The generation needed no new counter. `markProcessing()` already increments
`attempts` exactly once per handing-out, which is why this project says an
attempt counts a **delivery** rather than a run — the fencing token was
already in the model and was not being used as one.

This is the shape production systems use: a lease token, a fencing token, an
SQS receipt handle. The right to acknowledge belongs to a specific claim,
not to whoever holds the id.

---

## Dead letter queue

Without one, a job that can never succeed has two endings and both are bad:
retry forever, burning workers on work that will not complete, or drop it
and lose the fact that it existed.

```php
$dlq->all();                  // every record
$dlq->find($jobId);           // one
$dlq->retry($jobId);          // FAILED → READY, and hand it back
$dlq->delete($jobId);         // give up on it
```

A record keeps the job, the exception that finished it, the attempt count
and the time — enough to answer "what broke, and how often" without going to
the logs. The attempt count is **copied** rather than read on demand,
because `retry()` puts the job back into circulation and the record has to
keep saying "this failed three times" after the fourth attempt starts.

---

## Worker crashes

A worker can die in two states, and they are detected by two different
paths. Both are tested; the second one was a genuine hole.

**Busy.** The socket reaches EOF, `collect()` reports an outcome with a null
result, and that outcome names the job that went down with it. This path has
to stay the one that handles a busy crash, because it is the only one that
knows which job was lost.

**Idle.** Nothing is selecting on an idle worker's socket, because nothing
is coming. The death used to stay invisible until the pool handed it a job —
and then the write to a dead socket threw out of the dispatch loop, taking
the runtime down and leaving the job marked PROCESSING with no worker behind
it. [`WorkerPool::maintain()`](src/Worker/WorkerPool.php) closes that with a
non-blocking `waitpid` per idle worker, and the race it cannot close (killed
after the check, before the write) is reported as `WorkerDiedException`,
which the dispatcher answers by putting the job straight back.

```text
worker dies
     ↓
reap (or EOF)
     ↓
mark DEAD  →  fork a replacement  →  capacity restored
     ↓
the job it held goes back to READY  →  runs again
```

DRAINING and STOPPING workers are skipped by the reaper on purpose: those
are leaving because we told them to, and counting a deliberate shutdown as a
crash would make the crash counter useless.

> A worker crash must not permanently lose a job. Worker lifecycle is not
> job lifecycle.

---

## Persistence

```php
$storage = new FileStorage('/var/lib/jobs.log');
// …later, in a new process:
$queue = InMemoryQueue::restoreFromStorage($storage, $clock);
```

An append-only log, one JSON object per line, keyed by job id, last write
wins. Append rather than rewrite because appending is the operation that is
hard to half-finish: a crash mid-write leaves a truncated last line and
every complete line before it intact, where rewriting a whole file can lose
all of it.

`load()` therefore **tolerates a malformed final line** — that is a write
torn by the crash we are recovering from, and refusing to start because the
last record is half-written would make the log useless exactly when it is
needed. A malformed line anywhere else is corruption, and throws.

On restore:

| last known state | what happens |
|---|---|
| READY, DELAYED | restored as it was |
| PROCESSING | back to READY — nobody is left to ACK it, so it runs again, **with its attempt count intact** |
| COMPLETED, FAILED | not restored; a finished job is not work |

The PROCESSING row is the at-least-once bargain restated at the persistence
layer, and it only works because the dispatcher writes the record when the
job goes out, not only when it comes back. That write is what makes
`attempts` durable — without it the log's last word on an in-flight job is
the READY record from `push()`, so a crash hands the job its whole
allowance again and a job that reliably kills its worker loops across
restarts forever instead of reaching the DLQ. The price is one more append
per dispatch, which is what durable attempt counting costs in a log-only
design.

The other cost is bounded replay: the log grows with every state change and
`load()` replays all of it. A real system pairs this with periodic
snapshots; this one does not, and says so.

---

## Priority and starvation

```php
$queue = new PriorityQueue($clock, new StrictPriority());        // default
$queue = new PriorityQueue($clock, new WeightedRoundRobin());    // 5:3:1
```

[`StrictPriority`](src/Queue/StrictPriority.php) always takes the highest
non-empty lane. It is the default because that is what "priority queue"
means to most people — and because its failure is worth being able to watch
rather than hide:

```text
strict priority                H H H H H H H H H H H H H H H H H H H H
                               low-cleanup NEVER RAN

weighted round robin (5:3:1)   H H H H H N N N L H H H H H N N N L H H
                               low-cleanup ran
```

That is real output from `make docker-example EXAMPLE=priority-jobs`, over
the same load: a steady stream of HIGH work, one job arriving for every job
served, plus a backlog of NORMAL and LOW that arrived first. Under strict
priority the backlog never moves. Not "moves late" — never. Nothing in that
policy ever gives it a turn.

[`WeightedRoundRobin`](src/Queue/WeightedRoundRobin.php) gives each lane
credits for the round and skips a lane that has run out. HIGH hitting its
limit is the pressure release that lets the others move. An empty lane
forfeits its turn instead of blocking, so fairness costs nothing when there
is nothing to be fair to.

---

## Metrics

Two objects, because counters and gauges are different things.

[`MetricsCollector`](src/Metrics/MetricsCollector.php) — cumulative:

```text
created   completed   failed   retried   dlq   worker_crashes   stale_acks
```

and three latencies, which are the point:

| | measures | what a high value tells you |
|---|---|---|
| `queue_wait` | available → dispatched | add workers |
| `execution` | how long the handler ran | fix the handler |
| `end_to_end` | created → completed | includes deliberate delays and every failed attempt |

> A slow job does not necessarily mean a slow handler. It may mean the job
> waited in the queue.

They are not three views of one number, and end-to-end is not the sum of the
other two: a retried job passes through queue wait and execution once per
attempt, inside one end-to-end span. A deliberate delay counts towards
end-to-end and deliberately does **not** count as queue wait — the caller
did wait, but the queue was not behind.

[`QueueMetrics`](src/Metrics/QueueMetrics.php) — an immutable gauge reading:

```php
$dispatcher->observe()->toArray();
// ['ready' => 12, 'delayed' => 3, 'processing' => 4, 'workers' => 4,
//  'busy_workers' => 4, 'idle_workers' => 0, 'dead_lettered' => 1]
```

A counter is cumulative; a gauge is already stale when you read it. One bag
for both is how "queue size: 40,000" ends up on a dashboard as a number that
never goes down. It is read from the live objects rather than maintained by
hand, because the one path that forgets to adjust a hand-kept gauge is
invisible.

The reading that matters most is two of them together: **idle workers and a
non-empty ready queue at the same time** means the dispatcher is not keeping
up, which is a different problem from either number being high alone.

---

## Graceful shutdown

```text
SIGTERM
   ↓
stop accepting   →   stop dispatching   →   busy workers finish
   ↓
apply their results   →   tear the pool down
```

```php
$dispatcher->shutdown(grace: 5.0);   // null → wait as long as it takes
```

The grace period bounds the third step. When it runs out the remaining
workers are killed — [`Worker::shutdown()`](src/Worker/Worker.php) closes
the socket first, which is how a worker between jobs exits by itself, and
reaches for SIGKILL only for one still inside a handler.

The jobs those killed workers were holding stay PROCESSING — never
acknowledged, and never released from their lease. What brings them back is
worth being exact about, because the two recovery paths are not
interchangeable:

| the worker dies… | reclaimed by | when |
|---|---|---|
| while the runtime keeps running | the visibility timeout | the next tick after the deadline passes |
| killed by the shutdown grace period | the persistence log | the next start, as a PROCESSING record restored to READY |

The visibility timeout cannot save the second case: the runtime is on its
way out, so there is no later tick to expire anything on. **A bounded
shutdown is only as safe as the storage behind it** — with no
[`JobStorage`](src/Persistence/JobStorage.php) attached, a job killed by the
grace period is lost, exactly like any other in-flight job when the process
ends. Two tests cover the pair: one that the lease survives the shutdown
unreleased, and one that follows a grace-period-killed job through a real
restart back to READY.

SIGTERM and SIGINT only set a flag. Nothing is shut down from inside a
signal handler, because that is the only way the order of the steps stays
knowable. Workers also reset their inherited signal handlers on fork —
without that, a SIGTERM to the process group would run a runtime shutdown
inside every worker.

---

## The runtime

[`QueueRuntime`](src/Master/QueueRuntime.php) is the long-running process.
`drain()` runs until the work runs out, which is right for a script and
wrong for a server: a delayed job due in ten minutes, and a job whose
visibility timeout has not expired yet, are both work that does not exist
yet.

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

The wait is the interesting part. A loop with no wait burns a core; a loop
with a fixed sleep adds that sleep to every job's latency. So it waits on
whichever comes first:

* a worker answering — `stream_select` wakes on it;
* the next deadline it owns — `nextDeadline()`, answered without scanning;
* 50ms, so a signal is never further away than that.

`tick()` is public, because that is the honest way to test a runtime: a test
drives the ticks itself with a clock it controls, instead of racing a
background process.

---

## At-least-once, and what it costs you

The queue cannot promise a job runs exactly once. Not because it is small —
because the information does not exist:

```text
worker executes the job
        ↓
the side effect happens
        ↓
worker dies before the ACK
```

Retry, and the job may run twice. Do not retry, and it may never have run at
all. Nothing in the queue can tell which, so it assumes the pessimistic one
and delivers again.

Which moves the problem to where it can actually be solved — the handler:

```php
// Bad: "charge this card"
// Good: "charge order #123", once, whatever happens
final class ChargePaymentJob
{
    public function __invoke(Job $job): JobResult
    {
        $key = $job->getIdempotencyKey();

        if ($this->guard->isProcessed($key)) {
            return JobResult::success();      // already happened
        }

        ($this->charger)($job->getPayload()['order_id'], $job->getPayload()['amount']);
        $this->guard->markProcessed($key);

        return JobResult::success();
    }
}
```

The key names the **operation**, not the delivery — it comes from the
producer, so it is the same string on every redelivery. One derived from the
job id or the attempt number would deduplicate nothing. And
[`IdempotencyGuard`](src/Idempotency/IdempotencyGuard.php) persists, because
a restart is exactly when a job that already ran comes back — PROCESSING
jobs in the log return to READY, side effect and all.

### And this is deduplication, not exactly-once

Worth being precise about, because the gap is one line of code wide:

```text
isProcessed()  →  false
charge         →  the money has moved
💀              →  markProcessed() never runs
restart
isProcessed()  →  still false
charge         →  AGAIN
```

[`testACrashBetweenTheChargeAndItsRecordChargesTwice`](tests/Job/IdempotencyTest.php)
does exactly that — a forked child SIGKILLs itself between the two steps —
and asserts the double charge.

Closing that window needs the side effect and its record to **commit
together**: one transaction that both charges and stores the key, or the
charged system's own idempotency key so the second call is the one that
deduplicates. Both are properties of the thing being charged, not something
a queue can hand you. What a queue can do is deliver at least once, say so,
and leave the seam visible.

---

## Experiments

```bash
make docker-example EXAMPLE=basic-job        # the minimal milestone
make docker-example EXAMPLE=failed-job       # retries → backoff → DLQ → manual retry
make docker-example EXAMPLE=delayed-job      # deadline order, not push order
make docker-example EXAMPLE=worker-crash     # kill -9 mid-job, job survives
make docker-example EXAMPLE=priority-jobs    # starvation, and its cure
```

`worker-crash` prints this, and it is worth reading line by line:

```text
  [worker 5661] attempt 1, working...
  [worker 5661] about to be killed mid-job
  [worker 5662] attempt 2, working...
  [worker 5662] finished cleanly

job is COMPLETED after 2 attempt(s)
counters {"created":1,"worker_crashes":1,"completed":1}
```

Different pid, second attempt, same job.

### Break it yourself

**Watch a graceful shutdown.** `make docker-run-worker`, then from another
shell:

```bash
docker compose exec php pkill -TERM -f bin/worker.php
```

The 15-second job finishes, its result is applied, and only then does the
process exit. Now do it with `-KILL` instead and nothing is applied — and
since `bin/worker.php` runs without storage, that job is simply gone. Give
the dispatcher a `FileStorage` and try again to see it restored on the next
run.

**Make the visibility timeout too short.** Set it below your handler's
runtime and watch the same job run in two workers at once, with nothing
crashed and nothing failing. That is the row of the ACK table people do not
expect.

**Take the DLQ away.** Pass `dlq: null` and run `failed-job`. The job still
stops retrying — it just leaves nothing behind to look at.

**Take the visibility timeout away.** Pass `visibilityTimeout: null`, then
`kill -9` a busy worker. The EOF path still recovers the job; now kill the
whole runtime instead and see what the log alone can restore.

---

## Failure matrix

| what breaks | detected by | job outcome |
|---|---|---|
| handler throws | NACK | retried, then DLQ |
| worker killed while busy | EOF on its socket | back to READY at once, worker replaced |
| worker killed while idle | `maintain()` reaper | no job involved; capacity restored |
| worker killed between check and write | `WorkerDiedException` | back to READY at once |
| handler slower than the visibility timeout | deadline expires | delivered again; the expired delivery's answer is fenced out as a stale ACK |
| worker never answers | visibility timeout | back to READY when the deadline passes |
| runtime killed with SIGTERM | signal handler | in-flight jobs finish within the grace period |
| runtime killed with SIGKILL | nothing, until restart | PROCESSING jobs in the log return to READY |
| queue process dies with no storage | nothing | jobs are lost — this is what storage is for |
| job fails every attempt | attempts exhausted | FAILED, and a DLQ record |

---

## Performance

`make docker-bench ARGS="<jobs> <workers> <work-microseconds>"`, no-op
handlers, 8 workers, on the `php:8.5-cli` image:

| jobs | drained in | throughput | peak memory | queue wait (avg) | execution (avg) |
|--------:|-----------:|-----------:|------------:|-----------------:|----------------:|
| 1,000 | 0.055s | 18,000/s | 4 MB | 23 ms | 0.08 ms |
| 10,000 | 0.416s | 24,000/s | 12 MB | 215 ms | 0.14 ms |
| 100,000 | 8.368s | 12,000/s | 96 MB | 5,129 ms | 0.30 ms |

Which is the metrics section as a measurement rather than a claim: at
100,000 jobs the average job took five seconds and the average handler took
a third of a millisecond. The jobs were not slow. They were queued.

Doubling the workers on no-op jobs does not double throughput — the
dispatcher is one process writing to one socket at a time, and past a point
it, not the workers, is the limit. Which is the honest shape of this design,
not a bug in it.

---

## Project layout

```text
php-job-queue/
├── bin/
│   ├── run.php                     the lifecycle once (make run)
│   ├── worker.php                  the long-running process (make run-worker)
│   └── bench.php                   throughput and latency (make bench)
├── examples/                       one mechanism each (make example EXAMPLE=…)
├── src/
│   ├── Job/                        Job, JobId, JobState, JobPriority, JobResult
│   ├── Delivery/                   Delivery — the lease an ACK answers for
│   ├── Producer/                   Producer, JobFactory
│   ├── Queue/                      Queue, InMemoryQueue, PriorityQueue,
│   │                               LaneSelector + StrictPriority/WeightedRoundRobin
│   ├── Scheduler/                  DelayedJobScheduler, DelayedJobHeap
│   ├── Worker/                     Worker, WorkerPool, WorkerState,
│   │                               WorkerOutcome, WorkerDiedException
│   ├── Dispatcher/                 JobDispatcher
│   ├── Retry/                      RetryPolicy + FixedDelay/ExponentialBackoff
│   ├── Timeout/                    VisibilityMonitor
│   ├── DLQ/                        DeadLetterQueue, DeadLetterRecord
│   ├── Persistence/                JobStorage + InMemory/File
│   ├── Metrics/                    MetricsCollector, QueueMetrics
│   ├── Idempotency/                IdempotencyGuard, ChargePaymentJob
│   ├── Master/                     QueueRuntime
│   └── Support/                    Clock, SystemClock
├── tests/
│   ├── Chaos/                      kill things and follow the job
│   ├── Stress/                     1,000 and 10,000 jobs
│   └── …                           one directory per src/ namespace
└── docs/
    ├── ARCHITECTURE.md             the mechanisms in more depth
    └── DECISIONS.md                what was chosen, rejected, and fixed
```

Time is injected everywhere through [`Clock`](src/Support/Clock.php).
Everything about a queue is time — deadlines, delays, backoff, timeouts,
latency — and reading it through an interface is what lets a test advance
sixty seconds instantly instead of sleeping. One exception, marked where it
happens: the shutdown grace period is wall-clock, because it is how long a
real forked process gets to finish and no amount of faking time makes it
faster.

---

## Engineering questions, answered

**Why is exactly-once delivery difficult?** Because the queue cannot tell
"the worker died before doing the work" from "the worker did the work and
died before saying so". It has to guess, and guessing wrong in one direction
loses work while the other repeats it. See
[at-least-once](#at-least-once-and-what-it-costs-you).

**Request timeout vs visibility timeout?** A request timeout is about how
long *someone waits*. A visibility timeout is about how long a job may
remain *unacknowledged*. Different problems: exceed the first and a caller
gives up; exceed the second and the job is delivered to somebody else while
the first worker is still running it.

**Why does ACK exist?** Because "the worker received the job" is not "the
job completed". Without an explicit acknowledgement there is no moment at
which the queue may forget a job, and no way to distinguish a slow job from
a lost one.

**Why can a job execute twice?** Job executed → ACK lost → the queue assumes
failure → redelivery. Expected in an at-least-once system, and observable
here in two ways: a crash before the ACK, and a handler that outlives the
visibility timeout. The second is the harder case, because both workers are
alive and both will answer — see
[deliveries and fencing](#deliveries-and-fencing).

**Why must handlers be idempotent?** Because the queue's guarantee is
at-least-once, and *your* guarantee has to be built on top of it. "Charge
this card" is not safe to repeat; "charge order #123" is — up to the crash
window that only the charged system itself can close.

**What happens when a worker crashes?** The worker is gone; the job is not.
That distinction — worker lifecycle is not job lifecycle — is what most of
this repository is about.

---

## What this is not

Not RabbitMQ, Kafka, Redis, Temporal, Laravel Queue or Sidekiq. Specifically
missing, and deliberately:

* no network protocol — one process, forked workers, a Unix socket pair;
* no shared queue between machines, so no distributed locking or leader
  election;
* no snapshots to bound the persistence replay;
* no transactional handler boundary — a side effect and its idempotency
  record cannot commit together, so deduplication keeps a crash window;
* no autoscaling — the pool size is fixed, because the interesting question
  here is what happens to a **job** when a worker disappears
  ([php-worker-pool](https://github.com/Researcher86/php-worker-pool) is
  where autoscaling and recycling live);
* no handler registry, container, or serialization framework;
* no jitter on the backoff, and no rate limiting.

Every one of those is a real production concern. Adding them would make the
mechanisms harder to see, which is the only thing this repository is
optimising for:

```text
Small enough to understand.
Simple enough to modify.
Real enough to fail.
Reliable enough to study.
```

---

## Related projects

Three repositories, one subject, in order of depth:

* [**php-concurrency**](https://github.com/Researcher86/php-concurrency) —
  processes, forks, signals and IPC in PHP from the bottom up. The
  groundwork.
* [**php-worker-pool**](https://github.com/Researcher86/php-worker-pool) — a
  master process with an event loop, a pool of forked workers, autoscaling,
  recycling, telemetry over shared memory. How to keep N processes alive and
  busy.
* **php-job-queue** (this one) — what happens to the *work* when those
  processes fail. Worker lifecycle is not job lifecycle, and this is the
  half of the problem the pool does not answer.

---

## License

MIT.
