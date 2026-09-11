# Decisions

What was chosen, what was rejected, and the bugs that changed the design.
Indexed by decision, so a question like "why is a retry not its own queue?"
has one place to look.

The phases these belong to are in [PLAN.md](../PLAN.md); the mechanisms
themselves are in [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. Workers are forked processes, not objects

**Chosen:** `pcntl_fork()` per worker, with a Unix socket pair.

**Rejected:** a "worker" class that calls the handler in-process. It would
have made every test synchronous and every failure a caught exception.

**Why:** the questions the project exists to answer are what happens to a
*job* when its worker segfaults, hangs, or is `kill -9`'d. None of those are
expressible in-process. A caught exception is not a crash, and a test that
simulates one is testing the simulation.

The cost is real: tests fork, the suite takes nine seconds instead of one,
and a leaked child process is a real failure mode the suite has to avoid.
Worth it — every chaos test kills a real process and follows a real job.

---

## 2. A retry is a delayed job

**Chosen:** `markRetry($availableAt)` puts a job in READY with a future
`availableAt`, and the same `DelayedJobScheduler` that holds delayed jobs
holds it.

**Rejected:** a separate retry queue, or a "retry at" field checked
separately at pop time.

**Why:** they are the same thing. A job that may not run until a time is a
job that may not run until a time, whether that time came from
`delay: 3600` or from an exponential backoff. Two mechanisms would mean two
places to get the boundary condition wrong, and two things to explain.

The visible consequence: `delayedSize()` counts jobs waiting on a backoff as
well as jobs waiting on a delay, which is correct — both are "waiting, not
available".

---

## 3. The delayed set is a min-heap

**Chosen:** `SplHeap` ordered by `availableAt`, with insertion order as a
tie-break.

**Rejected:** an array re-sorted on every push (what it was originally).

**Why:** three reasons, in increasing order of importance.

1. O(log n) to insert instead of O(n log n).
2. The sort was redundant work — it re-ordered jobs whose position had not
   changed. With a fixed-delay retry policy and a queue of failing jobs,
   every single failure paid for a full sort.
3. **`nextDeadline()` in O(1) off the root.** This is the one that mattered:
   it is what lets `QueueRuntime` sleep *until* the next deadline instead of
   waking up to ask whether it has arrived. Without it the loop needs a
   fixed poll interval, which is either a busy loop or added latency on
   every job.

The tie-break is not decoration. A burst of fixed-delay retries all land on
the same timestamp, and without it the order they come back out in is
whatever the heap's internal swaps produce. The sorted array had FIFO there,
because PHP's sort has been stable since 8.0, and losing it would have been
a silent regression.

**And see decision 3a**, which is the same lesson applied to the set this
one is not about. Getting the delayed set right while leaving the ready set
on `array_shift()` left an O(n²) in the hot path for the whole of the
project's life.

---

## 3a. The ready set is an SplQueue

**Chosen:** `SplQueue` for the ready FIFO in `InMemoryQueue` and for each
lane in `PriorityQueue`.

**Rejected:** a plain array with `array_shift()`, which is what it was.

**Why:** `array_shift()` reindexes the whole array. That is O(n) per pop, so
draining n jobs is O(n²) - and it is the hot path, touched on every single
pop. Popping a full queue with no workers involved:

| pops | array_shift | SplQueue |
|--------:|--------:|--------:|
| 25,000 | 0.328s | 0.015s |
| 50,000 | 1.191s | 0.029s |
| 100,000 | 4.815s | 0.059s |
| 200,000 | 19.602s | 0.116s |

Doubling the depth quadrupled the time. End to end, 1,000,000 jobs through
the dispatcher went from 541s to 30s, and throughput stopped falling as the
queue got deeper. `php-worker-pool`'s `RequestQueue` is an `SplQueue` for
exactly this reason, which is the part that stings.

**Worth recording as a mistake, not just a fix.** Decision 3 moved the
DELAYED set off an O(n log n) sort onto a min-heap, with a paragraph about
why the data structure mattered - and left the ready set, which is hit far
more often, on an array. Fixing the interesting structure and leaving the
obvious one is easy to do twice.

It also corrupted the previous measurement's conclusion. The storage share
appeared to FALL with depth (28% at 10k, 15% at 100k), which read like a
real property of the log and was an artefact of everything around it
getting slower. With the pop linear, the share is stable and slightly
rising, which is what a per-record cost against faster surroundings should
look like.

**Found by:** the profiling run at 1,000,000 jobs that the review suggested
- not by reading the code. It had been there since Phase 2.

---

## 4. Attempts count deliveries, not runs

**Chosen:** `markProcessing()` increments `attempts`, so a job handed to a
worker that died before starting has used one.

**Rejected:** incrementing on completion, or refunding an attempt when a
delivery demonstrably never ran.

**Why:** the queue cannot tell whether a delivery ran. That is the whole
premise of at-least-once — if it could distinguish "never started" from
"finished but never acknowledged", exactly-once would be easy. Refunding
attempts for the cases it *can* detect would make the counter mean different
things in different failure modes.

Real queues count receipts too (SQS's `ApproximateReceiveCount`), for the
same reason.

It also turned out to be the fencing token the system needed. Because
`markProcessing()` increments it exactly once per handing-out, `(job id,
attempts)` uniquely names one delivery — see decision 15, which is built on
that and needed no counter of its own.

---

## 5. A worker reports an outcome, not a result

**Chosen:** `WorkerOutcome(Job, ?JobResult)`, where a null result means the
worker died holding the job.

**Rejected:** `JobResult::failure(new WorkerCrashedException())`.

**Why:** they need opposite handling. A NACK consumes an attempt and may end
in the DLQ, because the job was tried and did not work. A crash consumes
nothing conclusive and puts the job straight back, because nothing reported
anything about it. Encoding a crash as a failure would have made
`maxAttempts` count worker deaths against a job that may be perfectly fine.

The outcome also carries the `Delivery` rather than the job, so the answer
can be attributed to one specific handing-out — see decision 15.

---

## 6. Two crash detection paths, kept apart

**Chosen:** a busy worker's death is found by EOF in `collect()`; an idle
worker's by `waitpid(WNOHANG)` in `maintain()`. `Worker::reap()` refuses to
look at busy workers.

**Rejected:** one reaper for all states, or a `SIGCHLD` handler.

**Why:** only the EOF path knows *which job* died with the worker. A reaper
that got there first would mark the worker DEAD, and the job would sit in
the in-flight set until its visibility deadline instead of being requeued
immediately. Keeping the paths disjoint means each crash is handled by the
one detector that has enough information.

`SIGCHLD` was rejected separately: an async handler racing the poll loop is
how the reap-versus-respond ordering bugs in
[php-worker-pool](https://github.com/Researcher86/php-worker-pool) happened,
and a synchronous non-blocking `waitpid` once per tick costs one syscall per
idle worker.

**Bug this fixed:** an idle worker's death was invisible. The pool reported
it as available, `hasDeadWorkers()` as false, and the next `assign()` threw
`RuntimeException` out of the dispatch loop — taking the runtime down and
leaving the job marked PROCESSING with nothing behind it. Verified before
fixing.

---

## 7. Nothing happens in a signal handler

**Chosen:** `SIGTERM`/`SIGINT` set a boolean. The loop notices on its next
pass and runs the shutdown itself.

**Rejected:** shutting down from inside the handler.

**Why:** the shutdown has an order — stop accepting, drain, collect, tear
down — and a handler can fire in the middle of any of those steps. Setting a
flag makes the ordering knowable, at the cost of up to `$maxWait` (50ms) of
latency before the shutdown starts.

Related: workers reset inherited handlers to `SIG_DFL` immediately after
forking. Without that, a SIGTERM to the process group runs a runtime
shutdown inside every worker.

---

## 8. The shutdown grace period is wall-clock

**Chosen:** `microtime()` for the grace deadline, even though `Clock` is
injected everywhere else.

**Why:** every other deadline in the system is about job semantics, and a
test needs to advance those sixty seconds instantly. This one is about how
long a real forked process gets to finish real work, and no amount of faking
time makes a fork run faster. A `FakeClock` that never advances would have
turned the grace loop into an infinite one.

Marked at the site, because an inconsistency that is deliberate has to say
so.

---

## 9. Counters and gauges are separate objects

**Chosen:** `MetricsCollector` (cumulative) and `QueueMetrics` (a snapshot,
read from the live objects by `observe()`).

**Rejected:** gauges as more entries in the counter bag; or gauges
maintained incrementally as jobs move.

**Why:** two different reasons.

* A counter only grows; a gauge is already stale when you read it. One bag
  for both is how "queue size: 40,000" ends up on a dashboard as a number
  that never goes down.
* A hand-maintained gauge drifts. Every push, pop, crash, requeue and
  shutdown would have to remember to adjust it, and the one path that
  forgets is invisible — the number is simply wrong, with nothing to
  compare it against. Reading it from the live objects cannot drift.

---

## 10. Three latencies, not one

**Chosen:** `queue_wait`, `execution`, `end_to_end`, measured separately.

**Why:** they lead to different fixes. Queue wait high and execution low
means add workers; the reverse means fix the handler. One number cannot say
which, and the bench makes the difference concrete: at 100,000 jobs the
average job took five seconds and the average handler took a third of a
millisecond.

Queue wait is measured from `availableAt`, **not** from `createdAt`, so a
job deliberately delayed by an hour does not report an hour of queue wait.
It reports that hour in end-to-end, where it belongs: the caller did wait,
but the queue was not behind. There is a test for exactly that distinction,
because it is the kind of thing that looks like a bug either way round.

---

## 11. Strict priority is the default, and starvation is demonstrated

**Chosen:** `StrictPriority` by default, `WeightedRoundRobin` available,
both tested — including a test that shows strict priority starving a LOW job
forever.

**Rejected:** making the fair policy the default and mentioning starvation
in a comment.

**Why:** strict priority is what "priority queue" means to most people, and
its failure mode is the thing worth understanding. A default that quietly
avoids the problem teaches nothing. The starvation test keeps one HIGH job
arriving for every job served — what a busy system looks like — and the LOW
job pushed first is still waiting fifty pops later. The next test fits the
weighted selector to the same load and it comes out sixth.

The policy is an object rather than a flag for the same reason `RetryPolicy`
is: the queue's job is holding jobs in lanes, and this question has more
than one defensible answer.

---

## 12a. The dispatch is written before the job leaves the process

**Chosen:** `dispatch()` appends a PROCESSING record after marking the job
and taking its lease, before writing to the worker's socket.

**Why:** without it the log's last word on an in-flight job is the READY
record from `push()`, with attempts 0. Two things follow, and both are
worse than the extra write.

The attempt count is not durable, so a crash hands the job its whole
allowance again - and a job that reliably kills its worker (an OOM payload,
a segfaulting extension) loops across restarts forever instead of reaching
the DLQ, which is the one thing the DLQ exists to prevent.

And the PROCESSING branch of `restoreFromStorage()` was unreachable from
anything the system itself wrote. It was covered only by a test that
appended the record by hand, which is how it went unnoticed: the branch
worked, nothing ever took it.

**Cost:** measured rather than estimated - `bin/bench.php` grew a storage
argument for it, and the numbers live in
[BENCHMARKS.md](BENCHMARKS.md#what-durability-costs). At 10,000 no-op jobs
on 8 workers: 27,100/s with no storage, 23,600/s in memory, 19,100/s to a
file, at exactly 3.0 records per job. The per-record cost is a constant
5 µs whatever the depth, and about three quarters of it is the filesystem
append rather than the serialisation - an `encode` probe in the bench
splits the two. What argues for the snapshots this project does not have is
the log size at a million jobs: 804 MB, replayed in full at startup.

**Related:** the rule that keeps the write volume at three records and not
four is in `JobDispatcher::persist()` - the queue persists any job it takes
in, the dispatcher persists only a state change that does not go into a
queue. `DispatcherPersistenceTest` asserts the sequence per outcome,
because a redundant record is not a correctness bug and nothing else would
have complained: the retry path wrote READY twice until that test existed.

---

## 12. Append-only log, and a torn final record is tolerated

**Chosen:** one JSON object per line, appended, keyed by job id, last write
wins. `load()` drops a malformed **final** line and throws on a malformed
line anywhere else.

**Rejected:** rewriting a snapshot file on each change; and refusing to load
a log with any bad line in it.

**Why:** appending is the operation that is hard to half-finish. A crash
mid-append leaves a truncated last line and every complete line before it
intact; a crash mid-rewrite can lose the whole file. And the torn line is
*from the crash we are recovering from* — refusing to start because the last
record is half-written makes the log useless exactly when it is needed. A
bad line in the middle is a different claim (this file is corrupt, or is not
a job log) and should not be swallowed.

Not done: periodic snapshots to bound the replay. The log grows with every
state change and `load()` replays all of it. Stated as a limitation rather
than hidden.

**Bug this fixed:** `load()` used `JSON_THROW_ON_ERROR` and died on the torn
line — the docblock claimed crash tolerance the code did not have.

---

## 13. Every collaborator of the dispatcher is optional

**Chosen:** `RetryPolicy`, `DeadLetterQueue`, `JobStorage`,
`MetricsCollector` and the visibility timeout are all nullable constructor
arguments.

**Rejected:** requiring them, with null-object implementations for the
"off" case.

**Why:** each one is a mechanism the reader is supposed to be able to
examine on its own — and to *see the system work without*. Running the queue
with no DLQ and watching a job stop retrying with nothing left behind
explains the DLQ better than any docblock. Null objects would have hidden
that switch behind an extra class each.

---

## 14. `dispatchPending()` and `collect()` are separate

**Chosen:** two methods, called in sequence by both `drain()` and
`QueueRuntime::tick()`.

**Rejected:** `dispatchNext()` as the only loop primitive (which is what it
was).

**Why:** dispatch-one-and-wait gives a pool of one, whatever its size says.
Splitting the halves is what lets N workers run N jobs concurrently.
`dispatchNext()` still exists, because a script or a test sometimes wants
one job to have happened by the time the call returns — with a docblock
saying exactly why a runtime must not be built on it.

---

## 15. An ACK is attributed to a delivery, not to a job id

**Chosen:** every handing-out of a job is a `Delivery` — the job, which
handing-out this is, the worker, when it went out, when its ACK is overdue.
`VisibilityMonitor` holds the delivery currently entitled to answer for each
job, and `applyResult()` fences on `isCurrent($delivery)`.

**Rejected:** checking whether the JOB is still PROCESSING, which is what it
did first.

**Why:** the state check cannot work, and this is the most interesting bug
the project has had. A job can legitimately be in two workers' hands at once
— the deadline expires while a slow handler is still running — and by the
time the first worker's late answer arrives the job genuinely IS PROCESSING,
because the *second* worker put it there. Same object, same state, two
deliveries, nothing to tell them apart.

Measured before fixing it, worker A slow, deadline expired, worker B given
the job, A answering first:

```text
after 2 deliveries: state=PROCESSING attempts=2 tracked=true
after A's late ACK: state=COMPLETED  tracked=false  completed=1
after B's real NACK: state=COMPLETED stale_acks=1
```

Three failures in one line of output. The obsolete answer completed a job
the live worker was still running. It released the LIVE delivery's tracking,
leaving a job in a worker's hands with no deadline that could ever bring it
back — a duplicate delivery turned into a losable job. And worker B's real
answer, a failure, was discarded as stale: a job that failed was recorded
completed. The roles were inverted.

**Where the generation comes from:** nowhere new.
`Job::markProcessing()` already increments `attempts` exactly once per
handing-out, which is why decision 4 says an attempt counts a delivery
rather than a run. The fencing token was in the model already and was not
being used as one.

**Two things the fence must not do**, both of which it once did: apply a
stale answer, and release the lease on one. The lease under that job's id
belongs to the live delivery.

Counting rather than silently dropping, because a rising `stale_acks` has a
diagnosis: the visibility timeout is shorter than the work it is timing.

**Also fixed by the same change:** the dispatcher's `startedAt` map, keyed by
job id, so two deliveries of one job shared an entry and whichever answered
first consumed it — an execution latency attributed to the wrong delivery.
`Delivery::getDispatchedAt()` replaced the map.

**Prior art:** this is a lease token, a fencing token, an SQS receipt handle.
The right to acknowledge belongs to a specific claim, not to whoever holds
the id.

---

## 15a. And the earlier bug the state check was introduced for

Before any of the above, there was no stale check at all: the second
answer walked into `markCompleted()` on an already-COMPLETED job and threw
`LogicException` out of the dispatch loop. Found by writing the
slow-handler chaos test, not by review — and the state check that fixed it
was itself only right for the case that test covered.

Worth recording because it is a pattern: a fix aimed at the symptom (an
exception) that stopped short of the cause (deliveries had no identity).

---

## 16. State machines are tables, and flags are not states

**Chosen:** one `TRANSITIONS` table per state machine (`Job`, `Worker`),
with a single private `apply()` as the only thing that assigns a state.

**Rejected:** per-method guards (what both classes had); and a boolean
alongside the enum (what `Worker` had).

**Why:** with a table, adding a state means editing one place and
immediately seeing every event it has to answer for. With guards spread over
seven methods, a state that answers one event wrongly is invisible until
something breaks.

The boolean was worse than untidy. `Worker` kept a `$draining` flag next to
a `WorkerState` enum that had a `DRAINING` case it never entered, and
`drain()` assigned `$this->state` directly — bypassing the only gate that
was supposed to exist. What the flag was standing in for is a real
distinction, and it is now explicit: the **state** says whether new work may
be dispatched here, `isWorking()` says whether work is happening. A worker
drained mid-job is DRAINING — not available, still working.

---

## 17. `fromName()` walks `cases()`

**Chosen:** an explicit loop over `self::cases()`, throwing `ValueError`.

**Rejected:** `constant("self::$name")`, which is what it was.

**Why:** `constant()` resolves *any* class constant. A persistence log with
an unexpected value in it would have returned whatever constant matched —
including `Job::TRANSITIONS`. The input comes from a file on disk; it
deserves a `ValueError`, not a lookup. There is a test that feeds it
`'TRANSITIONS'`.

---

## 18. Formatting is settled by a tool

**Chosen:** PHP CS Fixer, PER-CS 2.0 plus four rules, checked in CI.

**Why:** the project is meant to be read, and a diff should show a change in
behaviour rather than a change in brace placement.

One preset rule is overridden: PER-CS wants `fn(`, and both this project and
php-worker-pool were written with `fn (`. The existing spelling wins over
the preset — consistency with the sibling repository is worth more than
consistency with a document.

---

## 19. The idempotency guard is deduplication, and says so

**Chosen:** `IdempotencyGuard` is documented as a deduplication mechanism,
with the crash window it leaves stated explicitly and covered by a test that
demonstrates a double charge.

**Rejected:** the earlier wording, which claimed exactly-once could be
"built at the other end, by the handler, out of at-least-once delivery plus
a key it can check. That is the whole of it."

**Why:** it is not the whole of it. Checking the key and performing the side
effect are two steps, and so are the side effect and recording it:

```text
isProcessed()  →  false
charge         →  the money has moved
💀              →  markProcessed() never runs
restart        →  isProcessed() is still false, so it charges AGAIN
```

A forked child SIGKILLs itself between the two steps in
`IdempotencyTest::testACrashBetweenTheChargeAndItsRecordChargesTwice`, and
the assertion is the double charge. Making the limit observable is worth
more than a caveat next to it, and it is the same principle as
demonstrating starvation rather than describing it (decision 11).

Closing the window needs the side effect and its record to commit together —
one transaction that does both, or the charged system's own idempotency key.
Both are properties of the thing being charged, not something a queue can
provide. What a queue can do is deliver at least once, say so, and leave the
seam visible.

---

---

## Rejected outright

Things that were considered and left out, with the reason.

| idea | why not |
|---|---|
| Autoscaling the pool | [php-worker-pool](https://github.com/Researcher86/php-worker-pool)'s subject. Here a fixed pool makes "what happens to the job" easier to watch. |
| A handler registry / DI container | A `Closure` per pool and a `match` on job type is enough, and keeps handlers free of the framework. |
| Jitter on the backoff | Right in production, unreadable in an example. Named in the docblock instead. |
| Snapshots for the persistence log | Bounded replay is a real need and a second mechanism to explain. Stated as a limitation. |
| A network protocol | Would need auth, framing over TCP, and payload validation — all of it orthogonal to job semantics. |
| Serializing exceptions across the socket | Not reliably possible, and the stack refers to a dead process. Class plus message is what a human needs. |
| `SIGCHLD`-driven reaping | An async handler racing the poll loop is a known source of ordering bugs. One `waitpid` per tick is enough. |
| A `ProcessingQueue` class | The in-flight set is the visibility monitor's, and it needs the leases and deadlines anyway. A second holder of the same jobs would be two sources of truth. |
| A UUID per delivery | The attempt count already identifies a handing-out uniquely and reads better in a log ("delivery 2"). A second identifier would have to be kept in step with the first. |
| Making the idempotency guard transactional | It cannot be, from here — the side effect belongs to something else. See decision 19. |
