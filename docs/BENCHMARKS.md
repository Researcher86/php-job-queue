# Benchmarks

Numbers from `bin/bench.php`, and what they mean. Everything here is
reproducible:

```bash
make docker-bench ARGS="<jobs> <workers> <work-microseconds> <storage>"
```

Measured on the project's own `php:8.5-cli` image, 12 cores available,
no-op handlers, 8 workers. The absolute figures belong to that machine; the
shapes are the point.

## How the harness measures

The bench pushes every job first, then drains. So it measures a **full
backlog**, not a steady state — which is why the queue-wait figures are
seconds rather than milliseconds, and why the queue depth is bounded by RAM
(1,000,000 jobs is 906 MB of `Job` objects, and needs
`php -d memory_limit=3G`).

`storage` selects the persistence backend, and one of the four is a
measurement probe rather than a usable one:

| value | what it does |
|---|---|
| `none` | no persistence |
| `memory` | `InMemoryStorage` — an array assignment per record |
| `encode` | serialises each record exactly as `FileStorage` would, then drops it |
| `file` | `FileStorage` — `json_encode` plus an append |

`encode` exists to split the cost of turning a job into JSON from the cost
of writing the JSON down.

---

## Throughput and depth

No storage:

| jobs | drained in | throughput | peak memory | queue wait (avg) | execution (avg) |
|--------:|-----------:|-----------:|------------:|-----------------:|----------------:|
| 1,000 | 0.077s | 13,000/s | 4 MB | 27 ms | 0.16 ms |
| 10,000 | 0.369s | 27,100/s | 14 MB | 186 ms | 0.13 ms |
| 100,000 | 3.631s | 27,500/s | 98 MB | 1,877 ms | 0.14 ms |
| 1,000,000 | 30.088s | 33,200/s | 906 MB | 18,765 ms | — |

Three things to read out of it.

**Throughput is flat from 10,000 up.** That is the property worth having and
it was not free — see [the quadratic pop](#the-quadratic-pop) below.

**1,000 jobs is the slowest run.** Forking eight workers costs the same
whether they then handle a thousand jobs or a million, and over 77
milliseconds that fixed cost is most of the measurement. Short benchmarks
measure startup.

**Queue wait grows with depth and execution does not.** 1.9 seconds of
average wait against 0.14 ms of average handler at 100,000 jobs. This is
Phase 14's whole argument as a measurement: a job that took two seconds is
not evidence of a slow handler. It waited.

---

## What durability costs

10,000 jobs, 8 workers:

| storage | throughput | time in storage | per write | log |
|---|---:|---:|---:|---|
| `none` | 27,100/s | — | — | — |
| `memory` | 23,600/s | 4.4% | 0.6 µs | — |
| `encode` | 22,800/s | 8.4% | 1.2 µs | — |
| `file` | 19,100/s | 28.5% | 5.0 µs | 30,000 records, 8 MB |

The same at larger depths, `none` against `file`:

| jobs | none | file | time in storage | per write | log |
|--------:|----------:|----------:|---:|---:|---|
| 10,000 | 27,100/s | 19,100/s | 28.5% | 5.0 µs | 8 MB |
| 100,000 | 27,500/s | 20,600/s | 30.5% | 5.0 µs | 80 MB |
| 1,000,000 | 33,200/s | 22,300/s | 33.3% | 5.0 µs | 804 MB |

So durability costs roughly a quarter to a third of the throughput here, and
the cost per record is **constant at 5 µs** regardless of depth — which is
what a per-record cost should look like.

### Where the 5 µs goes

Subtracting the probes:

```text
memory   0.6 µs   ← array bookkeeping
encode   1.2 µs   ← + json_encode        →  serialisation ≈ 0.6 µs
file     5.0 µs   ← + the append         →  filesystem    ≈ 3.8 µs
```

The filesystem is about three quarters of a write. Serialisation is not the
bottleneck, which is worth knowing before optimising the wrong half: a
faster encoder would buy back a tenth of the storage cost at most.

Three records per job — `READY`, `PROCESSING`, and the outcome — and the
middle one is what makes the attempt survive a crash. See
[DECISIONS 12a](DECISIONS.md#12a-the-dispatch-is-written-before-the-job-leaves-the-process).

### The number that argues for snapshots

**804 MB of log for 1,000,000 trivial jobs**, and `load()` replays all of it
at startup. That is the concrete case for periodic snapshots and compaction,
which this project deliberately does not have. It is also why the figure is
here rather than described: "the log grows" is easy to nod at, 804 MB is
not.

---

## The quadratic pop

The numbers above are the second set. The first set looked like this:

| jobs | throughput |
|--------:|-----------:|
| 10,000 | 24,000/s |
| 100,000 | 12,000/s |
| 1,000,000 | 1,800/s |

Throughput collapsing as the queue got deeper, which is not what a FIFO
should do. Popping a full queue with no workers involved isolated it:

| pops | before | after |
|--------:|--------:|--------:|
| 25,000 | 0.328s | 0.015s |
| 50,000 | 1.191s | 0.029s |
| 100,000 | 4.815s | 0.059s |
| 200,000 | 19.602s | 0.116s |

Doubling the depth quadrupled the time. The ready set was a plain array and
`pop()` used `array_shift()`, which reindexes it — O(n) per pop, so draining
n jobs was O(n²). `SplQueue` is a doubly linked list: `dequeue()` is O(1),
and the rate is flat at about 1.7 million pops a second.

End to end, 1,000,000 jobs went from **541s to 30s**. The same bug was in
`PriorityQueue`'s lanes.

Two things it is worth having found this way rather than by reading:

- The delayed set had already been moved off an O(n log n) sort onto a
  min-heap ([DECISIONS 3](DECISIONS.md#3-the-delayed-set-is-a-min-heap)),
  and the *hot* path — the ready set, touched on every single pop — was
  left on an array. Fixing the interesting data structure and leaving the
  obvious one is an easy mistake to make twice.
- It corrupted the conclusion of the previous measurement. The storage
  share appeared to *fall* with depth (28% at 10k, 15% at 100k), which
  looked like a real property and was an artefact: everything else was
  getting slower. With the pop fixed, the share is stable and slightly
  rising, as a per-record cost against faster surroundings should be.

`StressTest::testPoppingDoesNotGetSlowerAsTheQueueGetsDeeper` holds the
property now: 50,000 pops in under half a second, an order of magnitude
above the linear timing and well below the quadratic one.

---

## Where the ceiling is

At the top end the limit is the dispatcher, not the workers. It is one
process, writing to one socket at a time, and every job costs it a
`stream_select`, a framed write, a framed read, and the bookkeeping around
them. Doubling the workers on no-op jobs does not double throughput.

That is the honest shape of a single-master design rather than a defect in
it — and it is visible in the numbers above, which is the reason they are
published.
