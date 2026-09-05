# The crossing is CPU-heavy and serialised at one lock, so threads cannot fix it

<!-- inflight-type: register -->

Ran 2026-08-24 to answer two objections raised against
[`../plans/2026-08-24-002-feat-streams-invocation-bundling-plan.md`](../plans/2026-08-24-002-feat-streams-invocation-bundling-plan.md)
in review, both of which would have killed that plan had they held. **Neither held**, and the
experiment turned up a third thing nobody had asked about which matters more than either.

## The two objections

1. **"The core-hours argument rests on wall-clock, not CPU."** The ~150us crossing was measured on
   the sink's log-append clock at one stream thread - serial latency, not CPU burned. A syscall, a
   thread handoff and a wake are blocked time occupying no core. If the crossing were mostly
   blocked, more threads would recover the throughput at a fraction of the core cost and bundling's
   efficiency case would collapse.
2. **"Threads are named as try-first and were never tried."** Every measurement in this repo pinned
   `num.stream.threads` to 1, so the cheapest lever had never once been run.

## Method

CPU is read from `/usr/bin/time`, which accounts for child processes - the JVM sidecar is a child of
the demo, so both processes' CPU lands in one total. **Verified before use** rather than assumed: a
control burning CPU in a child and sleeping in the parent reported 2.17s real against 1.10s user.

Arm A holds threads at 1 and sweeps record count, treatment against control, taking the slope so
fixed startup cancels - the same discipline the two prior measurements established. Arm B sweeps
thread count at a fixed record count with partitions raised to 8 so threads are not partition-capped.
1 KB payloads throughout, the low end of the stated profile. **24 runs, all exited 0 with exact
per-key counts and a settled consumer group.**

## Finding 1: the crossing is CPU, not blocked - the objection is refuted

| | per record |
|---|---|
| Crossing **wall** cost | 152us |
| Crossing **CPU** cost | **232us** |

CPU **exceeds** wall time, which is not a contradiction: the host and the engine burn CPU
concurrently on different cores, so one crossing occupies roughly **1.5 cores for its duration**.

The objection is refuted, and the correction runs the other way from the one it predicted: the
core-hours figures in
[`next-batching-modes-for-clients.md`](next-batching-modes-for-clients.md) equate occupied stream
threads with burned cores, and that **understates** CPU by about 1.5x rather than overstating it.
The efficiency case for bundling is stronger than it was written, not weaker.

## Finding 2: threads plateau at 1.5x and cost more CPU per record

| threads | rec/sec | speedup | CPU us/record |
|---|---|---|---|
| 1 | 6,483 | 1.00x | 541 |
| 2 | 9,070 | 1.40x | 577 |
| 4 | 9,662 | 1.49x | 619 |
| 8 | 9,501 | 1.47x | 665 |

**Threads are not the answer.** Throughput stops improving after two, and CPU per record gets
steadily worse as threads are added - 8 threads burn 23% more CPU per record than 1 for no
additional throughput. "Try threads first" is now answered: they were tried, and they do not work.

## Finding 3, which nobody asked for and matters most: the boundary is serialised

The plateau is not a CPU limit. **Every crossing is serialised through a single lock.**
`StreamsSessionService` holds one `Session`, one `StreamObserver` to the client, and guards every
outbound message with one `transmitLock`. However many stream threads exist, their invocations
queue at that lock and cross one at a time.

That explains the shape exactly: two threads help because one can compute while the other transmits;
beyond two there is nothing left to overlap, and the extra threads only add contention.

**This reframes what bundling is for.** The plan justified it as amortising a fixed CPU cost. The
better justification is that bundling reduces how often the serialised path is traversed, which is
the actual bottleneck. Same build, sounder argument.

**It also opens an option nobody has considered: more than one session.** One gRPC stream per stream
thread would remove the serialisation without any bundling, any Processor API migration, any buffer
between process and forward, and any commit hazard. It is a smaller change than the bundling plan
and it attacks the mechanism this experiment actually found. **It should be measured before the
bundling plan proceeds** - the same argument that sent this experiment to threads first applies to
it with more force, because unlike threads it addresses the real constraint.

## Limits

- One machine, loopback gRPC, 1 KB payloads, two reps per point.
- Arm B's plateau is established at 8 partitions; a partition-starved topology would plateau earlier
  for a different reason.
- The multi-session option above is **reasoned from the code, not measured.** It is a hypothesis
  with a named mechanism, which is exactly the status "threads will help" had before this ran.
- CPU attribution is process-total, so it includes gRPC's own threads. That is the honest number for
  "what does a crossing cost the machine", and it is not a per-thread figure.

## Prior art

- [`perf-streams-crossing-attribution.md`](perf-streams-crossing-attribution.md) - the crossing's wall cost
- [`perf-crossing-fixed-versus-per-byte.md`](perf-crossing-fixed-versus-per-byte.md) - its fixed/per-byte split
- [`next-batching-modes-for-clients.md`](next-batching-modes-for-clients.md) - the core-hours table this corrects
