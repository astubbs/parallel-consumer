# The crossing split: ~120us fixed plus ~6.5us per KB, so bundling's payoff depends on record size

<!-- inflight-type: register -->


Answers the question [`next-batching-modes-for-clients.md`](next-batching-modes-for-clients.md)
named as the next experiment, and it is the last input to the data-plane decision: **bundling can
only amortise the crossing's FIXED cost, never its per-byte cost**, so the split decides whether
bundling is worth building at all.

## The prediction, written before the run

Stated in conversation before any measurement: the crossing at 15-byte records would prove
fixed-dominated, because 150us to copy 15 bytes is absurd on its face - but the number had been
measured at a payload size no real workload uses, so the interesting question was **where the
crossover sits**, not whether small records are fixed-bound.

**Both halves held.** Below 1 KB the cost does not vary with size at all, and the crossover is at
roughly 18 KB, which is inside the range real Kafka Streams workloads occupy.

## Method

Payload size swept with the **record count held fixed at 20000**. That is the design decision that
makes the result readable: the crossing path warms up over tens of thousands of invocations, so
every run carries a warm-up component - but warm-up is a function of invocation *count*, not of
payload size. Holding the count constant makes that component identical at every point, so it
cancels out of the size-slope instead of contaminating it.

Both arms at every size. The control (`--no-transform`) removes the mapValues node entirely, so it
pays the engine's own per-byte cost - Kafka deserialisation, bytes through the topology - with no
crossing at all. Sizes were interleaved rather than run in ascending order, so a machine drifting
slower through the session cannot masquerade as a per-byte slope.

**24 runs, 6 sizes (16 B to 16 KB), 2 arms, 2 reps. All 24 exited 0 with exact per-key counts and
the group STABLE throughout**, so no rebalance contaminates any window. Measured on the sink's
log-append clock, on a dedicated broker with no other agents running.

## What it found

Per-record cost, treatment arm (mean of two reps):

| Payload | 16 B | 64 B | 256 B | 1 KB | 4 KB | 16 KB |
|---|---|---|---|---|---|---|
| us/record | 159 | 148 | 147 | 153 | 166 | 251 |

Fitted against size: **148us + 6.15us/KB, r2 = 0.98.** Split at 1 KB the two regimes are explicit -
below it the slope is **-0.09us/KB with r2 = 0.00**, which is to say no size dependence whatsoever;
above it, 6.53us/KB.

The control arm sits at ~50us below 1 KB, so **the crossing itself is about 120us fixed plus
~6.5us/KB**.

## What that means for bundling

| Record size | Crossing | Fixed share | Bundle of 100 | Gain |
|---|---|---|---|---|
| 16 B | 120us | 100% | 1.3us | 92x |
| 1 KB | 126us | 95% | 7.7us | 16x |
| 4 KB | 146us | 82% | 27.2us | 5x |
| 16 KB | 224us | 54% | 105.2us | 2x |
| 64 KB | 536us | 22% | 417.2us | 1x |

**Bundling is transformative for small records and marginal for large ones**, and the crossover
where per-byte cost overtakes fixed cost is around **18 KB**.

So the answer to "50x or 5x" is: **both, depending on payload**. A note claiming one figure would
have been wrong for half the range.

## What this decides

- **Build bundling if the target workloads carry records under a few KB.** Nothing else available
  is worth 16-92x, and it needs no new toolchain.
- **Above ~16 KB, bundling is not the lever.** There the per-byte copy dominates, and that is
  exactly what an FFI transport can remove and bundling cannot: shared memory passes a pointer
  where gRPC must serialise and copy. The two levers are not interchangeable, and which one wins is
  a property of the payload rather than of the design.
- **A correction to what was said earlier in this work:** that the two levers "compose". They do
  arithmetically, but whichever is built first eats most of the other's justification on small
  records - once bundling has divided a 120us fixed cost by 100, a faster transport has almost
  nothing left to save. On large records they do not compete at all, because bundling never
  addresses the term that dominates there.

## Limits, stated rather than buried

- **Bundle-assembly cost is not measured.** The gains above are upper bounds: they assume grouping
  N records into one frame is free, and it is not. Whoever builds bundling should measure that
  before quoting these numbers.
- The above-1 KB slope rests on **three points**, so r2 = 1.00 there means "three points are
  collinear", not "the model is confirmed".
- The control arm was noisy at 1 KB and 16 KB (spreads of 24 and 36us against 0-6us elsewhere),
  which is why the slope is taken from the much cleaner treatment arm and the control used only as
  a level below 1 KB.
- Two reps, one machine, loopback gRPC, one stream thread, in-memory state store.

## Prior art

- [`perf-streams-crossing-attribution.md`](perf-streams-crossing-attribution.md) - established the
  ~150us crossing and that the engine's own marginal cost is statistically zero. This note splits
  that number.
- [`next-batching-modes-for-clients.md`](next-batching-modes-for-clients.md) - owns the batching
  and bundling definitions, and the per-record-outcome constraint that bundling must not break.
- [`../language-bindings.md`](../language-bindings.md) - the five axes; this is axis 3.
