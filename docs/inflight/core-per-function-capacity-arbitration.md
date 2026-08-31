# Per-function capacity arbitration: the scaling unit is the function, not the application

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - needs multi-function instances; dimension 1 is astubbs#333, assumed merging soon -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (the first review's
breakdown is [`core-engine-thesis.md`](core-engine-thesis.md); the controller this extends is
[`core-auto-scaling.md`](core-auto-scaling.md)).

## The claim

Once one process hosts many processing functions - several topics, several handlers, or a Streams
topology's processors - "how many replicas" stops being the first scaling question. Today the pod
is the scaling unit, so one hot subscription duplicates every cold consumer, idle cache and unused
client in the process to get more capacity for itself. The proposal inserts a control loop below
the two [`core-auto-scaling.md`](core-auto-scaling.md) already stages:

```
function   discovers its own useful concurrency          (dimension 1, run per function)
process    arbitrates shared capacity across functions   (THIS NOTE - reallocate before you replicate)
fleet      delta-vote instance recommendation            (dimension 2)
```

The allocator's question, in the conversation's sharpest form: *where does the next unit of
concurrency produce the greatest marginal benefit?* Give 100 spare operations to a
downstream-saturated function and get nothing; to a CPU-saturated one and get worse; to a
key-starved one and get nothing; to the one function with headroom and get +30%. An economic
allocator inside the process, fed by the regimes the controller already classifies
([`core-bottleneck-attribution.md`](core-bottleneck-attribution.md)).

External scaling becomes last-resort and evidence-based: **scale-out is the consequence of failing
to satisfy profitable internal demand**, and scale-in of every function's marginal return
collapsing. That is not a new signal - it is the strongest form of the existing +1 delta vote,
reached through one more layer of "tried everything cheaper first". The endpoint claim, candidate
thesis material: a Kafka application stops having a meaningful configured size, and "Kafka
application" stops being a scheduling concept at all - the scheduling entities are functions and
their ordering domains, while the process is merely where some of them execute, the pod a capacity
envelope, the partition an ownership boundary, and the language where the user's function happens
to be written. PC decoupled ordering from partitions; this track decouples execution from
everything Kafka traditionally used as a proxy for it.

## The budget is measured, not configured (corrected 2026-08-31)

The first draft of this note called the "process has room for ~500 useful concurrent operations"
pool a fiction until something concrete bounded it. Wrong, per the owner's correction: the number
is the output of the dimension-1 controller, which astubbs#333 implements (assume it and the
`perf/engine-concurrency` stack under it merge soon). That controller discovers an **admission
target** - records allowed in flight, measured from service time, outcome signals and sustained
achieved in-flight, thread pool fixed at the ceiling - and admission is the right variable for
arbitration too: per-function admission sub-targets decompose a process-wide target naturally, and
admission behaves identically on platform and virtual threads, which is that PR's own argument for
choosing it.

What survives of the caveat: the arbiter only bites when the sum of per-function profitable
concurrency exceeds the discovered process-wide target. Functions IO-bound on distinct downstreams
can each sit at their own dimension-1 answer with the sum still under the process ceiling - then
there is nothing to arbitrate, and the right amount of scheduler is none. astubbs#333 also already
names the edge this layer inherits per function: where ordering starves a workload, admission is
not the binding constraint, and the controller reports rather than adapting against a constraint
it cannot move.

## Prerequisites, promoted from earmark to load-bearing

The many-functions process does not exist in PC's own API today - one instance, one function. The
plain-consumer route stands on astubbs#254 (per-topic processing functions, confluentinc#372) and
astubbs#245 (runtime subscription change), which [`core-auto-scaling.md`](core-auto-scaling.md)
carries only as earmarks. But the *nearest existing* multi-function process is a Kafka Streams
topology under astubbs#255 / astubbs#271 - its operators are exactly the many functions with
radically different concurrency->useful-work curves, so the Streams work is the likelier first
host for this layer, not the later one. Dimension 1 itself is no longer
a blocker - astubbs#333 implements it. If the multi-function work never lands, this note reduces
to "dimension 1, run once".

## Split rationale

Separate note, not a third dimension in [`core-auto-scaling.md`](core-auto-scaling.md), by that
note's own precedent: split when the pieces reach mergeable PRs at different times. Dimensions 1
and 2 need nothing here; this needs both of them plus the multi-function work.
