# Per-function capacity arbitration: the scaling unit is the function, not the application

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - needs multi-function instances and the dimension-1 controller; endpoint of the auto-scaling track, not its entry -->

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

External scaling becomes last-resort and evidence-based: ask for an instance only when a function
has exhausted what internal reallocation can give it AND still has profitable parallelism. That is
not a new signal - it is the strongest form of the existing +1 delta vote, reached through one more
layer of "tried everything cheaper first". The endpoint claim, candidate thesis material: a Kafka
application stops having a meaningful configured size - partition count, thread count and replica
count all become implementation constraints around an engine discovering where useful work exists.

## The caveat recorded with it, so nobody builds the wrong half first

**"The process has room for ~500 useful concurrent operations" is a fiction until something
concrete bounds it.** With virtual threads, threads are not the scarce resource. What is actually
arbitrable: CPU (the JVM scheduler already arbitrates it), memory holding in-flight records
(real, and PC-controlled), fetch bandwidth of a shared consumer (real). Functions that are IO-bound
against *distinct* downstreams barely contend - each hill-climber finds its own ceiling and there
is nothing to arbitrate. So the entry shape is per-function dimension-1 controllers plus
shared-resource *ceilings* (min-composition - ideation idea 5/8, same seam as
[`core-distributed-throttling.md`](core-distributed-throttling.md)); the marginal-benefit
scheduler is where the track ends up if the ceilings prove insufficient, not where it starts.

## Prerequisites, promoted from earmark to load-bearing

The many-functions process does not exist in PC today - one instance, one function. This idea
stands on astubbs#254 (per-topic processing functions, confluentinc#372) and astubbs#245 (runtime
subscription change), which [`core-auto-scaling.md`](core-auto-scaling.md) carries only as
earmarks; the Streams flavour additionally stands on astubbs#255. If those never land, this note
reduces to "dimension 1, run once".

## Split rationale

Separate note, not a third dimension in [`core-auto-scaling.md`](core-auto-scaling.md), by that
note's own precedent: split when the pieces reach mergeable PRs at different times. Dimensions 1
and 2 need nothing here; this needs both of them plus the multi-function work.
