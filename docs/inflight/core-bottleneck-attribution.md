# Bottleneck attribution: the controller's probes already answer "why am I not going faster?"

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - the classification exists inside astubbs#333; the feature is surfacing it per function, causally, and across languages -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (first review's breakdown:
[`core-engine-thesis.md`](core-engine-thesis.md)). The satellite of
[`core-per-function-capacity-arbitration.md`](core-per-function-capacity-arbitration.md) that is
useful *before* any allocator exists - and, like it, a projection of
[`core-execution-opportunity-model.md`](core-execution-opportunity-model.md).

## The claim

The adaptive controller does not merely pick a concurrency - it runs experiments, and the results
classify **why** more concurrency stops helping. Probe 64 -> 96 gains 0.3% under CPU pressure:
`LOCAL_CPU`. Probe 200 -> 300 loses 4% while callback latency rises 110 -> 171ms:
`DOWNSTREAM_SATURATION`. Backlog 800k with 11 independent keys and concurrency stuck at 11:
`ORDERING_PARALLELISM`. Three regimes, three different remedies - another process helps only the
first, cannot help the second, and cannot help the third at any price. (A fourth sits one level
up: ownership itself binding - [`core-partition-advisor.md`](core-partition-advisor.md).) The differentiator over
every observability product: **the evidence is experimental, not inferred**. The engine did not
read a CPU graph and guess that 300 would be worse; it tried 300. Across the language bindings
this compounds - the Python developer gets the experiments they would have had to run by hand,
without becoming a concurrency expert.

## What already exists, so this is scoped as surfacing rather than invention

- astubbs#333 already *recognises* the ordering regime ("where ordering starves a workload,
  admission is not the binding constraint... the controller's job there is to report") and its
  `OBSERVE` mode already reports "what concurrency the engine would pick and why it is not moving".
- The delta-vote design in [`core-auto-scaling.md`](core-auto-scaling.md) already names the three
  situations, because the vote must encode them.

The feature is promoting that internal classification to a per-function, operator-facing output
with a stable taxonomy - and then two compositions:

**Causal propagation through a topology.** When `enrich` saturates at 12k/s, everything downstream
of it also reads 12k/s, and conventional telemetry shows four slow operators. PC can tell them
apart without inference: a *limited* function has deep runnable work and a flat probe; a *starved*
one has an empty runnable queue. Naming the causal bottleneck rather than the busiest operator is
the operationally valuable half, and it needs the Streams work (astubbs#255 / astubbs#271) to have
a graph to propagate through.

**Bottleneck-directed scaling.** "Scale out specifically to create capacity for `enrich`" - and
the new instance's allocator preferentially feeds the function the instance exists for, rather
than distributing capacity evenly. That is the arbitration note's allocator plus this note's
diagnosis, composed.

**The explanation graph** is the UI: click "why are orders at 12k/s" and walk limited-by ->
regime -> "another instance predicted (not) to help". Natural home is the embedded dashboard
(astubbs#268, [`web-gui-observability-ideas.md`](web-gui-observability-ideas.md)) - it clears that
PR's own bar of "only what no external tool can show", since no broker-side tool can see probe
results.

## The design rule this implies (2026-08-30): every adaptive decision must be externally explainable

Do not let the controller output only `targetConcurrency=137` - have it retain the decision
history that produced it (`96->128: +19% throughput; 128->160: +2%; 160->192: -7%, latency +31%;
operating region ~128-150`). One retained ledger becomes autoscaling evidence, diagnostics, GUI
content, research data and promotional material simultaneously - and it is what turns the product
surface from a metrics page into a **conclusions page** ("this workload is compute constrained;
evidence; action"), which is the attribution feature's mature form. This should bind astubbs#333's
controller as it evolves: the engine's unusual asset is measurement *plus actuation* - it can
change concurrency and observe the causal response - and a decision whose evidence is discarded
wastes the experiment it ran.

## A tension to hold, not resolve silently

The conversation wants the scale-out signal to carry magnitude ("an additional instance has ~4,000
records/sec of exploitable work"). The settled dimension-2 design deliberately clamps the vote to
+1/0/-1 - bounded steps are how the fleet converges without oscillation
([`core-auto-scaling.md`](core-auto-scaling.md)). Both survive if magnitude lives in the
*diagnosis* (this note's output, for humans and for capacity planning) while the *vote* stays
clamped (for the autoscaler). Upgrading the vote itself to a magnitude would reopen the
oscillation question the clamp answered.
