# The three-reveal demo: one partition, and the architecture demonstrates itself

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - gated on the adaptive controller; each later reveal on its own workstream -->

From the Codex strategy review of 2026-08-22/23 (breakdown in
[`core-engine-thesis.md`](core-engine-thesis.md)). Candidate centrepiece demo for whatever comes
after v6 - a demonstration of the architecture, not a microbenchmark.

## The setup

Deliberately hostile topology: **1 partition, 10,000 keys, ~100ms handler**. A "1 partition" label
sits in the corner of the screen the entire time - it communicates more than the numbers do.

## The run

1. Adaptive concurrency on, **no configured answer**. Watch it climb 1 -> 2 -> 4 -> ... until the
   downstream simulator degrades, then settle at the sustainable point.
2. **Change downstream capacity while it runs.** Watch it adapt.
3. Add load past what internal scaling can absorb. Watch the external recommendation flip
   `HOLD -> SCALE_OUT` ([`core-auto-scaling.md`](core-auto-scaling.md) dimension 2).

## The reveals, in order

1. *One Kafka partition. Thousands of keys. Hundreds of concurrent operations. Per-key ordering.
   No concurrency setting.*
2. *And that's Kafka Streams.* (astubbs#255)
3. *And the application is written in Python.* (astubbs#242 / astubbs#334)

Each reveal degrades gracefully: reveal 1 alone is a complete demo the day the adaptive controller
works; 2 and 3 attach when their workstreams deliver. Vanilla Kafka Streams on the same topology
has exactly one execution lane, which is the comparison frame.

## Relationship to existing demo work

Not the same artefact as the uber demo (astubbs#332: eleven language clients, one workload -
breadth) or the per-language demos (astubbs#331). This is depth: one topology, one climbing line
on a chart. The web GUI ([`web-gui-observability-ideas.md`](web-gui-observability-ideas.md),
astubbs#268) is the natural view for it, and the downstream simulator overlaps the "deliberately
made Kafka slow" harness ideas in [`docs-content-series.md`](docs-content-series.md).
