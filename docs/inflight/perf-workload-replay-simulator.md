# Workload replay: capacity planning against the user's own production trace

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - a consumer of the bench harness (astubbs#362) plus a trace format; no payloads required -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)).

Record a compact, sanitised trace of what PC observed - timestamp, topic/partition, key *hash*,
handler duration, outcome, retry delay - and replay the *scheduling problem* offline without
replaying the business operation. Then the what-ifs become runnable: 4 partitions instead of 32;
UNORDERED instead of KEY; downstream 30% slower; twice the compute; what would the adaptive
controller have selected. The bench harness (astubbs#362) already knows how to drive arms from a
work model; this feeds it a *recorded* model instead of a synthetic one, turning the measurement
campaign's tooling into a capacity-planning simulator grounded in someone's real workload -
**with the bound the GitHub Codex review, 2026-08-31 added: recorded durations and outcomes are products of the
original run's concurrency, caches, downstream saturation and retry load, not invariant service
demands, so replaying them under different topology removes the feedback loop the counterfactual
asks about. Trace-only replays are open-loop scheduling estimates (ordering availability, key
structure) and must be labelled as such; capacity conclusions need a service/resource model with
uncertainty, or measurements at multiple real operating points.** No
payloads in the trace - key hashes and timings answer most of the questions, which is also what
makes the trace shareable in a support thread or a
[`docs-research-program.md`](docs-research-program.md) publication.
