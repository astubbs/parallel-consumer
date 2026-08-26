# Chaos Pain Suite - Phase 2+ roster

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->
<!-- inflight-state: deferred - after v6, new scenarios rather than repairs -->


- **A REAL Class 2 hunt - unexplored levers.** The old calibrated tripwire is gone: it watched a
  committed offset, which one incomplete record legitimately pins, so it could not tell a busy fleet
  from a wedged one, and it now reports instead of gating. What that leaves open is the hunt itself -
  a true unbounded Class 2 stall has still never reproduced on master, and finding one now needs a
  trigger rather than a wider bound. Unexplored levers, most promising first: sub-second commit
  intervals; EoS/transactional mode; `confluentinc#909` stale-container restart patterns. Note the
  liveness claim now rests on `INSTANCE_STALL`, whose per-shard gap is tracked in
  [`test-per-shard-liveness-has-no-gate.md`](test-per-shard-liveness-has-no-gate.md) - so a hunt that
  wedges a single shard beside busy siblings is currently invisible to every gate, which makes it the
  more interesting target, not the less.

- **KEY-ordered processing: tried as W5, and it did NOT concentrate contention into a stall.**
  `ChaosKeyOrderIT` runs the lever previously ranked first on the list above. Its calibrated shape is
  green with a 22s lag-stagnation peak against the 150s bound, so as a Class 2 trigger this lever is
  spent; it landed instead as the ordering half of the correctness ledger. What it did surface is that
  a KEY-ordered workload turns two ordinary sizing mistakes into something indistinguishable from a
  Class 2 stall - a heavy tail collapsing onto one shard, and a dwell longer than the gap between
  rebalances chaining forever - both now checked or measured in the scenario. Anyone still hunting
  Class 2 under KEY ordering should start from W5's constants, not W1's.
- **Thin margin.** W4's legit lag-stagnation peaks (117-123s) sit only ~1.25x under the 150s Class 2
  bound. Fine for a non-gating suite; widen it (shorter storm or dwell) if it ever flakes.
  **It has flaked, and the cause was measured** - see
  [`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md).
- **Revoke-event instrumentation (open).** Nothing logs actual `onPartitionsRevoked` events, so the
  ~6x revoke-drop finding is not reproducible from a run's own logs. Add a per-instance revoke counter
  to `ManagedPCInstance`'s rebalance listener and fold `revokeEvents=` into the driver's run summary.
  Then revisit the ledger's `perDisturbanceAllowance` (5000) under cooperative - with measured counts
  the tightening becomes evidence-based instead of a guess.
- **Unit-test seams (from astubbs#85's review, MOSTLY CLOSED 2026-08-25).** The seams exist now, and
  three of the four named items are covered in milliseconds without a broker:
  `disableRebalanceDwellViolation` and the "peak always measured, violation only suppressed" invariant
  through `ProgressProbe.recordRebalanceDwell` (`RebalanceDwellToggleIT`, both directions, with an
  ARMED control so the disabled case cannot pass vacuously), and the Class 2 classifier through
  `recordLagStagnation` (`Class2ObservationIT`). **Still open:** `withNoProgressWindow`, and
  `ManagedPCInstance.Config.extraConsumerProps` (null vs present, wins-last ordering).


## A breadth checklist to audit this against (2026-08-21)

[`next-formal-verification-and-correctness-methods.md`](next-formal-verification-and-correctness-methods.md)
records a competitor's published chaos matrix in full, as a checklist rather than an aspiration. The
gap it identifies first is **network-level fault injection** (Toxiproxy: packet loss, latency jitter,
payload truncation, asymmetric partitions), which this suite does not do at all. It also records two
cheaper wins that would make soak runs assert something: a per-message hash so truncation and
corruption are detected, and an end-to-end validator confirming every produced message reached either
the primary store or the dead-letter store with no gaps.
