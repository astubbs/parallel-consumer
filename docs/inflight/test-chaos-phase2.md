# Chaos Pain Suite - Phase 2+ roster

- **Class 2 RED hunt - stands as a calibrated tripwire.** A true unbounded Class 2 stall has not
  reproduced on master: a 9-seed sweep found 0 hits (stagnation peaks banded 95-112s, all
  legit-window), and the cooperative-sticky W4 variant was green on both arms (sticky drops revoke
  events ~6x, refuting the more-revokes hypothesis; eager-calibrated Class 1 bounds do not transfer to
  cooperative). GREEN-side validated on both assignors; the RED side awaits a real occurrence or a new
  trigger idea. Unexplored levers, most promising first: sub-second commit intervals;
  EoS/transactional mode; `confluentinc#909` stale-container restart patterns.
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
- **Revoke-event instrumentation (open).** Nothing logs actual `onPartitionsRevoked` events, so the
  ~6x revoke-drop finding is not reproducible from a run's own logs. Add a per-instance revoke counter
  to `ManagedPCInstance`'s rebalance listener and fold `revokeEvents=` into the driver's run summary.
  Then revisit the ledger's `perDisturbanceAllowance` (5000) under cooperative - with measured counts
  the tightening becomes evidence-based instead of a guess.
- **Unit-test seams (from astubbs#85's review, open).** ProgressProbe's per-scenario toggles
  (`disableRebalanceDwellViolation` / `withNoProgressWindow`) and the "peak always measured, violation
  only suppressed" invariant have no fast coverage - the samplers are private, so extract a seam first.
  Same for `ManagedPCInstance.Config.extraConsumerProps` (null vs present, wins-last ordering). Both
  become millisecond broker-free tests once the seams exist.
