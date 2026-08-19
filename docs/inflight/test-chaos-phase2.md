# Chaos Pain Suite - Phase 2+ roster

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->


- **Class 2 RED hunt - stands as a calibrated tripwire.** A true unbounded Class 2 stall has not
  reproduced on master: a 9-seed sweep found 0 hits (stagnation peaks banded 95-112s, all
  legit-window), and the cooperative-sticky W4 variant was green on both arms (sticky drops revoke
  events ~6x, refuting the more-revokes hypothesis; eager-calibrated Class 1 bounds do not transfer to
  cooperative). GREEN-side validated on both assignors; the RED side awaits a real occurrence or a new
  trigger idea. Unexplored levers, most promising first: KEY-ordered processing to concentrate commit
  contention per shard; sub-second commit intervals; EoS/transactional mode; `confluentinc#909`
  stale-container restart patterns.
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
