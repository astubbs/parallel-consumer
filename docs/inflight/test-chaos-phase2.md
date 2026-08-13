# Chaos Pain Suite - Phase 2+ roster

- **Class 2 RED hunt - stands as a calibrated tripwire.** A true unbounded Class 2 stall has not
  reproduced on master: a 9-seed sweep found 0 hits (stagnation peaks banded 95-112s, all
  legit-window), and the cooperative-sticky W4 variant was green on both arms (sticky drops revoke
  events ~6x, refuting the more-revokes hypothesis; eager-calibrated Class 1 bounds do not transfer to
  cooperative). GREEN-side validated on both assignors; the RED side awaits a real occurrence or a new
  trigger idea. Unexplored levers, most promising first: KEY-ordered processing to concentrate commit
  contention per shard; sub-second commit intervals; EoS/transactional mode; `confluentinc#909`
  stale-container restart patterns.
- **Thin margin - it flaked, and the cause is measured.** W4's legit lag-stagnation peaks (117-123s)
  sit only ~1.25x under the 150s Class 2 bound. On 2026-08-13 the eager arm went RED on PR
  astubbs#206 ([run 31668991207](https://github.com/astubbs/parallel-consumer/actions/runs/31668991207),
  seed `3341129570805008712`): 13 of 80 partitions stagnant **154.5s**, group STABLE. It looked like
  the real occurrence the RED-side hunt wants - the frozen partitions held 94.5% of the run's
  shortfall (lag 34,686 of 36,694 unconsumed) while the rest of the fleet drained. **It is not.**
  Replaying the same seed on an uncontended 32-thread box passed: `maxLagStagnation` **121.3s**, zero
  violations, drained 252,665 >= 250,000. A real Class 2 stall is *unbounded*, so a schedule that
  drains fully on replay does not encode one; 121.3s also lands inside the known legit band.
  **What differed was the clock, not the schedule.** `Performance` and `Chaos Pain Suite` are matrix
  jobs sharing `runs-on: [self-hosted, highcpu]`, and on that run they overlapped (Performance
  05:03:41-05:06:57, Chaos 05:03:57-05:11:28) across the storm phase and the stagnation window -
  the contention `pr-highcpu-fast-feedback.yml`'s own header already warns about ("its timing is
  noisy when it shares the box with chaos"). Same seed, ~27% inflation, 121s -> 154s, over a bound
  whose modelled worst legit case is only 100s.
  So the widen-it advice is now evidence-backed rather than a guess: **the legit distribution reaches
  154s on a shared runner.** Two candidate fixes, not yet applied - widen `LAG_STAGNATION_BOUND` (or
  shorten the storm/dwell), or stop the two jobs sharing the box so chaos measures what it thinks it
  measures. Prefer the second: widening hides the contention instead of removing it, and the bound is
  the tripwire's whole sensitivity.
- **Chaos results are provisional until the production fix backlog lands.** The suite currently runs
  against a `master` missing a stack of merged-nowhere fixes in exactly its blast radius - rebalance,
  offsets, commit path and executor lifecycle: astubbs#29 (paused consumption after rebalance with
  multiple consumers), astubbs#31 (stale container at a reused offset after rebalance), astubbs#296 /
  astubbs#209 (work submitted to an already-closed worker executor), astubbs#267 (concurrent listener
  registration silently lost), astubbs#204 (poll thread's real error masked by the commit-response
  timeout), astubbs#257 and astubbs#261 (produce-lock double release; partial result set after a
  terminally failed send). **Sequence the hunt accordingly:** land those first, then re-run the
  suite. A RED observed before they land is odds-on one of these *known* defects rather than a new
  Class 2 finding, so investigating it as an unknown burns effort on an already-solved bug. This also
  qualifies the "0 hits in a 9-seed sweep" result above - that sweep ran against the same incomplete
  tree.
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
