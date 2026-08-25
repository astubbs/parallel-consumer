# Handoff: the adaptive control law is verified green - what a continuation session needs

Written 2026-08-25 as a deliberate handoff point: the owner's flagship-model budget for the week is
nearly exhausted, and this is the natural stopping point - the law rewrite (astubbs#227,
confluentinc#21) has just gone fully green on the broker. A continuation session on any system
starts here.

## Where the work lives

- **Branch `feats/ideate-distributed-throttling`, PR astubbs#333**, base `perf/engine-concurrency`,
  worktree `.claude/worktrees/throttling-ideation`.
- The design artifact is `docs/plans/2026-08-24-003-feat-admission-control-law-design.md`
  (implementation-ready, twice-reviewed). Research: the 005/006 docs beside it. The pressure-signal
  API (phase 4's dependency) is the 004 plan.
- `docs/inflight/pr-333-adaptive-concurrency-outstanding.md` tracks what the PR leaves open,
  including the owner's merge-prep directive: **this PR gets a Codex cross-model review**.

## State at handoff

**Everything through law-U13 is implemented and verified.** The final verification (2026-08-25):
falsifier suite green with all mutants failing; `AdaptiveConcurrencyEnforceIT` and
`AdaptiveConcurrencyClosedLoopIT` green post-cadence-change; `AdaptiveConcurrencyComparisonIT`
green on **all strict phases** - the adaptive arm beat the hand-tuned static on degrade (p95
34.3s vs 39.9s, +34% completions), recovery (12,863 vs 11,113 completions), and the final phase.
Per-phase tables and trajectories are recorded **in the test javadocs themselves** (the born-red
histories retained under History headings) - not in any scratchpad.

The three defects the broker falsified and the law absorbed, in order: the verdict-starvation
freeze (U12: window-aggregated binding, `getOccupied()`, absorbing-warmup escape), FALL contraction
cadence (U12b: cut every adjudicated window, stopped by the marginal cross-level pair), and the
absorbing park (U13: the recovery re-ask probe - timer-gated because below-knee recovery is
provably unobservable, drift as accelerator only; constants carry their derivations in javadoc).

## What remains, in order

1. **law-U11 - records sweep** (task was pending at handoff): close the pr-333 inflight items the
   design answered (-1/1/2/3 and item 0), roadmap stage, the `admission-gradient2-port` tag note
   reference, and the operator-visible KTD7 statement in the feature docs ("the default brake is
   the throughput plateau band, not any latency bound").
2. **law-U14 - probe logging on its own channel** (owner-ratified 2026-08-25): second SLF4J logger
   `<AdmissionController>.probe`; both channels INFO; kept probes (real movements) log on the MAIN
   channel; probe lifecycle (excursions, restores, backoff) on the probe channel; operators filter
   by channel, never by level. Operator doc gets the one-line "how to silence probe chatter".
3. **Comparison IT phase 4** stays skipped-blocked on the 004 pressure-signal plan.
4. **U10 bench arm** on its own branch (pr-333 note item 4) - post-plan work.
5. **Merge prep** (when the PR is ready): `docs/merge-checklist.md`, the ce-compound step, the
   Codex review reminder above, and the defect-class sweep.

## Owner rulings this session (do not re-litigate)

- **Recovery re-ask over verdict-staleness decay** - conclusions are re-tested, never expired;
  decay is shelved unless a failing scenario the timer cannot cover appears.
- **Model tiering** (recorded in agent memory, model-generic): the scarce flagship thinks
  (planning/review/coding); the cheap tier does mechanics (test runs, monitoring, reporting).
  Waiting costs nothing; active turns are the spend. **Prompt the owner to check flagship usage
  regularly**; hand off across systems when it nears the cap.
- **Plain language**: the owner is not from this domain; jargon must be explained inline. Given
  plain explanations they co-design effectively (they independently re-derived the re-ask probe).
- **Share Groups positioning**: adaptive concurrency composes with Share Groups (delivery is not
  processing); now in `STRATEGY.md` and owned by
  `docs/inflight/next-what-survives-share-groups.md`.
- **Provenance story** (Gradient2 convicted → clean-sheet; BBR/RFC 7661 lineage) is owner-approved
  promotional copy, held in `docs/inflight/docs-adaptive-trust-pack.md`.

## Traps a continuation session must know

- **JDK**: use the PATH java (17). Never `/usr/libexec/java_home` (returns 26; delombok crashes).
- **Stale reports**: never trust a failsafe report whose mtime predates the run's exit.
- **Worker detach**: agents monitoring long maven runs stop silently; arm a `kill -0` process
  watcher (bash, not fish) on the maven pid and find its log via `lsof -p <pid>` (fd 1w).
- The falsifier discipline is the workflow: no law change without a deterministic scenario that is
  red before and green after, and mutants that fail it.
