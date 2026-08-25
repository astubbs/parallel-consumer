# Handoff: session two of the adaptive-concurrency continuation - built, tortured, trained

Written 2026-08-25 at the owner's request; the continuation session runs on Opus (flagship budget),
so keep the tiering ruling: heavy judgment stays lean, mechanics go to cheaper subagents, and prompt
the owner to check usage periodically. The predecessor handoff -
[`2026-08-25-001-handoff-adaptive-law-continuation.md`](2026-08-25-001-handoff-adaptive-law-continuation.md) -
described the law as verified-and-pushed; this session consumed that list whole and grew the work
three ways: the PoC is demonstrated, the law is tortured, and the merge path exists as a PR train.

## Where the work lives

- **Branch `feats/ideate-distributed-throttling`, PR astubbs#333**, worktree
  `.claude/worktrees/throttling-ideation`, base `perf/engine-concurrency`. Everything this session
  produced is pushed; the working tree is clean.
- **The merge path is the perf train**: eight PRs to master, all cut, all open, listed with
  human-readable titles at the TOP of astubbs#333's body (`depends on ...` lines the dependency
  gate reads; keep the list current as wagons move). The wagon ledger is
  [`../inflight/branch-engine-concurrency-pr-stack.md`](../inflight/branch-engine-concurrency-pr-stack.md);
  a DIFFERENT Claude session drives the train - coordinate through the ledger, never collide.
  astubbs#333 retargets onto master when astubbs#363 (the integration branch's own PR) lands.
- **astubbs#353 (the pre-commit-hook fix, this session's side product) is merge-ready and idle.** Its
  babysitter subagent finished: head `c73892ea0`, `CLEAN` merge state, every required check green
  including `claude-review` and the human LGTM, zero open threads, base current with master. **Merge
  is the owner's call and was deliberately not taken.** Worktree `.claude/worktrees/hook-commit-guard`
  is now unheld. Two environment notes it surfaced: the automated-review runner's workspace carries a
  persistent uncommitted edit *reverting* this PR's fix to `.claude/hooks/pre-commit-gate.sh` (our
  worktree is clean - it is runner residue that could be committed by accident from that checkout),
  and shellcheck is absent on this box, so the new `check-shell-lint.sh` gate only runs in CI.

## What landed this session (all pushed; `git log 55a73aeef..` has the bodies)

1. **CI triage of the 49-commit batch**: the virtual-threads lane's five reds were test-side
   ThreadPoolExecutor assumptions (pins + one visible skip; verified both engines); the vertx IT
   died on the core test-jar's undeclared threeten-extra (hoisted to parent dependencyManagement);
   plus dependabot coverage for the Go bench dirs, docs-data schema, quarantine-registry drift, and
   the issue-ref gate's character-entity false positives - each fixed at the owning layer.
2. **law-U11 records sweep and law-U14 probe channel**: the pr-333 note shrank to what is open
   (Codex review directive + the U10 measurement); KTD7's operator statement is in
   `docs/features/adaptive-concurrency.yaml`; the `admission-gradient2-port` tag exists and is
   pushed; probe lifecycle logs on `AdmissionController.probe` (both channels INFO, filter by
   NAME never level; falsifier-first, channel asserted by logger name).
3. **The demos** (`AdaptiveConcurrencyDemo`, three plants, `-Dpc.demo.arms` selection; the classic
   `Demo`'s documented run command fixed): hard knee - settles at 33 vs truth 32, over-provisioned
   arm pays 62.7ms vs 20.3ms for equal throughput; the owner's CPU plant - static-50 pays exactly
   the 50/32 oversubscription, adaptive turns around past the plateau; the moving world - the
   guess lottery (10/48/100) where every static number loses somewhere and adaptive walks 57->8->46
   tracking a 48->8->48 outage. **Owner-ratified promo copy** ("the only configuration that changes
   when the world does") is recorded in
   [`../inflight/docs-adaptive-trust-pack.md`](../inflight/docs-adaptive-trust-pack.md);
   publication stays gated on the real-hardware bench number.
4. **The soak/torture plan, reviewed then executed**
   ([`2026-08-25-002-test-adaptive-soak-torture-plan.md`](2026-08-25-002-test-adaptive-soak-torture-plan.md)):
   a four-reviewer ce-doc-review pass restructured it (the simulated-horizon insight, coverage with
   interaction teeth, the no-cron trigger, two honest open questions), then U1/U2/U3/U7/U4 landed -
   plant library with capacity schedules as data, derived-bounds-only invariant kit with sabotage
   controls, the 14-scenario matrix, twelve simulated hours through the REAL controller in 0.9s,
   and the torture set. **109 simulated tests green in ~2s.** U5 (real-broker soak workflow) and
   the engine-attached torture slice (chaos-conductor gaps) are deliberately NOT built - the plan
   carries them.
5. **Findings from trying to break it** (all pinned or recorded, none loosened): the settled band's
   second-order term at wide scale (steps taken at band top - the invariant kit derives it now);
   resonance does NOT ratchet (identical envelope at 40/200/800 cycles, steady state tighter than
   the early transient; at flicker faster than the settle cadence the law degrades CONSERVATIVELY);
   the second-wind pin - first-knee-seeking is designed, priced, and any future explorer must flip
   `AdmissionTortureTest.secondWindBeyondTheValleyIsNotCrossedUnaided` consciously.
6. **The gate-lane pass**: copyright table parsers skip in-table comments (self-test green);
   inflight-tags gate honours its own documented `clients/` exception; the 94-note tag-vocabulary
   corpus sweep (211 notes valid, zero findings); TODO index regenerated; demo arm-builder
   deduplicated. **jscpd is deliberately left at merge-prep judgment** - CPD (code-aware) passes;
   jscpd's excess is import blocks, shared IT prose, and `EngineParityTestBase`'s RECORDED
   keep-scenarios-duplicated decision, which is not ours to silently override.

## Owner rulings this session (do not re-litigate)

- **The mode ladder** (in [`../inflight/core-adaptive-concurrency-future-modes.md`](../inflight/core-adaptive-concurrency-future-modes.md)):
  latency-ceiling clamp first, catch-up as a CLAUSE of the clamp (self-contradictory without it -
  the owner's own re-derivation), rate-limit feedback after. **Pacing profiles parked** with the
  refuting experiment on the record (doubling the shared step destabilised every phase);
  **exploration probing recorded** beside it (far-step pin/measure/restore, a mode never the
  default, second-wind pin as its born-red acceptance test).
- The parity suites' duplication is deliberate (the base class's javadoc records it); jscpd
  judgment happens at merge prep with the owner.
- Depends-on lists carry short human-readable titles and sit at the TOP of PR bodies.
- astubbs#359's residence-time metric is the natural input for the future latency clamp - noted,
  not yet written into the ladder.

## What remains, in order

1. **Triage the automated review on astubbs#333** - requested on the current head; findings may be
   waiting. In-thread replies, resolve threads, per AGENTS.md.
2. **When astubbs#354 merges to master**: merge master into this branch; the chaos lane's two
   revoke-IT reds are that PR's timing-proxy class and should clear.
3. **Ride the train**: keep astubbs#333's depends-on list current; when astubbs#363 lands, retarget
   astubbs#333 to master and re-check its diff.
4. **Merge prep for astubbs#333** (docs/merge-checklist.md): the owner-directed **Codex cross-model
   review**, the jscpd judgment, the admission-test scaffolding dedup cluster (ClockedModule /
   tearDown / registerWork copies across ~6 admission test classes - flagged, unconsolidated), the
   re-cut offer (~85 commits), description re-check, human LGTM.
5. **Post-merge work, already planned**: U5 soak workflow (own PR, push-to-master trigger, timeout
   above 6h); engine-attached torture (the three chaos-conductor harness gaps are scoped in the
   plan's U4); U10 bench arm on real hardware (gates any published claim); then the mode ladder.

## Session traps (each cost real time today)

- **The pre-commit hook misfires on complex Bash commands** when the MAIN checkout is stale - the
  fix is on master (and astubbs#353 fixes the deeper parser bug); until the main checkout is
  pulled, avoid `if`/`for` in Bash tool commands or expect spurious gate runs.
- **`FILE_REFS_BASE=origin/perf/engine-concurrency` on every commit in this worktree** - the
  file-refs gate defaults to origin/master and reports ~97 inherited findings that are NOT this
  branch's; the env override scopes it to the true base. Same for `bin/check-issue-refs.sh
  origin/perf/engine-concurrency` when checking by hand.
- **The Truth assertion generator scans test-source ENUMS** and generates imports that cannot see
  package-private holders - use string-constant vocabularies in test helpers (ScenarioMatrix is
  the worked example), and purge stale generated Subjects from target/ after removing an enum.
- **Java 9+ APIs (`List.of`) are invisible at the release-8 target**; records need `@Desugar`.
  Test logging defaults to WARN (`-Dpc.log.level=info` to see INFO). Surefire's `.txt` reports
  show 0 tests for `@Nested` classes - read the XML. Single-module maven runs need `-am`.
- **`ControllerAdmissionPolicy` maps capacity-override phases to REBALANCES by default** - torture
  scenarios modelling a moving downstream must pass the third constructor arg `false`, or fake
  rebalances pollute the trajectory (this masked most of the first resonance red).
- The falsifier discipline is the workflow: no law change without a deterministic red-before /
  green-after scenario plus failing mutants; never loosen a scenario to green - report, shrink,
  fix.
