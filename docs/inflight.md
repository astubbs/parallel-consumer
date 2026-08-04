# In-flight & parked work

> Shared, cross-branch working notes kept on `master` so any branch or session can see what is in
> progress or parked. **Not** an issue tracker and **not** a backlog.
> Last verified against the repo, GitHub and the working tree: **2026-08-04**, master `6d390813`.
>
> **Scope rule: only work that is in flight, plus the context needed to resume it.** No
> completed-work narratives, root-cause write-ups or policy text - when work lands, **delete** its
> entry. The durable records live elsewhere: `CHANGELOG.adoc` (what shipped), PR bodies and commit
> messages (history), [`docs/solutions/`](solutions/) (lessons), [`docs/refactoring.md`](refactoring.md)
> (deferred internal work), [`docs/QUARANTINED_TESTS.md`](QUARANTINED_TESTS.md) (quarantine roster),
> [`docs/TODO_INDEX.md`](TODO_INDEX.md) (code markers), and
> [`src/docs/development/upstream-map.yaml`](../src/docs/development/upstream-map.yaml) (the source of
> truth for fork↔upstream mapping - record mappings there, not here).
>
> **Reference convention** (same as `CHANGELOG.adoc` and `refactoring.md`): a bare `#NN` is **this
> fork** (astubbs/parallel-consumer); an upstream reference is written **`upstream #NN`**
> (confluentinc/parallel-consumer). Fork branch names encode the *upstream* number
> (`bugs/857-...`, `fix/909-...`, `upstream-pr-905`), so a number seen in a branch name is upstream,
> not a fork issue.

## Open fork PRs

| PR | Branch (worktree) | vs master | State |
|----|-------------------|-----------|-------|
| [#80](https://github.com/astubbs/parallel-consumer/pull/80) | `fix/flaky-partitionstate-committedoffset-it` (`fix-flaky-partitionstate`) | 38 ahead, 0 behind | **Ready for review** - the big one, see below |
| [#57](https://github.com/astubbs/parallel-consumer/pull/57) | `fix/859-metrics-leak-plus-cherrypicks` (`dev-cc`) | 28 ahead, 11 behind | Draft; **needs a rebase** (was clean on 2026-07-31) |
| [#111](https://github.com/astubbs/parallel-consumer/pull/111) | `docs/mutation-testing-plan` (`mutation-plan`) | 7 ahead, 0 behind | Draft - why mutation testing under-delivers here |
| [#106](https://github.com/astubbs/parallel-consumer/pull/106) | `perf/sparse-offset-encoding` (`sparse-encoding`) | 1 ahead, 10 behind | Draft - stop walking every offset for distance-based encoders |
| [#105](https://github.com/astubbs/parallel-consumer/pull/105) | `optimize/unit-gate` (`optimize-unit-gate`) | 5 ahead, 11 behind | Draft - pack the unit-test fork tail (slowest classes first) |
| [#53](https://github.com/astubbs/parallel-consumer/pull/53) | `feat/java-17-baseline` | 1 ahead, 57 behind | Draft, stale - Java baseline + Kafka 4, see below |
| [#51](https://github.com/astubbs/parallel-consumer/pull/51) | `features/enable-virtual-threads` (origin only) | - | Open, untouched since 2026-07-28; edits `PCMetrics.java`, so it collides with #57 |
| [#31](https://github.com/astubbs/parallel-consumer/pull/31) | `fix/909-stale-container-replacement` | 2 ahead, 57 behind | Draft, **base is `master-confluent`** - retarget to `master` before it can merge |
| [#29](https://github.com/astubbs/parallel-consumer/pull/29) | `bugs/857-paused-consumption-multi-consumers-bug` | 3 ahead, 57 behind | Draft, **base is `master-confluent`** - same retarget needed |
| [#38](https://github.com/astubbs/parallel-consumer/pull/38) | dependabot: junit 5.10.2 → 6.1.2 | - | **Blocked** - JUnit 6 needs Java 17 *and* an ArchUnit engine (see deps below) |
| [#99](https://github.com/astubbs/parallel-consumer/pull/99) | dependabot: exec-maven-plugin | - | Routine |
| [#1](https://github.com/astubbs/parallel-consumer/pull/1), [#8](https://github.com/astubbs/parallel-consumer/pull/8) | `codeql`, `features/retry-dlq` | - | Ancient drafts (2022/2026-04). Close or finish - #8 is the DLQ skeleton |

### #80 - drain-zombie silent stall (upstream #857 family)

Two independent bugs found under one flaky check. **Product:** `ConsumerManager.shutdownRequested`
shadowed `BrokerPollSystem.runState`, so `consumer.poll()` was never called during drain - a ~10kHz
busy-spin plus a rebalance-unresponsive member holding its whole assignment until eviction. Fixed by
deleting the duplicated flag and deriving "closing" from the poll system's lifecycle. **Test
harness:** the `auto.offset.reset=latest` nudge race, fixed with a shared `awaitWithTopicNudge`
(no timeout enlarged, no assertion weakened). Both #80-owned quarantined tests are re-enabled on the
branch, so merging it empties the quarantine registry - which is what unblocks the 0.6.0.0 release.
Validation: 20 consecutive fork16 stress runs, 0/20 failures (historical rate ~33%). Write-ups:
`docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md` and
`latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md`.

**Note:** this makes the older `bugs/857-...` deadlock work (#29, `synchronized(commitCommand)` →
`ReentrantLock.tryLock()`) a *sibling* fix, not the same one. #29/#31 were verified not to fix the
drain defect; the uber-branch experiment (below) showed the #80 stack composes cleanly with both.

### #57 - PCMetrics leak (upstream #859) + cherry-picks

Fixes duplicate Micrometer meter re-registration on assignment/revocation, and bundles the
`upstream #893` (offset accuracy on assignment) and `upstream #905` (max-queued-records-per-shard
metric) cherry-picks into one PR instead of a 3-deep stack. Owns `PCMetrics.java`,
`PCMetricsDef.java`, `PartitionState.java`, `PartitionStateManager.java`, `ShardManager.java` - so
#51 and anything touching partition state should sequence after it. Now 11 commits behind master:
rebase before review.

### #53 - 0.7.x: Java baseline + Kafka 4

**The only reason to move off Java 8 is Kafka 4.** kafka-clients 4.x needs **Java 11**, so that is
the target baseline ("don't be stricter than Kafka"). Jabel is what lets `javac` accept Java 17
syntax while emitting Java 8 bytecode; the branch currently holds a provisional state (Jabel removed,
`release=17`) plus the Kafka 4 research docs. Approaches, decided when the work actually starts:
**keep Jabel at `--release 11`** (zero source refactor - currently breaks Lombok `@StandardException`
generation with 25 errors; unproven whether a Lombok bump fixes it - *try this first*); **remove
Jabel and rewrite** the Java 14+ syntax in ~9 core files incl. the offset-encoding hot path; or
**native Java 17** (dispreferred - drops Java 11-16 users).

Remaining units (plan on the branch, `docs/plans/2026-04-23-001-feat-apache-kafka-4-support-plan.md`):
bump `kafka.version` 3.9.1 → 4.2.x + the TestContainers CP image; migrate removed APIs
(`sendOffsetsToTransaction(Map,String)`, `MockConsumer(OffsetResetStrategy)`,
`new ConsumerGroupMetadata(String)`); downstream module audit; flip `test-kafka-compat` to a blocking
3.9.1 regression check; docs. Deferred further: `parallel-consumer-share` (KIP-932).

## Branches with no PR

- **`bugs/912-vertx-stream-memory-leak`** - clears the JStream deque on close (`upstream #912`,
  production leak) + `JStreamMemoryLeakTest912`. Committed and pushed, vertx-module only, isolated
  from core. 57 behind master. **Ready to resume: rebase → open PR.** The best low-risk parallel pick.
- **`experiment/stall-uber-fix` / `experiment/stall-uber-nofix` / `docs/uber-stall-experiment-results`**
  (worktrees `uber-fix`, `uber-nofix`, `results-doc`) - the composition experiment behind #80: does the
  full stall-fix stack compose with #29 + #31? Answer recorded on the results branch (yes; all guards
  green, zero conflicts). 19 ahead / 30 behind, no PR. Either fold the results doc into #80 or drop it.
- **`debug/committedoffset-firstpoll-stall`**, **`debug/chaos-w4-red-commit-response-stall`** -
  diagnostic-only branches; both investigations have since landed (#80 and #100). Delete once their
  findings are confirmed captured in `docs/solutions/`.
- **`docs/inflight-as-directory`** - parked idea: split this ledger into a directory of files instead
  of one growing document. 1 ahead. Worth revisiting if this file bloats again.
- **`astubbs/orca`** (worktree `/Users/astubbs/orca/...`) - 9 ahead, **66 behind**. CI/tooling
  (Claude review + PR-assistant workflows, PR-dependency check, CI matrix tweaks); master has since
  grown its own versions of most of it. Rebase and salvage what is still novel, or abandon.
- **Superseded / stale, safe to prune:** `cherry-pick/893-offset-reset`, `cherry-pick/905-max-shard-metric`,
  `upstream-pr-893`, `upstream-pr-905`, `pr-909-temp`, `bugs/859-pcmetrics-leak-v2` (all folded into
  #57); `refactor/test-hardening`, `ci/reenable-parallel-tests`, `backup/*`; `dev-cc` and
  `master-confluent` pinned at pre-rebrand `7f290122` (`master-confluent` is still a ruleset-protected
  branch and the base of #29/#31 - retarget those before touching it).

## Release 0.6.0.0

Not yet released: pom is `0.6.0.0-SNAPSHOT`, no `v0.6.0.0` tag, changelog section written. Release =
strip `-SNAPSHOT` and merge to `master`; `publish.yml` runs after CI succeeds, deploys via the
`maven-central` profile, tags `v<version>` and cuts a GitHub release (AGENTS.md → *Releasing*).
**Blocker: `release.yml` refuses to release while any test is quarantined** - see below.

## Quarantine lane

Registry: [`docs/QUARANTINED_TESTS.md`](QUARANTINED_TESTS.md), CI-enforced against the `@Quarantined`
annotations. **Two entries on master, both owned by [#80](https://github.com/astubbs/parallel-consumer/pull/80)**:
`ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger` (the drain-zombie the scenario was built to
detect) and `PartitionStateCommittedOffsetIT.committedOffsetRemoved` (the `[latest]` nudge race).
Both annotations and entries are already deleted on #80's branch, so merging it empties the registry
and unblocks the release.

## Chaos Pain Suite - Phase 2+

- **Class 2 RED hunt - stands as a calibrated tripwire.** A true unbounded Class 2 stall has not
  reproduced on master: a 9-seed sweep found 0 hits (stagnation peaks banded 95-112s, all
  legit-window), and the cooperative-sticky W4 variant was green on both arms (sticky drops revoke
  events ~6x, refuting the more-revokes hypothesis; eager-calibrated Class 1 bounds do not transfer to
  cooperative). GREEN-side validated on both assignors; RED side awaits a real occurrence or a new
  trigger idea - unexplored levers, most promising first: KEY-ordered processing to concentrate commit
  contention per shard; sub-second commit intervals; EoS/transactional mode; `upstream #909` stale-container
  restart patterns.
- **Thin margin:** W4's legit lag-stagnation peaks (117-123s) sit only ~1.25x under the 150s Class 2
  bound. Fine for a non-gating suite; widen (shorter storm or dwell) if it flakes.
- **Revoke-event instrumentation (open).** Nothing logs actual `onPartitionsRevoked` events, so the
  ~6x revoke-drop finding is not reproducible from a run's own logs. Add a per-instance revoke counter
  to `ManagedPCInstance`'s rebalance listener and fold `revokeEvents=` into the driver's run summary.
  Then revisit the ledger's `perDisturbanceAllowance` (5000) under cooperative - with measured counts
  the tightening becomes evidence-based instead of a guess.
- **Unit-test seams (from #85's review, open).** ProgressProbe's per-scenario toggles
  (`disableRebalanceDwellViolation` / `withNoProgressWindow`) and the "peak always measured, violation
  only suppressed" invariant have no fast coverage - the samplers are private, so extract a seam first.
  Same for `ManagedPCInstance.Config.extraConsumerProps` (null vs present, wins-last ordering). Both
  become millisecond broker-free tests once the seams exist.

## CI and gate caveats that affect work right now

- **A green `review` check can mean the reviewer never ran.** `claude-code-action` refuses to run when
  the workflow file differs from the default-branch copy, and reports that skip as **success**. So any
  PR editing `claude-code-review.yml` (or a workflow it validates) gets a green review check that
  verified nothing. Correct behaviour on the action's part, invisible unless you read the job log.
  **Do not read a green `review` check as "reviewed" on a workflow-touching PR.**
- **Reviewer credential exposure is unresolved, not cleared.** The review job runs PR-authored
  Maven/test code in the same job that holds `secrets.CLAUDE_CODE_OAUTH_TOKEN`, with
  `pull-requests: write`. Bounded by fork PRs not receiving secrets (same-repo, push-access only), which
  is not an answer to the actual question. Needs confirmation from the action's docs/maintainers about
  token scrubbing before spawning Bash subprocesses. Until then: trusted authors only. (`pull-requests:
  write` may also be droppable back to `read` if the action posts via its own app token.)
- **Two reviewer grants still missing:** `actionlint` (so the reviewer cannot lint workflow PRs - it
  said so itself on #102; it ships on `ubuntu-latest` and `.github/actionlint.yaml` already exists) and
  `bin/todo-index.sh` (the script merged with #103, the grant did not follow). Land both in a
  **non-workflow** PR, or the validation skip above means they are never exercised.
- **`bin/ci-integration-test.sh` in the review job is granted but unproven** against the 30-minute cap -
  Testcontainers on a 2-core hosted runner is slow, and an overrun looks like a timeout rather than a
  misconfiguration. Also unverified whether Docker works inside the action's sandbox at all.
- **Stacked PRs are ungated.** The only ruleset targets the default branch + `master-confluent`, so a PR
  whose base is another feature branch bypasses *every* required check, not just the dependency gate
  (observed: #87 failed the dep check yet showed mergeable). Fix: add a second ruleset targeting `**`
  requiring just the dependency check (it passes trivially on non-stacked PRs) - and verify the
  check-run name matches exactly, since rulesets match by name.
- **`Kafka Compat (experimental 4.x)` is disabled** (`if: false` in `maven.yml`) - it cannot compile
  under kafka-clients 4.x until the 0.7.x migration. Re-enable with `if: github.event_name ==
  'pull_request'` when that work starts.
- **The `local` self-hosted PR jobs are disabled** (`pr-local-fast-feedback.yml`, `pull_request` trigger
  commented out). That runner is offline indefinitely and its suites now run on the highcpu runner
  (a strict superset). `workflow_dispatch` still works; restore the trigger if the box comes back.
- **The highcpu lane runs six suites per branch on one box** (including mutation sweeps); three jobs
  died of runner-lost-communication during the W4 investigation. Consider a shared concurrency group or
  moving mutation off-box - it makes chaos timing SLOs noisy. Mutation strategy is being reconsidered
  wholesale in **#111**.
- **`MultiInstanceMetricsTest.sameRegistryCanBeReusedAfterPcInstanceClosed` - flaky, undiagnosed.**
  ~1/104 under the forked-per-broker integration run: `TimeoutException: Timeout while waiting to get
  produce lock (PT2S)` / commit lock (PT1S); passes on re-run. Hypothesis is test-tightness under CI
  contention rather than a real lock bug, but that is **not established**. Reproduce under artificial
  CPU load and classify before touching the timeouts (AGENTS.md rule).

## Deferred dependency upgrades

Everything is at its newest **non-major** version (`versions-maven-plugin -DallowMajorUpdates=false`
plus `bin/deps-version-rules.xml`, which also filters pre-releases and Confluent `-ce`/`-ccs` Kafka
builds - without that filter Kafka "latest" mis-resolves to a Confluent build). Held back deliberately:

| Held at | Latest | Why, and what unblocks it |
|---------|--------|---------------------------|
| kafka-clients / streams `3.9.1` | `4.3.1` | Needs the Java 11 baseline - tracked in #53 above |
| junit-jupiter / platform `5.14.4` | `6.1.2` | Needs Java 17, **and** `archunit-junit5` will not run on JUnit 6 with no `archunit-junit6` engine yet ([TNG/ArchUnit#1556](https://github.com/TNG/ArchUnit/issues/1556)). Rewire the ArchUnit tests before #38 can land |
| testcontainers `1.21.4` | `2.0.5` | Testcontainers 2.x, core artifact only (the `kafka`/`postgresql`/`junit-jupiter` modules already moved) |
| vertx-junit5 / web-client `4.5.31` | `5.1.5` | Vert.x 5 |
| mutiny `2.9.5` | `3.3.0` | Mutiny 3 |
| wiremock-jre8 `2.35.2` | `3.0.1` | WireMock 3 (artifact renamed `org.wiremock:wiremock`, test-only). **Side effect while on 2.x:** it drags in byte-buddy `1.12.18`, which wins the conflict and lacks the `JAVA_V21` field mockito 5.23 needs → every Mockito test errors. Worked around by pinning `byte-buddy.version=1.17.7`; **remove that pin when wiremock moves to 3.x** |
| micrometer-core `1.13.15` + registry-prometheus `1.12.2` | `1.17.x` | Not a major, but source-incompatible: micrometer 1.13 renamed `io.micrometer.prometheus` → `io.micrometer.prometheusmetrics`, breaking `example-metrics/CoreApp.java`. Migrate the imports + registry construction, then bump the family together |
| jackson-databind `2.17.2` (example-metrics, test scope) | `2.18.9` | Module-local **on purpose**: pinning it globally forces WireMock in `parallel-consumer-vertx` onto an incompatible Jackson and breaks `VertxTest` (HTTP 500). Dependabot told to ignore (#76); bump in the next curated sweep with an example-metrics integration-test run |
| maven-clean/deploy/install/jar/resources/source/compiler, surefire/failsafe, site | Maven-4 betas/milestones | Only pre-releases available; held by the risk policy. Revisit at GA |

## Next candidates (parallel-safe with the open PRs)

File collisions to respect: **#80** owns the poll/lifecycle internals, **#57** owns metrics + partition
state, **#106** owns the offset encoders. Ranked backlog and full verdicts live in
`src/docs/development/upstream-pr-analysis.adoc`; the ready picks:

- **`upstream #912` vertx leak** - branch done, just needs rebase + PR (above). Best immediate pick.
- **Logging-verbosity cleanup** - batch `upstream #629`/`#631`/`#640` into one PR
  (`ConsumerOffsetCommitter`, `RemovedPartitionState`, `AbstractParallelEoSStreamProcessor`).
  Low effort, high ROI.
- **Contributor-friction build fixes** - `upstream #162` (mvn compile without test-jar),
  `upstream #861` (`ManagedTruth` not found), `upstream #906` (pom version mismatch).
- **Security dep bumps** - `upstream #851` (postgres), `upstream #913` (assertj); pom-only.
- **[#40](https://github.com/astubbs/parallel-consumer/issues/40)** - dedup the `MockConsumer*` test
  classes (test-only; the duplication bot keeps flagging them).
- **`upstream #915` batch construction strategy** (cherry-pick; closes 4-year-old `upstream #266`) - medium effort.
- **DLQ** (`upstream #310` / revive `upstream #366`) - most-demanded missing feature; large, spec-stage only.

## Parked ideas

- **Extract the quarantine lane as its own FOSS project.** The `@Quarantined` lane (annotation +
  enforced registry + owner-claim verification + non-gating CI job + release blocking + self-tests) is
  generic. Differentiator: the closed loop is enforced *in CI* (registry cannot drift, owner PR must
  exist and stay open, merged-owner-without-re-enable turns red, releases blocked) rather than living in
  a SaaS dashboard. Check for prior art first - adjacent, mostly commercial: Trunk.io flaky-test
  quarantining, BuildPulse, Datadog Test Optimization, Develocity flaky management, JUnit Pioneer's
  `@DisabledUntil`. Would extract as annotation + scripts + a reusable GitHub Action.
- **User-facing upstream issue mirroring.** Internal upstream tracking is strong (`upstream-map.yaml`,
  this ledger, `docs/solutions/`); nothing is user-facing, so a user on the fork's Issues tab cannot tell
  whether `upstream #857` is fixed here, in flight or won't-fix - and upstream data could be archived one
  day. Plan: **mirror on touch, never in bulk** - create a fork issue only when we address an upstream
  one. Structure: title `upstream #NNN: ...`, a quoted+attributed snapshot of the upstream content, our
  disposition, links to fixing PRs and solutions docs, `upstream-mirror` label. Conversation then lives on
  the fork issue. Script it off `upstream-map.yaml` so the map stays the source of truth and issues are a
  rendered view; **one issue per invocation (no batch mode) and a first-class dry-run** that prints exactly
  what would be created or updated. First candidates once the PR queue drains: `upstream #857`, `#909`,
  `#893`/`#905`, `#912`.
- **Hardened "concede optimizer"** - letting the required GitHub-hosted gate report green without running
  its tests when the self-hosted highcpu runner already passed the same suite for the same SHA. Removed
  from #75 by review, re-introduced and dropped again on #80. **Do not revive without fixing all of:**
  (1) *gate spoof* - matching on free-text workflow name + job prefix + head SHA lets a PR add its own
  trivially-passing workflow named `highcpu` and skip the real tests; bind to the workflow's immutable
  path/ID and verify `event == 'pull_request'`, head repository and actor. (2) *timeout* - a 600s wait
  against a 15-minute job budget can burn 10 minutes before falling back; only concede to an
  already-complete run. (3) *non-equivalent contracts* - conceding hosted Integration (`forkCount=4`) to
  highcpu (`forkCount=8`) is not like-for-like. (4) *silent name drift* - names hand-synced across three
  files with a `KEEP IN SYNC` comment; needs a drift self-check. (5) *invisibility* - a conceded skip must
  land in `$GITHUB_STEP_SUMMARY` with a link to the trusted run. **Recommended instead: keep `highcpu`
  purely advisory.** The win is fast feedback, not skipping the hosted gate; the gate staying independent
  is worth more than the minutes saved.
