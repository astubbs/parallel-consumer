# In-flight & parked work

> Shared, cross-branch working notes kept on `master` so any branch or session can see what is in
> progress or parked. **Not** an issue tracker and **not** a backlog.
> Last verified against the repo, GitHub and the working tree: **2026-08-04**, master `bd717241`.
>
> **Scope rule: track only what is currently OPEN** and not already tracked elsewhere, plus the
> cross-branch context a future branch should inherit. When something CLOSES, **delete** its entry - do
> not keep it by rewriting it into a "FIXED/DONE" narrative, because making a stale entry *accurate* is
> the wrong move. Shrink it to the still-open follow-ups it surfaced, or remove it.
>
> **Never write down what a command can answer.** Open PRs are `gh pr list`; branch divergence is
> `git rev-list --left-right --count`; worktrees are `bin/worktree-status.sh`. Copying those here
> creates a second tracker that is wrong within a day and that a reader cannot tell is wrong. This
> file is for what no command knows: why something is parked, what blocks it, which decision is
> pending, and which pieces of work collide.
>
> **Work that your current PR resolves is tracked by that PR - so delete its entry in that PR.** Never
> leave a "delete this when #NN merges" marker behind on `master`: the merge is exactly the moment
> nobody is looking at this file, so the marker outlives the work and the next reader inherits a stale
> entry that reads as live. Deleting it in the PR that fixes it costs nothing and cannot rot. The durable records live elsewhere:
> `CHANGELOG.adoc` (what shipped), PR bodies and commit
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
>
> **If you are given new guidance that changes how this file is written, update this header too**, so
> that other agents and sessions inherit the same rule instead of rediscovering it.

## Open PRs: only what `gh` cannot tell you

**Do not list open PRs here.** `gh pr list` is always right and this file would be wrong within a day;
likewise branch ahead/behind counts (`git rev-list --left-right --count`) and worktree locations
(`git worktree list`, `bin/worktree-status.sh`). Record only the things no command can answer -
blockers, collisions, and decisions someone is waiting on:

- **#29 and #31 target `master-confluent`**, the pinned pre-rebrand mirror, so merging either would
  land its fix where no user can reach it. Retarget to `master` - but not mechanically: #29's deadlock
  fix predates the internals #80 reshaped, so it needs reconciling rather than replaying.
- **#38 (JUnit 6) is blocked on something other than the version bump**: JUnit 6 needs Java 17, *and*
  `archunit-junit5` will not run on it with no `archunit-junit6` engine in existence. The ArchUnit
  tests must be rewired first. See the dependency table below.
- **#51 (virtual threads) collides with #57** - both edit `PCMetrics.java`. Sequence, don't parallelise.
- **#57 owns metrics + partition state**, **#106 owns the offset encoders**, and **#29 will want the
  poll/lifecycle internals**. Pick parallel work accordingly.
- **#1 (`codeql`, 2026-04) and #8 (`features/retry-dlq`, 2022) are abandoned drafts** kept only because
  #8 is the sole DLQ code that exists. Close or finish them; they are not in flight.

### What is still open in the upstream #857 family

Three distinct defects sit behind upstream's one "paused consumption after rebalance" symptom. Two
have landed: **#100** (a mid-rebalance commit threw `RebalanceInProgressException`, which nothing
caught, permanently killing the broker-poll thread) and **#80** (a draining consumer never called
`consumer.poll()` - ~10kHz busy-spin plus a rebalance-unresponsive member zombie-holding its
assignment). Write-ups in `docs/solutions/test-flakiness/`.

**Still open: the original deadlock, in #29** - `synchronized(commitCommand)` between the poll thread
(`onPartitionsRevoked`) and the control thread (`commitOffsetsThatAreReady`), replaced there with
`ReentrantLock.tryLock()`. It is a sibling of the two landed fixes, not a duplicate: #29/#31 were
verified *not* to fix the drain defect, and the uber-branch experiment showed the #80 stack composes
cleanly with both. Live confirmation the deadlock is still present: `RebalanceEoSDeadlockTest`
failed once under the 20-run stress hunt (see the load-tightness family below, where it is
explicitly *not* a member). #29 needs a rebase and a retarget before any of that can land.

**Gated on #29: validating that thread-parallel integration tests are actually safe again.** #68 made
the integration suite reliable by *forking* per broker (`forkCount=4`), which sidesteps the deadlock
rather than proving it gone - the contended `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` failure it
was hiding is the real upstream #857 bug. So the deferred "Step 2" is to re-run with
`-Dparallel-tests=true` on a shared broker **after #29 lands**, and see whether it stays green. One
probe on the highcpu runner already hinted it might (forked unit suite green with threads enabled;
the integration red was the separate `PartitionStateCommittedOffsetIT` flake, since fixed by #80), but
one green run is not proof. Note forking stays the default regardless: fork×threads measured no
faster than fork alone, because forking already saturates the cores.

### #57 - PCMetrics leak (upstream #859) + cherry-picks

Fixes duplicate Micrometer meter re-registration on assignment/revocation, and bundles the
`upstream #893` (offset accuracy on assignment) and `upstream #905` (max-queued-records-per-shard
metric) cherry-picks into one PR instead of a 3-deep stack, superseding the old closed stack
(#42 → #43 → #45). Owns `PCMetrics.java`, `PCMetricsDef.java`, `PartitionState.java`,
`PartitionStateManager.java`, `ShardManager.java` - which is why #51 and anything touching partition
state sequences after it.

### #53 - 0.7.x: Java baseline + Kafka 4

**The only reason to move off Java 8 is Kafka 4.** kafka-clients 4.x needs **Java 11**, so that is
the target baseline ("don't be stricter than Kafka"). Jabel is what lets `javac` accept Java 17
syntax while emitting Java 8 bytecode; the branch holds a provisional state (Jabel removed,
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

## Work sitting on branches with no PR

The part `gh` cannot see. (`git branch -vv` and `bin/worktree-status.sh` give the mechanics.)

- **`bugs/912-vertx-stream-memory-leak`** - clears the JStream deque on close (`upstream #912`,
  a production leak) + `JStreamMemoryLeakTest912`. Done and pushed, vertx-module only, so it collides
  with nothing. **Rebase → open PR.** The cheapest open item to land.
- **`docs/uber-stall-experiment-results`** (with the `experiment/stall-uber-fix` / `stall-uber-nofix`
  arms) - the composition experiment behind #80, which has merged. One result still matters: the
  stall-fix stack composes cleanly with #29 + #31, which is what makes #29's rebase tractable. Fold
  that sentence into #29 and delete all three branches.
- **`debug/committedoffset-firstpoll-stall`**, **`debug/chaos-w4-red-commit-response-stall`** -
  diagnostic-only; both investigations landed (#80, #100) with write-ups in `docs/solutions/`.
  Safe to delete.
- **`docs/inflight-as-directory`** - parked idea: split this ledger into a directory instead of one
  growing document. Worth revisiting if this file bloats again.
- **`astubbs/orca`** - CI/tooling (Claude review + PR-assistant workflows, PR-dependency check, CI
  matrix tweaks) from before master grew its own versions of most of it. Badly diverged. Salvage
  whatever is still novel, or abandon it.
- **Superseded / stale, safe to prune:** `cherry-pick/893-offset-reset`, `cherry-pick/905-max-shard-metric`,
  `upstream-pr-893`, `upstream-pr-905`, `pr-909-temp`, `bugs/859-pcmetrics-leak-v2` (all folded into
  #57); `refactor/test-hardening`, `ci/reenable-parallel-tests`, `backup/*`; `dev-cc` and
  `master-confluent` pinned at pre-rebrand `7f290122` (`master-confluent` is still a ruleset-protected
  branch and the base of #29/#31 - retarget those before touching it).

## Release 0.6.0.0

Not yet released: pom is `0.6.0.0-SNAPSHOT`, no `v0.6.0.0` tag, changelog section written. Release =
strip `-SNAPSHOT` and merge to `master`; `publish.yml` runs after CI succeeds, deploys via the
`maven-central` profile, tags `v<version>` and cuts a GitHub release (AGENTS.md → *Releasing*).
**No longer blocked by the quarantine guard**: #80 emptied the registry when it merged, so
`release.yml`'s "no release while tests are quarantined" gate now passes.

## Quarantine lane

Registry [`docs/QUARANTINED_TESTS.md`](QUARANTINED_TESTS.md) is **empty** - #80 deleted both its
annotations and entries when it merged. Nothing to do here; the section stays only so the next
quarantine is recorded rather than invented. Rules live in AGENTS.md (Testing); the registry is
CI-enforced against the `@Quarantined` annotations in both directions.

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
- **The highcpu lane runs six suites per branch on one box** (including mutation sweeps); jobs
  repeatedly die of runner-lost-communication (3+ times on #80 alone). Consider a shared concurrency
  group or moving mutation off-box - it makes chaos timing SLOs noisy. Mutation strategy is being
  reconsidered wholesale in **#111**.

### Load-tightness flake family (undiagnosed)

Shared signature: a **fast-failing** assertion or timeout under heavy contention, passing in isolation
or on rerun. Roster and rates from the 20-run fork16 acceptance hunt on #80's branch (2026-07-30);
baseline for comparison is 15/20 runs fully clean, zero stall-class failures.

| Test | Rate | Symptom |
|------|------|---------|
| `MultiInstanceMetricsTest.sameRegistryCanBeReusedAfterPcInstanceClosed` | 0/20 hunt, ~1/104 on CI | 1-2s produce/commit lock timeouts |
| `TransactionTimeoutsTest.produceTimeout` | 1/20 + 1 highcpu | tight produce-timeout assertion |
| `LoadTest` | 1/20 | 60s throughput awaits |
| `DbTest` | 2/20 | postgres container start under contention |
| `KafkaSanityTests`, `TransactionMarkersTest` | singles | residual, uncategorised |

**Classify before touching any of them** (the #68 lesson): this family is exactly where the upstream
#857 deadlock and the drain zombie were hiding, and both looked like tightness first. Two members have
since been *solved* and are no longer in the family, which gives you their signatures to rule out: the
nudge race is an unwinnable await plus a `SubscriptionState` reset positioned past the data
(`latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md`), and the drain zombie is a poll spin
in `DRAINING` state (`pc-silent-stall-under-contention-2026-07-29.md`).

**Explicitly NOT a member: `RebalanceEoSDeadlockTest.noDeadlockOnRevoke`** (1/20). Per the #68 record
its contended failure maps to the real upstream #857 deadlock - so that sighting is live confirmation
the deadlock is still on master, with its fix waiting in #29.

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

Collisions are listed at the top of this file. Ranked backlog and full verdicts live in
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
