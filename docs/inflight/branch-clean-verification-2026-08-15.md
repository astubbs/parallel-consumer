# `feats/proxy-requirements` - the first verification on a quiet box

Every previous green on this branch was measured while seven or more agent JVMs shared one machine,
so no timing number from those runs is worth anything and several red runs were dismissed as
contention without a control. This is the run with the box to itself: load average 2.4 and falling at
the start, one idle Kotlin build daemon, no other build running, 32 cores.

**Verdict: mergeable on this evidence. The branch is clean; the one red is master's, not this
branch's, and this branch fails it LESS often than master does.**

> **CORRECTION, 2026-08-17.** The self-test row above was true locally and false in CI, at the same
> moment. `bin/test-check-proto-breaking.sh` builds two of its fixtures with `git commit-tree`, which
> refuses to write an object when git cannot resolve a committer - and a GitHub-hosted runner has no
> identity at any config level. So `proto: breaking` was red on **every** run from `222940cbb`, the
> commit that added the self-test, and this verification could not see it: every developer box has an
> identity configured, so the lane passed here exactly because of what made it fail there.
>
> **The verdict above still holds, and for a reason worth stating**: the failure was in the gate's own
> self-check, never in `buf breaking`, and the schema was independently confirmed unchanged since the
> freeze commit `c23794008` apart from the six per-language file options landed deliberately before
> the gate armed. Fixed on this branch by `1c4101780`, which stamps the identity per-invocation in the
> environment - **not** via `git -c`, which loses to an empty environment identity and was the first
> attempt.
>
> **The lesson this record now carries is about itself.** A green measured on one box is evidence
> about that box. This row was one of the inputs to a *mergeable* verdict, and the thing it was
> blindest to was a check that had never once been green where it actually runs. The row is left
> standing rather than rewritten - it is what was measured - and the correction sits beside it.

Base for every comparison below: `cae88c7aa` (master). The branch moved twice mid-run
(`280a4b8f2` → `0c369366c`, a parallel documentation session) - both commits are markdown only, so
no result here is split across a code change.

## What ran green

| Lane | Result | Uncontended time |
|---|---|---|
| 10 executable `bin/check-*.sh` gates | all pass | seconds each, ~4 min for the proto/CVE three |
| 10 `bin/test-check-*.sh` self-tests | all pass **locally** - see the correction below; one was red in CI at this moment | seconds each |
| Full reactor `bin/build.sh` (clean package, 30 modules) | **BUILD SUCCESS**, 615 tests, 0 failures, 8 skipped | **6 min 04 s** |
| Foreign-client lane, 7 languages, `-Dpc.foreignClients` | all pass, all with native test output | see below |
| Container lane, `bin/build-client.sh cpp/swift --test` | both pass | 1 s each - **cache hit, see the caveat** |
| Conformance suite incl. `LanguagesRunInParallelTest` | 10 tests pass; parallel test 3/3 on repeat | 10.5 s in-build, 26 s per repeat |
| Integration suite `bin/ci-integration-test.sh` (Docker 29.7.2, TestContainers) | **BUILD SUCCESS**, 125 tests, 0 failures, 6 skipped | **11 min 30 s** |

The three timing-sensitive tests that flaked repeatedly under tonight's contention all passed here
without incident: `processInKeyOrder(CommitMode)` in every one of the eleven core unit runs below,
`TransactionTimeoutsTest` 3/3 in 17.5 s, and `TransactionAndCommitModeTest` 33/33 in 49.2 s. On a
quiet box they are not flaky, which is the answer their `docs/solutions/test-flakiness/` write-ups
predicted and nobody had yet been able to demonstrate.

Gate detail worth keeping: `check-copyright-headers.sh` cleared 645 files, `check-issue-refs.sh`
cleared 403 changed files, `check-docs-data.sh` validated 82 fragments, `check-quarantine-registry.sh`
found the registry consistent at 2 entries, and `check-proto-breaking.sh` passed *vacuously* - it
reports that `origin/master` carries no frozen `proxy.proto` yet, so the gate is not yet armed. That
is the gate working as designed, but it is not evidence the wire is stable.

Three gates could not run and were not skipped silently: `check-human-lgtm.sh`,
`check-review-posted.sh` and `check-cve-exclusions.sh`'s GitHub half all need a PR number, and this
branch has never been pushed. `check-ossindex-audit.sh` was not invoked standalone, but the
`ossindex:3.2.0:audit` goal it reads ran inside every Maven build above and never failed.

Per-language times in the foreign lane (`./mvnw -Pci test -pl :<module> -am -Dpc.foreignClients`,
the exact command `.github/workflows/clients.yml` runs): go 146 s, python 150 s, typescript 143 s,
kotlin 170 s, rust 24 s, ruby 10 s, dotnet 12 s. The first four are slow only because their module
depends on the engine, so `-am` rebuilds and re-tests core each time; the last three have no engine
edge yet. Each one's native suite genuinely executed, which is the thing worth checking in a repo
with a history of checks that pass without running: go `ok ... 4.012s`, python `10 passed in 4.20s`,
typescript `pass 7 / fail 0`, rust three binaries at `17`, `1` and `4` tests, ruby `11 examples, 0
failures` after a clean RuboCop, dotnet `Passed: 1`, kotlin 21 surefire tests across `BridgeTest`,
`NoVerdictIsInventedTest`, `ProcessorOutcomeTest` and `OneRecordThroughTheSidecarTest`.

## Two things that are green but weaker than they look

**The container lane proved nothing new tonight.** Both `cpp` and `swift` returned in 1 second with
every BuildKit layer `CACHED` against images built at 23:54 earlier the same night. The host-side
half did run - the extracted static binary executed, the dynamically linked control failed as
expected, which is the portability assertion the script exists for - but no compilation happened. A
cache hit does mean the inputs were byte-identical to a build that succeeded, so this is not a hole;
it is just not a fresh measurement, and nobody should quote "swift builds in 1 s".

> **SUPERSEDED, 2026-08-17.** Both caveats in this section have since been closed, and two other
> statements above have gone stale. `LanguageRunners.all()` now registers **ten** command-driven
> languages - go, python, typescript, rust, ruby, dotnet, kotlin, scala, cpp, swift - which with the
> core and two Java in-process bindings is thirteen, all green in CI at 4 scenarios each. The branch
> **has** since been pushed, as astubbs/parallel-consumer#293, so the three gates recorded below as
> unrunnable for want of a PR number now run. And `check-proto-breaking.sh` still passes vacuously
> against `origin/master` for the reason given - the freeze arms only when the schema lands on master.

**The shared conformance suite drives one language, not every language.** `LanguageRunners.registered()`
returns `List.of(go())`, and python, typescript, rust and ruby are commented placeholders in that same
file describing the entry each still needs. So "the suite that drives every language" is currently
Go plus a deliberately-broken control runner. The suite itself is sound - `LanguagesRunInParallelTest`
measures a real overlap between two runners and attributes their opposite verdicts correctly, and it
passed 3 more times on repeat - but its coverage claim should be read as one language wide. The other
six languages tested their own suites, in their own module, not through the shared scenarios.

## The one red: `PCMetricsTest.metricsRegisterBinding`, and it belongs to master

```
PCMetricsTest.metricsRegisterBinding:108 ? ConditionTimeout
expected: 1212.0
 but was: 1209.0 within 2 minutes.
```

Line 108 is the `await().atMost(120s)` block asserting that `PARTITION_LAST_COMMITTED_OFFSET` has
caught up with the test-side processed counters; the specific assertion that timed out is the
partition-1 one - grep `isEqualTo(counterP1.get() + p1StartingOffset)` in `PCMetricsTest`, exactly one
hit. The gauge sat short of the counter for the full two minutes.

**Reproduction, all on the quiet box, `-Pci` (which on 32 cores means `forkCount=1C` = 32 JVMs, a
harsher environment than CI's 2-core runners):**

| Arm | Full core-suite runs | Failures |
|---|---|---|
| This branch | 11 | **1** |
| `cae88c7aa` (master), alternated run-for-run against the branch | 5 | **3** |
| This branch, `PCMetricsTest` alone | 10 | **0** (13.1 s every time) |

The branch's 11 are the full `bin/build.sh`, four `-am` rebuilds from the go/python/typescript/kotlin
client lanes, the conformance `-am` run that first surfaced it, and five dedicated runs. The master
arm ran from a throwaway worktree at `.claude/worktrees/verify-master-control`, alternating
branch-then-master so machine drift could not favour either side.

**Classification: a pre-existing master flake, and this branch is the better arm.** It reproduces on
master with the identical stack and assertion (`expected: 208.0 but was: 201.0`, `208.0 / 200.0`,
`1204.0 / 1198.0` - the gap varies, 3 to 8), and it does not reproduce in isolation at all, so it
needs the 32-fork self-contention of a full suite to appear. Nothing in this branch's core diff
(`WorkContainer`, `WorkManager`, `ShardManager`, `ProcessingShard` - the abandoned-without-verdict
path) is implicated: the new code is only reachable via `markAbandoned`, which no core unit test
calls, and the arm carrying it failed less, not more.

**Two things about it are new and are the reason this is worth a note.**

*It is not actually quarantined.* [`docs/inflight/test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md)
records it as "quarantined (owner astubbs#265)", but `bin/check-quarantine-registry.sh` reports two
entries and this is neither of them, and `PCMetricsTest` carries no `@Quarantined`. The quarantine
lives on astubbs#265's unmerged branch, so on master today the test gates every PR - which matches
what happened here. Anyone reading that ledger row will believe this failure cannot block them.

*The signature is not the documented one.* The ledger's entry is `expected: 203.0 but was: 207.0` at
`:115` - the gauge running **ahead** of a stale counter snapshot, diagnosed as comparing two moving
values. What reproduced here is the opposite: at `:108` the gauge is **behind** and never catches up
inside 120 seconds, at a point where the test's own comment - grep `The processed counters are frozen
now` in `PCMetricsTest` - says the counters have stopped moving. If that comment is right, a frozen
target the gauge cannot reach in two minutes is a
committed-offset shortfall rather than a sampling race. **That is an unverified lead, not a
diagnosis** - falsify it by logging whether `counterP1` is genuinely still while the await burns, and
rule the load-tightness family in or out before touching anything. Nothing in
`docs/solutions/test-flakiness/` describes this direction.

Nothing was loosened, retried, quarantined or skipped to produce any number above.

## Not run

- The **language-native static-analysis step** each `clients.yml` row runs after its test step
  (`scripts/analyse.sh`, `make lint`, `cargo clippy -D warnings`, `npm run check`, `bundle exec
  rubocop`, `cppcheck`, `swift format lint`) was not run as a separate step. Ruby's RuboCop ran
  anyway inside `rake`; the other eight are unverified here.
- The conformance module's other three test classes ran under the default profile in the full build,
  not under `-Pci`; only `LanguagesRunInParallelTest` was repeated under `-Pci`.
- No Kafka-version matrix (`bin/ci-build.sh`), no performance lane, no mutation lane.

Delete this file once the branch merges. The `PCMetricsTest` finding outlives it and belongs in
[`docs/inflight/test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md), whose row for that test
needs correcting either way.
