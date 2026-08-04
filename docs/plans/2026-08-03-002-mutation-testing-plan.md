# Mutation testing: what we have, why it under-delivers, and what to change

**Status:** the cheap half is DONE (shipped in PR #111, marked inline below); the substantive retarget
is still parked and unscheduled.
**Written:** 2026-08-03
**Context:** written while fixing the `ProducerManagerTest` flake (PR #110) that had been aborting the
PIT lane. That fix restores the lane; this document is about whether the lane is pointed anywhere
useful once it runs.

---

## 0. What shipped, and what is still only an opinion

Two things came out of implementing this that the original analysis had wrong, both discovered by
reading pitest's own source rather than its documentation. They are recorded in place below (§2, §3.4)
because a plan that quietly corrects itself teaches nothing.

| Done in #111 | |
|---|---|
| Full sweep off PRs | Deleted from the highcpu matrix. Now manual-only in `mutation-full-sweep.yml` - **not** nightly: scheduling a job that has never once completed only moves the waste to a quieter hour. It earns a `schedule:` the day a run finishes. |
| One lane, not three-plus-a-spare | PIT ran **three times per PR**, with a fourth copy configured but dormant (§3.5). Now exactly one: `maven.yml`. |
| `skipFailingTests` | §3.0's blast radius, fixed at the source rather than worked around. One flake no longer switches mutation testing off repo-wide. |
| A summary that explains itself | Every exit path now writes to the job summary, including the "nothing to mutate" one - so a green tick states which kind of green it is (§6). |
| **Retargeted to `offsets.*`** | §4.3, the substantive one. `internal.*` is no longer the sweep default, and `PIT_TARGET_CLASSES` / `PIT_TARGET_TESTS` make any other target a workflow input rather than a code change. |

**Applied but NOT yet measured:** the retarget is a config change made on the strength of the argument in
§4.3, not on a completed run. `offsets.*` is not obviously cheap either - `RunLengthEncoderTest` alone is
~140s, re-run per mutant - so the first question the manual sweep answers is whether it completes at all.
Do not quote a mutation score for this project until one has.

---

## 1. What mutation testing is for

Change the main code in a small, deliberate way — flip a conditional, drop a call, alter a return —
and see whether the test suite notices. If no test fails, that mutant **survived**, and it marks a
behaviour nothing actually asserts.

The essential structure: **the main code is the subject, and the test suite is the oracle.** Coverage
tells you a line executed; mutation testing tells you someone would have complained had it been wrong.
That is a much stronger claim, and it is the only automated one we have that grades the tests
themselves.

## 2. Never mutate test code

Our `targetClasses` glob is `io.confluent.parallelconsumer.internal.*`, and our tests live in the
production package — so `internal.ProducerManagerTest`, `internal.PCModuleTestEnv` and
`internal.TestParallelEoSStreamProcessor` all match it **by name**.

They are not mutated in practice, and our numbers are not polluted.

**Correction (2026-08-04, found while implementing §4.5):** the original claim here was that we are saved
by an *unstated default* we could accidentally widen. That is wrong, and the proposed fix was
unimplementable. `mutableCodePaths` is a **command-line-tool option that pitest-maven does not expose at
all** - there is no such `@Parameter` on `PitMojo`, and adding a `<mutableCodePaths>` element fails the
build outright. `MojoToReportOptionsConverter` hard-codes the mutable code path to
`${project.build.outputDirectory}`, widening it only when `crossModule=true`.

So main-only is **structural under Maven**, not a default anyone can flip by accident. The real
correspondent of the original worry is `crossModule`, which is off and now pinned off explicitly in the
pom with the reasoning attached. The lesson generalises: this was documentation-vs-source drift, and the
source settled it in one grep. Read the mojo.

**Why mutating tests would be wrong** - still worth knowing, because it explains why `crossModule` is
pinned rather than merely left at its default:

It removes the oracle and keeps the subject. Mutation testing *is* the test of your tests; mutating the
tests asks "what tests the test-of-tests", and there is nothing left to answer with.

The mechanical harm is worse than "meaningless". Mutate an assertion in test `T` — flip `isTrue()` to
`isFalse()`. PIT runs the tests covering that line. The test covering `T`'s assertion **is** `T`. So `T`
fails, and the mutant is recorded as **KILLED**.

Nearly every test mutant kills itself. Mutation score is kills over total mutants, so padding the target
set with self-killing mutants drives the score **toward 100% while adding no information about the main
code at all**. The score rises as the signal falls — the worst property a metric can have, because it is
most reassuring exactly when it is least meaningful.

**Action (done):** `<crossModule>false</crossModule>` on pitest-maven, with the above as its comment.
Not because the default is wrong, but because "why is this off?" is a question the next person will ask.

### The legitimate adjacent case

We do have test-tree code that is really production-like infrastructure — `PCModuleTestEnv`,
`PausableWorkManager`, `LimitedDynamicExtraLoadFactor`. Complex enough to harbour bugs, and a bug there
silently weakens every test built on it.

The answer is *not* to mutate it in place. It is to give that infrastructure its own tests, at which
point it is an ordinary subject with an ordinary oracle.

## 3. Why the current setup under-delivers

### 3.0 Any single flaky test disables mutation testing repo-wide

The property that makes everything else fragile, and the least obvious one.

PIT refuses to run when **any** test is unstable *without* mutation — it needs a green baseline to
attribute kills to mutants rather than to noise:

```
1 tests did not pass without mutation when calculating line coverage.
Mutation testing requires a green suite.
```

So the lane is not degraded by one flaky test; it is **switched off**. No mutants are scored at all,
anywhere, regardless of which class flaked or whether it has anything to do with the code being
mutated.

That has happened twice already, and both times the flake was somewhere unrelated:

| PR | Test | Effect |
|---|---|---|
| #101 | `ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown` | whole lane aborted |
| #110 | `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` | whole lane aborted |

Three consequences worth holding onto:

- **The lane's green-ness has been tracking suite stability, not mutation coverage.** It reports on the
  health of the tests as a whole, which is a different — and much weaker — signal than the one we
  wanted.
- **`rerunFailingTestsCount` cannot rescue it.** Surefire reruns hide a flake from the *unit gate*, but
  PIT performs its own coverage run and sees the raw result. A flake papered over for the gate still
  kills mutation testing. It has to be *fixed*.
- **The blast radius is disproportionate to the cause**, so it is worth knowing that a red mutation lane
  most often means "something, somewhere, is flaky" rather than anything about mutants. Check for the
  `did not pass without mutation` line before investigating mutation config.

#### FIXED in #111: `skipFailingTests`

This one turned out to have a direct fix in pitest itself, which the original draft missed by reading
the docs rather than the source: `skipFailingTests` makes PIT **drop a failing test's coverage instead
of aborting**. `DefaultCoverageGenerator` simply doesn't feed a red result into the coverage data, so
`allTestsGreen()` never trips and the run continues. One flake costs a few mutants' worth of coverage
instead of switching mutation testing off repo-wide.

**Verified both directions** (deliberate always-failing test added to `offsets`, then removed):

| `skipFailingTests` | Result |
|---|---|
| `false` | exit 1, `1 tests did not pass without mutation ... requires a green suite`, **zero** mutants scored |
| `true` | exit 0, run completes, 18 mutations scored |

**Two traps found while doing it:**

1. **It is pom-only.** `PitMojo`'s `@Parameter` declares a `defaultValue` and **no `property`**, so
   `-DskipFailingTests=true` on the command line is *silently ignored* - no warning, no error, it just
   doesn't apply. Same for anything else declared that way. If a pitest setting seems to have no effect,
   check whether it has a `property` before concluding it doesn't work.
2. **Surefire can silently override it.** `parseSurefireConfig` defaults to true, and
   `SurefireConfigConverter.convertTestFailureIgnore` maps surefire's `<testFailureIgnore>` straight onto
   this setting. Adding one to the surefire config would turn this back off from a distance.

**And its cost, which is silence.** PIT logs *nothing* when it skips a test this way - the
`Tests failing without mutation` line only exists on the abort path. A mutant that only the failing test
covered quietly becomes "no coverage" rather than killed. That is an acceptable trade (degraded signal
beats no signal) but it must not be invisible, so the job summary now carries the caveat next to the
no-coverage count. The flake itself is *not* hidden: it still reddens the Unit gate of the same PR run,
which is where a flake belongs. PIT is not a flake detector, and the fix here is to stop it pretending
to be one.

### 3.1 The full sweep has never completed

`bin/ci-mutation-test.sh` said so itself at the time: *"The full internal.* sweep is impractically slow
(it has never completed on CI)."* Observed on PR #110: still running at 42 minutes on CI, and 83+
minutes locally with minions dying on `MEMORY_ERROR` and `TIMED_OUT`. (That statement is about
`internal.*`, which is no longer the target — but nothing has completed under the new one either yet,
so it stands until a run proves otherwise.)

An unbounded sweep that never finishes scores **zero** mutants. It is not a slow signal, it is no
signal, and it costs high-CPU runner time on every PR to produce it.

**Done in #111:** removed from the PR lane; now manual-only in `mutation-full-sweep.yml`, with
`target-classes` / `target-tests` as dispatch inputs so pointing it at a decidable package (§4.3) is a
form field rather than a commit.

### 3.5 The same run happened three times per PR, with a fourth copy waiting

Not in the original draft, and visible only when the workflows are read side by side. Every PR was
starting three mutation jobs: `maven.yml` (scoped, GitHub-hosted), `pr-highcpu` "Mutation (PIT,
scoped)", and `pr-highcpu` "Mutation (PIT, full)". Two of those three were the same scoped computation.

A **fourth** was configured in `pr-local-fast-feedback` but dormant: that workflow's `pull_request`
trigger is commented out while the laptop runner is offline, so it only ran on manual dispatch. Worth
being precise about rather than rounding up to "four times per PR" - a disabled trigger is a real
difference, and the copy that isn't running is not the one costing anything.

It was still worth removing, because it carried a hazard the others don't: that workflow checks out
**shallow**, and when the base ref fails to resolve `bin/ci-mutation-test.sh` falls back to the *full*
sweep. A shallow checkout therefore silently promotes a scoped run into the sweep that has never
completed - on a laptop. Re-enabling that trigger would have shipped the hazard with it.

**Done in #111:** one lane, `maven.yml`, which checks out with `fetch-depth: 0`. The fallback is
documented at the point of the fallback, because it is only dangerous in combination with a checkout
setting in a different file.

The same reasoning removed **Unit and Integration** from the highcpu matrix in the same pass: measured as
not actually faster there than the GitHub-hosted gate that already runs them, so they were a second copy
of an existing verdict. A duplicate check is not free - it is another tick to triage on every PR, and a
checks list that is mostly duplicates teaches people to skim it, which is precisely how a real red gets
missed. The highcpu lane now carries only what needs the cores: Performance and the Chaos Pain Suite.

### 3.2 `internal.*` is close to the worst possible target

That package is the controller, broker poller, committers and work manager — the concurrency core.

- Mutants to lock acquisition, loop conditions and timeouts **hang by construction**. They do not die
  quickly; they sit out the full timeout.
- The covering tests are timing-based, so a surviving mutant is often *unfalsifiable* rather than
  informative — you cannot tell "nothing asserts this" from "the race did not happen this run".

### 3.3 The timeout settings multiply that

`timeoutConstant=30000` with `timeoutFactor=3.0` means a hanging mutant costs 30s plus three times the
baseline test time before it counts. Multiply by every hang-prone mutant, and by PC's own slow tests
(`RunLengthEncoderTest` ~140s, `CoreBatchTest` ~48s, `ProducerManagerTest` ~28s), each re-run per
mutant.

**Do not "just lower the timeout".** PIT counts `TIMED_OUT` as `KILLED`. Shortening it does not only
speed things up — it reclassifies slow survivors as kills and **inflates the score**. The timeout can
only come down once the targets are not hang-prone (§4.3).

### 3.4 Group exclusions are accidental

The script notes it: pitest does not honour `excluded.groups`, so quarantined / chaos / performance
tests are excluded only *coincidentally*, via the `integrationTests` source-dir glob. Today's single
quarantined test happens to be an IT, so it works. The day someone quarantines a **unit** test, PIT
runs it per-mutant. That is a trap with a note on it rather than a fix.

**Possibly already false - do not "fix" this without checking.** `parseSurefireConfig` defaults to
**true**, and `SurefireConfigConverter.convertGroups` reads surefire's `<excludedGroups>` directly into
PIT's group config. Our pom sets `<excludedGroups>${excluded.groups}</excludedGroups>`, defaulting to
`performance,chaos,quarantined`. Whether that works turns on one thing: whether `${excluded.groups}` is
interpolated in the raw `Xpp3Dom` pitest reads, or arrives as the literal string. If it interpolates,
group exclusion is already correct and §4.4 is not work but a comment to delete.

The distinction matters beyond tidiness, because the two failure modes look identical from outside: a
run that correctly skips quarantined tests and a run that "excludes" a tag literally named
`${excluded.groups}` both simply don't run them today. Only a deliberately-tagged probe test tells them
apart.

## 4. Proposed changes

Roughly in payoff order, with one deliberate exception: **4.2 is numbered here for continuity but
belongs last** — see the reasoning in that section. **4.3 is the one that unblocks everything else**,
and it is the only substantive item still outstanding.

### 4.1 Move the full sweep off PRs ~~onto a schedule~~ — DONE in #111, but not onto a schedule

It has never completed, produces no signal, and burns runner time on every PR.

Shipped as `mutation-full-sweep.yml`, `workflow_dispatch` only. The original "put it on a nightly" was
wrong *for `internal.*`*: scheduling a job that has never once finished relocates the waste to a quieter
hour and adds a recurring red-or-cancelled result everyone learns to ignore - worse than no lane, since
it trains people to ignore this one specifically.

**That argument does not survive the retarget, and should not be recycled as though it did.** With
`offsets.*` the sweep is plausibly minutes, not never. The only thing still missing is a measurement:
run it once, then wire the trigger. When that happens, prefer `push: branches: [master]` to a cron - a
mutation score changes only when the code changes, so a nightly recomputes an identical answer whenever
master did not move, and blames a date rather than a merge.

### 4.2 Incremental analysis (`withHistory`) — NOT AVAILABLE TO US AT ALL, and not the lever it looks like

**Correction (2026-08-04): this section had it wrong twice over**, and the repo already knew. It claimed
the basic history file is free in OSS pitest and merely asked someone to "confirm which capability we
want". `docs/inflight.md` records the opposite as an *already-verified* PR #69 finding: pitest 1.25.x
(we bumped 1.17.4 → 1.25.8 in #73) dropped the built-in file-based history entirely. Re-verified here
rather than picking between two documents - `-DwithHistory=true` on the current build:

```
[ERROR] History has been enabled but no history plugin has been installed/activated.
[ERROR] If you are using https://www.arcmutate.com remember to activate the history plugin with +arcmutate_history
```

So there is no free tier to check: history now lives entirely in **arcmutate**, and the work item is
obtaining and wiring a licence, not setting a flag. arcmutate is free for open-source projects, but that
needs the maintainer to sign up, and the licence is a file at the repo root - which on a *public* repo
means committing a key or plumbing a CI secret. `docs/inflight.md` has the full shelved plan.

**The automated reviewer flagged this contradiction five times before it was fixed.** Worth recording as
its own lesson: a repeated review finding that keeps being deferred is usually a real one, and this one
was cheap to settle - a single local run answered it. The plan doc and the inflight ledger disagreeing
about a verified fact is exactly the failure the ledger exists to prevent.

Everything below stands regardless, and is the reason this was already ranked last:

1. **A history file only helps a run that completes.** Ours never has — the full sweep aborts or times
   out, and the scoped lane mostly prints "nothing to mutate". There was never a successful run to
   cache, and a run that dies in the coverage stage writes nothing useful. This is an optimisation for
   *"completes, but slowly and repeatedly"*; we are at *"does not complete"*. **4.3 is the enabler, not
   this.**
2. **For scoped runs the dominant cost is the coverage pass, not the mutants** — 332s of instrumented
   full-suite execution, paid whether you mutate two classes or two hundred. Caching mutation results
   does not touch that. The lever for scoped runs is reducing what must be executed for coverage at
   all (narrower `targetTests`), not remembering previous mutant verdicts.

#### Where the history file would live, and why CI is the hard part

Kept for whenever the arcmutate licence question is settled, since none of this changes with the plugin
that produces the file. It is a plain generated file under the module build directory (`target/`, gitignored), or wherever
`historyInputFile` / `historyOutputFile` point. **It must never be committed:** it changes every run, so
it would conflict constantly, and its verdicts are valid only for one specific code state — a stale
committed file yields confidently wrong results, which is worse than no file at all, particularly given
PIT documents incremental analysis as experimental.

Two facts about our setup:

- **`bin/ci-mutation-test.sh` does not run `clean`** (it runs `test-compile ... mutationCoverage`), so
  *locally* a history file would survive between runs and start paying off straight away.
- **In CI every job starts from a fresh checkout with an empty `target/`**, so there is nothing to
  inherit. Carrying it between runs via `actions/cache` (or an artifact) **is** the work item — the
  pitest flag is the trivial part.

The non-obvious bit is cache-key design: key it per-SHA and you never get a hit; use a restore-key chain
so PR runs inherit master's history, and schedule a periodic full invalidation so stale verdicts do not
accumulate silently.

### 4.3 Retarget from `internal.*` to `offsets.*` — APPLIED in #111, not yet measured

The substantive change. The high-value target is the offset encoders/decoders: a silent bug there means
**lost or duplicated records**, and the mutants are *decidable* — pure-ish logic with deterministic
tests, so a survivor is a real gap rather than an unobserved race. `state.*` bookkeeping is a
reasonable second.

Caveat: `offsets.*` is not free either — `RunLengthEncoderTest` alone runs ~140s. Measure before
assuming it is quick.

The script's own posture — *"walk the scope back up as it proves fast enough"* — is right. The
suggestion is that it should walk **sideways**, to where mutants are decidable, rather than up to
everything.

**Done in #111, in both places.** `offsets.*` is the sweep default (`bin/ci-mutation-test.sh` and the
`mutation-full-sweep.yml` input), *and* the per-PR lane now intersects its changed-class list with the
same decidable set (`PIT_DECIDABLE_PACKAGES`, default `offsets.`), naming in the log and job summary any
changed class it declined to mutate.

The lane needed it as much as the sweep, which the first draft of this change missed: the sweep is the
path that runs least often, so retargeting only that would have left the recurring case exactly as bad
as before. A PR touching `internal.*` would still get hang-prone mutants and unfalsifiable survivors,
and the job timeout is no protection - it converts "slow" into "cancelled with nothing scored", which is
the same zero signal the sweep produced for months.

Measured while deciding it (12 cores, one changed `internal` class - `BrokerPollSystem`): the coverage
pass alone took **271s**, and mutation analysis of that *single* class had still not reached the
statistics stage four minutes later, when it was killed. The hosted PR lane has two cores. For contrast,
the same lane on one changed `offsets` class completes end to end in **430s** and scores 18 mutants.

**Widen the allowlist only on evidence.** `state.*` is the obvious next candidate, but it is bookkeeping
wrapped around the same concurrency, so it earns inclusion by measurement rather than by argument.

**Still unmeasured, which is the part that matters.** This is a config change made on the strength of
the argument above, not on a completed run - so it is a better-founded guess, not a result. Run it:

```
gh workflow run mutation-full-sweep                  # against master
gh workflow run mutation-full-sweep --ref <branch>   # against a branch
```

**Only once this has merged.** `workflow_dispatch` requires the workflow file to be on the *default
branch* before it can be dispatched at all - even when dispatching a different ref - so running it from
the PR branch fails with `could not find any workflows named mutation-full-sweep`. That is a GitHub
rule, and it means a workflow introduced by a PR cannot be exercised by that PR.

Either it completes - at which point a `schedule:` trigger and a history file both become real options,
and there is finally a mutation score worth quoting - or it doesn't, which is itself the answer to
whether mutation testing can work here at all.

**If the coverage pass turns out to dominate**, and only then, add
`-f target-tests='io.confluent.parallelconsumer.offsets.*'`. That is the one lever on the 332s
instrumented pass (§4.2), but it is deliberately *not* the default, because it trades accuracy for
speed: a mutant killed only by a test outside `offsets.*` would then be reported as no-coverage rather
than killed. Buy that trade knowingly rather than inheriting it.

### 4.4 Wire `excludedGroups` explicitly — VERIFY FIRST, it may be a no-op

Close §3.4 so the exclusion is intentional - **but** see the correction in §3.4: pitest may already be
importing our surefire `<excludedGroups>`. Establish which world we are in before writing config: add a
throwaway `@Quarantined` *unit* test, run PIT, and see whether it executes. Then either delete the scary
comment or wire the exclusion, but not both.

### 4.5 ~~Set `mutableCodePaths` explicitly~~ — VOID, not possible and not needed

Withdrawn. pitest-maven has no such parameter, and main-only is structural under Maven. See the
correction in §2. `<crossModule>false</crossModule>` is pinned in the pom in its place.

## 5. Keep

- **Per-PR scoping to changed main classes.** Correct instinct, and the reason PR runs are usable at
  all. The changed-class derivation reads `src/main/java/` only, so it is main-only by construction.
- **Non-gating / advisory.** Mutation score is a conversation starter, not a merge gate. Especially so
  while §3.2 stands.

## 6. One caveat when reading the checks — now self-declaring

`Mutation Tests (PIT, PR-scoped)` passing in ~17s on a test-only or CI-only PR is **not evidence of
anything** — it prints `no core main-source classes changed vs origin/master - nothing to mutate,
skipping` and exits green. A green mutation check means "nothing to do" at least as often as it means
"all mutants killed".

**Done in #111:** the advice used to be "read the log, not the tick", which requires knowing to be
suspicious of a green tick in the first place - a caveat that only helps the people who already know it.
Every exit path now writes to the GitHub job summary instead, so the check states its own meaning:

- **skipped** - says outright that a green tick here is not evidence about test quality;
- **scored** - the run totals (score, no-coverage count, test strength), with the `skipFailingTests`
  caveat attached to the no-coverage number where it is actually needed;
- **died before scoring** - says *that* explicitly rather than leaving a red tick to be read as "mutants
  survived", and names the failing tests if the green-suite abort was somehow reached.

Note the deliberate asymmetry: only the final `- Statistics` block is reported. PIT also prints a
per-class running tally in the identical `>> Generated N Killed M` shape, and pasting those into a
summary would show a reader several numbers that all look like the score.

**And the survivors are listed, because the score is not the product.** "50% killed" cannot be acted on
- it does not say which half, so it reads as a grade. Each survivor, by contrast, names a specific
behaviour that can be broken without any test noticing:

```
SURVIVED     OffsetSimpleSerialisation.java:38  removed call to ObjectOutputStream::writeObject
NO_COVERAGE  OffsetSimpleSerialisation.java:55  removed call to SnappyOutputStream::write
```

That is a work item. Parsed from `mutations.xml` (`-DoutputFormats=XML,HTML`) with `sed`, since pitest
writes one `<mutation>` element per line and an XML parser would be a dependency for no gain. Two
details worth keeping: sort the line numbers **numerically** (as text, 119 sorts above 38 and the table
reads as unordered), and strip the fully-qualified paths from the descriptions, since `file:line`
already locates the mutant and the package prefix is ~60 characters of noise per row. Capped at 50 rows,
and the cap is stated when it bites - a truncated list that looks complete is worse than no list.
