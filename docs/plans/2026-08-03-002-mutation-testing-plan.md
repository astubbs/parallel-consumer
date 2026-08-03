# Mutation testing: what we have, why it under-delivers, and what to change

**Status:** analysis only — parked deliberately, not scheduled. Nothing here is urgent.
**Written:** 2026-08-03
**Context:** written while fixing the `ProducerManagerTest` flake (PR #110) that had been aborting the
PIT lane. That fix restores the lane; this document is about whether the lane is pointed anywhere
useful once it runs.

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

They are not mutated in practice: pitest-maven defaults `mutableCodePaths` to the module's main output
(`target/classes`), so test classes are never candidates. Our numbers are not polluted. But note what
protects us — an unstated default, not our configuration. The glob does not say "main only"; it merely
fails to matter.

**Why mutating tests would be wrong**, if we ever widened `mutableCodePaths`:

It removes the oracle and keeps the subject. Mutation testing *is* the test of your tests; mutating the
tests asks "what tests the test-of-tests", and there is nothing left to answer with.

The mechanical harm is worse than "meaningless". Mutate an assertion in test `T` — flip `isTrue()` to
`isFalse()`. PIT runs the tests covering that line. The test covering `T`'s assertion **is** `T`. So `T`
fails, and the mutant is recorded as **KILLED**.

Nearly every test mutant kills itself. Mutation score is kills over total mutants, so padding the target
set with self-killing mutants drives the score **toward 100% while adding no information about the main
code at all**. The score rises as the signal falls — the worst property a metric can have, because it is
most reassuring exactly when it is least meaningful.

**Action:** set `mutableCodePaths` explicitly, so this is guaranteed by intent rather than by a default
nobody chose.

### The legitimate adjacent case

We do have test-tree code that is really production-like infrastructure — `PCModuleTestEnv`,
`PausableWorkManager`, `LimitedDynamicExtraLoadFactor`. Complex enough to harbour bugs, and a bug there
silently weakens every test built on it.

The answer is *not* to mutate it in place. It is to give that infrastructure its own tests, at which
point it is an ordinary subject with an ordinary oracle.

## 3. Why the current setup under-delivers

### 3.1 The full sweep has never completed

`bin/ci-mutation-test.sh` says so itself: *"The full internal.* sweep is impractically slow (it has
never completed on CI)."* Observed on PR #110: still running at 42 minutes on CI, and 83+ minutes
locally with minions dying on `MEMORY_ERROR` and `TIMED_OUT`.

An unbounded sweep that never finishes scores **zero** mutants. It is not a slow signal, it is no
signal, and it costs high-CPU runner time on every PR to produce it.

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

## 4. Proposed changes

Nothing here is scheduled. Listed so the reasoning is not lost.

Roughly in payoff order, with one deliberate exception: **4.2 is numbered here for continuity but
belongs last** — see the reasoning in that section. **4.3 is the one that unblocks everything else.**

### 4.1 Move the full sweep off PRs, onto a schedule

It has never completed, produces no signal, and burns runner time on every PR. An unbounded sweep
belongs on a nightly with a generous timeout — where taking hours is fine.

### 4.2 Incremental analysis (`withHistory`) — LATER, and it is not the lever it looks like

PIT's OSS history file (`historyInputFile` / `historyOutputFile`) lets a run re-analyse only what
changed. Worth having eventually, but **not** the early win it first appears to be, for two reasons:

1. **A history file only helps a run that completes.** Ours never has — the full sweep aborts or times
   out, and the scoped lane mostly prints "nothing to mutate". There was never a successful run to
   cache, and a run that dies in the coverage stage writes nothing useful. This is an optimisation for
   *"completes, but slowly and repeatedly"*; we are at *"does not complete"*. **4.3 is the enabler, not
   this.**
2. **For scoped runs the dominant cost is the coverage pass, not the mutants** — 332s of instrumented
   full-suite execution, paid whether you mutate two classes or two hundred. Caching mutation results
   does not touch that. The lever for scoped runs is reducing what must be executed for coverage at
   all (narrower `targetTests`), not remembering previous mutant verdicts.

**Check before relying on it:** the basic history file is free in OSS pitest; the git-aware,
change-based incremental analysis is part of **arcmutate** (pitest Pro), which is commercial. Confirm
which capability we actually want and whether the free tier covers it, rather than assuming.

#### Where the history file lives, and why CI is the hard part

It is a plain generated file under the module build directory (`target/`, gitignored), or wherever
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

### 4.3 Retarget from `internal.*` to `offsets.*`

The substantive change. The high-value target is the offset encoders/decoders: a silent bug there means
**lost or duplicated records**, and the mutants are *decidable* — pure-ish logic with deterministic
tests, so a survivor is a real gap rather than an unobserved race. `state.*` bookkeeping is a
reasonable second.

Caveat: `offsets.*` is not free either — `RunLengthEncoderTest` alone runs ~140s. Measure before
assuming it is quick.

The script's own posture — *"walk the scope back up as it proves fast enough"* — is right. The
suggestion is that it should walk **sideways**, to where mutants are decidable, rather than up to
everything.

### 4.4 Wire `excludedGroups` explicitly

Close §3.4 so the exclusion is intentional.

### 4.5 Set `mutableCodePaths` explicitly

Close §2 so main-only is guaranteed rather than inherited.

## 5. Keep

- **Per-PR scoping to changed main classes.** Correct instinct, and the reason PR runs are usable at
  all. The changed-class derivation reads `src/main/java/` only, so it is main-only by construction.
- **Non-gating / advisory.** Mutation score is a conversation starter, not a merge gate. Especially so
  while §3.2 stands.

## 6. One caveat when reading the checks

`Mutation (PIT, scoped)` passing in ~17s on a test-only or CI-only PR is **not evidence of anything** —
it prints `no core main-source classes changed vs origin/master - nothing to mutate, skipping` and
exits green. A green mutation check means "nothing to do" at least as often as it means "all mutants
killed". Read the log, not the tick.
