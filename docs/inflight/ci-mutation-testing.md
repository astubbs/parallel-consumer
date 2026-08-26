# Mutation testing is deliberately narrow - re-widening is tracked here

<!-- inflight-type: register -->
<!-- inflight-impact: blind-spot -->

Shipped in astubbs#111. Recorded because it is a deliberate *reduction* in coverage, which is the kind of
thing a future session otherwise rediscovers as a bug.

**What runs automatically:** one per-PR job (`maven.yml`) that mutates *only changed classes*, and
*only* those within `PIT_DECIDABLE_PACKAGES` - currently `offsets.` alone. Nothing else is automatic:
the full sweep is dispatch-only (`mutation-full-sweep.yml`). So on most PRs mutation testing does
nothing, by design - elsewhere the mutants hang by construction and a survivor cannot be told from a
race that did not happen. Reasoning and measurements:
[`docs/plans/2026-08-03-002-mutation-testing-plan.md`](../plans/2026-08-03-002-mutation-testing-plan.md).

## The lane was reporting green over nothing, 40 runs out of 40

**Measured 2026-08-25, and the answer is *scope*, not a stale regex.** The build-hardening register
suspected the `io.confluent` to `bz.stub` rename had staled the package regex, which `AGENTS.md`
warns exits 0 printing "nothing to mutate, skipping". It had not: the regex, the FQCN derivation and
the whole PIT invocation are correct post-rename, and a scoped run scores mutants normally (below).

What the log read actually found is worse in one way and better in another. Over the last **40**
`maven.yml` `pull_request` runs the `Mutation Tests (PIT, PR-scoped)` check reported **success 40
times and scored zero mutants 40 times**:

| Outcome | Runs |
|---|---|
| Skipped - no core main-source class changed at all | 23 |
| Skipped - changed classes all outside `offsets.` | 14 |
| **Scored any mutant** | **0** |
| Still in flight when sampled | 3 |

Every one of those skips was individually correct. The defect is that "correct skip", "stale scope
that can never match again" and "185 mutants killed" were the same green tick - because *every path
out of the script exited 0*. Red was always honest here and green never was; the check row does
render red when the step fails, `continue-on-error` notwithstanding, which
[`ci-mutation-lane-skip-reads-as-a-pass.md`](ci-mutation-lane-skip-reads-as-a-pass.md) measured.
Nothing ever made the step fail.

The 14 not-decidable runs are the interesting half, because they say what a widened scope would
have bought. The classes they declined to mutate were, in order of frequency,
`internal.admission.*` (the control-law work), `internal.*`, `state.PartitionStateManager`,
`state.WorkManager`, `metrics.PCMetricsDef`, and the top-level `CommitFailure*` /
`ParallelConsumerOptions` group.

### What changed, and the contract that replaces the tick

`bin/ci-mutation-test.sh` now answers in its **exit code**, and its header owns the contract: `0`
scored at least `PIT_MIN_MUTANTS` mutants, `2` cannot run, `3` nothing in scope, otherwise PIT's own
status. Three guards produce the `2`:

- The decidable regex is validated against the classes that exist **before it decides anything**, on
  every run including ones that would have skipped. A rename that stales it is caught by the next PR
  of any kind, not by the next PR that touches the one package it names.
- The full-sweep glob's literal prefix is checked the same way.
- The run's verdict is the **generated-mutant count**, not the presence of a statistics block. A
  tidy, complete block over zero mutants is the harder half of the vacuity problem to spot.

`maven.yml` maps those codes: `3` posts a `::notice::` and passes, `2` and PIT's own failures fail
the step and so redden the row, survivors gate nothing. `continue-on-error: true` stays on the job,
so none of this gates a merge - the change is that the row now has more than one colour available to
it. Whether a *skip* should render grey instead of green is a separate, open decision that belongs
to [`ci-mutation-lane-skip-reads-as-a-pass.md`](ci-mutation-lane-skip-reads-as-a-pass.md).

`bin/test-ci-mutation-test.sh` holds it there. Fifteen cases, six of them red controls, each with a
green near-miss one package name or one mutant away; it feeds canned PIT logs through the verdict so
the whole file runs in seconds without maven. **Against the pre-change script all fifteen fail**
(0 passed, 15 failed) - including the first arm, which is the `AGENTS.md` scenario exactly: a stale
`io.confluent` regex, over which the old script exited **0**.

**The self-test did not catch everything, and saying where is the point.** The first cut of the
full-sweep prefix guard read `TARGET_CLASSES` as one string when it is a comma-separated list, so it
rejected `Foo,Foo$*` - a live target the PR path builds itself - and the lane exited 2 on a run that
had scored 27 mutants minutes earlier. Nothing in the self-test saw it; running the real lane
end-to-end did, which is the only reason the guard is not now a second inert-config bug of its own.
Both the comma list and a bare `*` are arms now. The end-to-end run after the fix scores the same
**27 generated, 23 killed** as the pre-change script, so the restructuring changed no PIT behaviour.

## Baseline - first completed sweep, 2026-08-05

The `offsets.*` sweep **completes**, which the `internal.*` one it replaced never did in 42+ minutes on
CI or 83+ locally. Recorded here because nothing else keeps it: the PIT report is a 14-day artifact and
the statistics live only in a job log, which ages out and which a re-run silently replaces.

| | |
|---|---|
| Mutation score | **83%** - 185 generated, 153 killed |
| Test strength | **92%** |
| No coverage | 19 |
| Line coverage (mutated classes) | 481/597 (81%) |
| Tests run | 1392 (7.52 per mutation) |
| **Wall clock** | **21m55s** - of which **311s** is the instrumented coverage pass |
| Run | `mutation-full-sweep`, first run, against master at `58991506` |

Two things follow from the runtime. It is schedulable, so the `push: branches: [master]` decision below
is now a cost question with a real number rather than a guess. And **narrowing `target-tests` is not
worth it**: coverage is 311s of 1315s, about 24%, so the accuracy it would cost buys little.

Compare future sweeps against this. A score that moves without a deliberate test change is the signal
worth acting on.

## `state.` is NOT the next candidate - measured 2026-08-25, and it hangs

This entry used to read "`state.` is the obvious next candidate, but it earns inclusion by
measurement, not by argument." It was measured, and the measurement refutes it.

**Control arm, one term changed.** Same script, same machine (12 cores, JDK 17.0.18-tem), same
`targetTests=bz.stub.parallelconsumer.*`, same timeouts, same everything - only `targetClasses`
differs:

| `targetClasses` | Coverage pass | Total wall clock | Mutants scored |
|---|---|---|---|
| `offsets.OffsetRunLength` (+ nested) | 53s | **2m10s, BUILD SUCCESS** | 27 generated, 23 killed, 4 survivors, 88% test strength |
| `state.*` | 325s | **killed at 32m32s**, i.e. past the CI job's whole 30-minute budget | **0** - not one per-class tally line was ever printed |

The `state.*` mutation phase therefore ran for about **27 minutes producing no output whatsoever**,
where the `offsets.` one finished in well under a minute. Under `timeoutConstant=30000` with
`timeoutFactor=3.0`, individual minion JVMs stayed alive for sixteen continuous minutes: hung, not
slow. That is the failure mode `internal.*` is excluded for, arriving in the package that was
supposed to be the safe step sideways - unsurprising in hindsight, since `state.` is bookkeeping
around the same concurrency and the plan doc says so. `PartitionStateManager`, `ShardManager` and
`WorkContainer` are exactly the classes the torn-read family lives in.

So the scope stays at `offsets.` and the lane will keep skipping most PRs. **That is now visible
rather than disguised**, which was the actual problem: a `3` and a notice, not a green tick.

## Ranked: what to widen to next, and the criterion

**Criterion: value over cost, where value is how close the package sits to a defect class this repo
has paid for, and cost is the measured risk of hang-prone mutants plus runner minutes.** Each entry
names what has to be measured before it is switched on - nothing here goes in by argument, which is
the mistake `state.` was about to be.

1. **The top-level `bz.stub.parallelconsumer.` package, non-recursive.**
   `CommitFailurePolicies`, `CommitFailureContext`, `ParallelConsumerOptions`,
   `OffsetCommitBudgetExceededException`. Configuration and policy objects: no locks, no timing, so
   a survivor is decidable on the same grounds `offsets.` is. It appeared in the not-decidable list
   above, so it is live traffic rather than a guess, and `CommitFailurePolicies` is new code from an
   in-flight branch, which is where an unasserted branch is likeliest. **Measure first:** one
   full-sweep run against `bz.stub.parallelconsumer.*` minus the sub-packages; it has to complete
   inside the 30-minute job timeout with a non-zero score. A regex for the non-recursive case is
   `^bz\.stub\.parallelconsumer\.[A-Z]`.
2. **`metrics.`.** `PCMetricsDef` and friends are declarative, and metrics correctness has already
   cost this repo a leak (astubbs#57). Small, so the marginal cost over the 325s coverage pass is
   near zero. **Measure first:** same completion test. Lower than 1 only because the defect class is
   narrower.
3. **`state.` under a real per-mutant timeout, not under today's settings.** The hang is the
   blocker, not the package. If `timeoutConstant` were low enough that a hung mutant died fast, the
   survivors would still be undecidable for the timing-based tests - but `ShardKey`,
   `ConsumerRecordId` and `WorkContainer`'s value logic are not timing-based, and a *class-level*
   allow-list inside `state.` would sidestep the whole argument. **Measure first:** the three named
   classes individually, one run each. Do NOT lower `timeoutConstant` to make the package finish:
   PIT counts `TIMED_OUT` as `KILLED`, so a short timeout manufactures kills - the
   `mutation-full-sweep.yml` header says this outright.

**Deliberately left out:** `internal.` and `internal.admission.` - the largest group in the
not-decidable list and the least usable, for the reason the plan doc gives and `state.` has now
confirmed by measurement. Padding this list with the thing nobody should do next is how rankings
stop being read.

### Exclusions, with a reason and a re-enable trigger each

Read [`static-analysis-rule-profiles.md`](static-analysis-rule-profiles.md) for what the profile
marker means. **Every exclusion here is `profile: old`, and the reason is structural rather than a
judgement call:** the PR lane is already diff-scoped, so it only ever mutates code somebody just
changed. There is no "on for new code" left to switch on - PIT *is* the new-code profile - so a
package that is off is off for new code too, permanently, until its trigger fires.

| Excluded | Why | Re-enable trigger | Profile |
|---|---|---|---|
| `internal.`, `internal.admission.` | mutants to locks, loop conditions and timeouts hang rather than dying; covering tests are timing-based, so a survivor cannot be told from a race that did not happen | a deterministic harness for the concurrency core (Lincheck, astubbs#347) that can decide a survivor | `old` |
| `state.` | measured above: no mutant scored inside the CI job's whole timeout | per-class allow-list for the non-timing classes, each measured on its own - ranked 3 | `old` |
| `metrics.` | never measured, not excluded on evidence | ranked 2 - one completion measurement | `old` |
| top-level `bz.stub.parallelconsumer.` | never measured, not excluded on evidence | ranked 1 - one completion measurement | `old` |
| everything in other modules | SpotBugs measured zero findings across vertx, reactor and mutiny; they are six source files between them | those modules growing | `old` |

## The rest of the re-widening list

- ~~Run the sweep once and record the runtime~~ **DONE** - see the baseline above.
- **Then give it a trigger** - now unblocked, at a known 22 minutes per run. Prefer
  `push: branches: [master]` over a cron: the score changes only when the code does, so a nightly
  recomputes an identical answer whenever master did not move, and blames a date rather than a merge.
  Add a `concurrency` group with `cancel-in-progress` - only the latest master state is worth scoring.
  Still unwired - now the *only* way the lane scores anything regularly, given that the PR lane
  correctly skips most PRs, so this is a bigger lever than it looked when it was written.
- **`excludedGroups`: verify before "fixing".** pitest-maven's `parseSurefireConfig` defaults to true
  and may already import our surefire `<excludedGroups>`; a throwaway `@Quarantined` *unit* test
  settles it. The warning comment in `bin/ci-mutation-test.sh` may be obsolete.
- **`withHistory` is blocked on an arcmutate licence, not a flag** - re-verified 2026-08-04 that
  pitest 1.25.8 errors with *"no history plugin has been installed"*. arcmutate is free for
  open-source projects, but claiming that needs maintainer signup and a licence file at the repo root,
  which on a public repo means a committed key or a CI secret.


## Corroboration (2026-08-21)

[`next-formal-verification-and-correctness-methods.md`](next-formal-verification-and-correctness-methods.md)
notes that a competitor reaches the same conclusion this work rests on - that coverage measures what
executed, not what was verified - and pairs mutation testing with formally-derived invariants
asserted directly in unit tests. That pairing is the part we do not have.
