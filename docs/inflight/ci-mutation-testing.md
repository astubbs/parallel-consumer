# Mutation testing is deliberately narrow - re-widening is tracked here

<!-- inflight-priority: low -->

Shipped in astubbs#111. Recorded because it is a deliberate *reduction* in coverage, which is the kind of
thing a future session otherwise rediscovers as a bug.

**What runs automatically:** one per-PR job (`maven.yml`) that mutates *only changed classes*, and
*only* those within `PIT_DECIDABLE_PACKAGES` - currently `offsets.` alone. Nothing else is automatic:
the full sweep is dispatch-only (`mutation-full-sweep.yml`). So on most PRs mutation testing does
nothing, by design - elsewhere the mutants hang by construction and a survivor cannot be told from a
race that did not happen. Reasoning and measurements:
[`docs/plans/2026-08-03-002-mutation-testing-plan.md`](../plans/2026-08-03-002-mutation-testing-plan.md).

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

To re-widen, in order:

- ~~Run the sweep once and record the runtime~~ **DONE** - see the baseline above.
- **Then give it a trigger** - now unblocked, at a known 22 minutes per run. Prefer
  `push: branches: [master]` over a cron: the score changes only when the code does, so a nightly
  recomputes an identical answer whenever master did not move, and blames a date rather than a merge.
  Add a `concurrency` group with `cancel-in-progress` - only the latest master state is worth scoring.
  Still unwired - that is the next PR, and now a cost decision rather than a blocked one.
- **Then widen `PIT_DECIDABLE_PACKAGES`** - `state.` is the obvious next candidate, but it is
  bookkeeping around the same concurrency, so it earns inclusion by measurement, not by argument. The
  marginal cost is smaller than it looks: the 311s coverage pass is paid whatever the target, so adding a
  package costs only its own mutants.
- **`excludedGroups`: verify before "fixing".** pitest-maven's `parseSurefireConfig` defaults to true
  and may already import our surefire `<excludedGroups>`; a throwaway `@Quarantined` *unit* test
  settles it. The warning comment in `bin/ci-mutation-test.sh` may be obsolete.
- **`withHistory` is blocked on an arcmutate licence, not a flag** - re-verified 2026-08-04 that
  pitest 1.25.8 errors with *"no history plugin has been installed"*. arcmutate is free for
  open-source projects, but claiming that needs maintainer signup and a licence file at the repo root,
  which on a public repo means a committed key or a CI secret.
