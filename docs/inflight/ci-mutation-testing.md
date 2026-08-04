# Mutation testing is deliberately narrow - re-widening is tracked here

Shipped in #111. Recorded because it is a deliberate *reduction* in coverage, which is the kind of
thing a future session otherwise rediscovers as a bug.

**What runs automatically:** one per-PR job (`maven.yml`) that mutates *only changed classes*, and
*only* those within `PIT_DECIDABLE_PACKAGES` - currently `offsets.` alone. Nothing else is automatic:
the full sweep is dispatch-only (`mutation-full-sweep.yml`). So on most PRs mutation testing does
nothing, by design - elsewhere the mutants hang by construction and a survivor cannot be told from a
race that did not happen. Reasoning and measurements:
[`docs/plans/2026-08-03-002-mutation-testing-plan.md`](../plans/2026-08-03-002-mutation-testing-plan.md).

To re-widen, in order:

- **Run the sweep once and record the runtime** - `gh workflow run mutation-full-sweep`. Nothing has
  completed under the `offsets.*` target. **Do not quote a mutation score until one has.**
- **Then give it a trigger.** Prefer `push: branches: [master]` over a cron: the score changes only
  when the code does, so a nightly recomputes an identical answer whenever master did not move, and
  blames a date rather than a merge. Add a `concurrency` group with `cancel-in-progress` - only the
  latest master state is worth scoring. Deliberately NOT wired until the runtime is known.
- **Then widen `PIT_DECIDABLE_PACKAGES`** - `state.` is the obvious next candidate, but it is
  bookkeeping around the same concurrency, so it earns inclusion by measurement, not by argument.
- **`excludedGroups`: verify before "fixing".** pitest-maven's `parseSurefireConfig` defaults to true
  and may already import our surefire `<excludedGroups>`; a throwaway `@Quarantined` *unit* test
  settles it. The warning comment in `bin/ci-mutation-test.sh` may be obsolete.
- **`withHistory` is blocked on an arcmutate licence, not a flag** - re-verified 2026-08-04 that
  pitest 1.25.8 errors with *"no history plugin has been installed"*. See
  [`parked-quarantine-lane-foss.md`](parked-quarantine-lane-foss.md) for the same OSS-licence
  question in another form.
