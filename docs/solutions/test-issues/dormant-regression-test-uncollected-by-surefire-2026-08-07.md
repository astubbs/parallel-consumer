---
title: A regression test named ...Test909 matched none of Surefire's include patterns and ran zero times in 104 days
date: 2026-08-07
category: test-issues
module: parallel-consumer-core
problem_type: test_not_collected_by_surefire
component: testing
symptoms:
  - "The class never appears in any Surefire report XML between 2026-04-23 and 2026-08-05, on any run"
  - "Reverting the ProcessingShard fix produces no red build - the regression test meant to catch it never executes"
  - "Class name ends `...Test909`; the trailing issue number breaks the `*Test` suffix Surefire matches on"
  - "The PR stayed green for four months while carrying an unproven fix"
  - "Running the class alone with -Dtest= passes, because -Dtest bypasses collection entirely"
root_cause: surefire_include_pattern_mismatch_from_trailing_issue_number
resolution_type: renamed_to_match_convention
severity: high
tags:
  - surefire
  - test-collection
  - naming-convention
  - archunit
  - false-green
  - regression-test
---

# A regression test named ...Test909 matched none of Surefire's include patterns and ran zero times in 104 days

## Problem

The regression test proving the `confluentinc#909` fix - a stale `WorkContainer` at the same offset
blocking fresh work after a rebalance - was named `ProcessingShardStaleReplacementTest909`. Maven
Surefire collected it never. For 104 days the fix shipped in a PR whose "proof" had not run once.

## Symptoms

The defect's signature is the absence of a signal, which is what makes it survive:

- **Nothing.** No failure, no error, no skip. The suite is green, and green is what you were hoping
  for. Test counts go up over time from other work, so no single number looks wrong.
- The class appears in **no** Surefire report XML for the whole window.
- Reverting the fix in `ProcessingShard.addWorkContainer()` produces **no** red build.
- Running the class on its own passes, which is the cruellest part - `-Dtest=ClassName` selects by
  explicit name and **bypasses the include patterns entirely**. A developer who verifies their new
  test locally sees it go red-then-green exactly as intended, and concludes it works. The commit
  message for the `confluentinc#912` instance put it precisely: *"It passed locally only because
  `-Dtest=` bypasses collection."*

## What Didn't Work

Four months of ordinary safeguards, none of which can see this:

- **Code review.** The name reads perfectly naturally. `ProcessingShardStaleReplacementTest909` looks
  like a test for issue 909, because it is one. Nothing about it looks wrong to a human.
- **A green CI suite.** Green was the symptom, not the reassurance.
- **The author's own local verification**, defeated by `-Dtest=` as above.
- **Being on the wrong branch to be caught.** The PR was cut against `master-confluent`, the pinned
  pre-rebrand mirror, which never received the guard that `master` later grew. The gate existed for
  the last day of the window and simply wasn't in this branch's history.

## Solution

**Rename so the issue number precedes `Test`**, via `git mv` so history follows:

```
ProcessingShardStaleReplacementTest909   ->   ProcessingShardStaleReplacement909Test
                              ^^^^^^^^                              ^^^^^^^^
                    breaks the *Test suffix              issue number moved before Test
```

Surefire runs on Maven's unmodified defaults here - `pom.xml:713-732` declares only `<excludes>`,
never `<includes>` - so the patterns in force are `Test*.java`, `*Test.java`, `*Tests.java`,
`*TestCase.java`. `...Test909` matches none of the four.

Evidence it now runs, from the report rather than from confidence:

```
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
  -- in io.confluent.parallelconsumer.state.ProcessingShardStaleReplacement909Test
```

And evidence it is a real guard rather than two green assertions: reverting
`ProcessingShard.addWorkContainer()` sends `staleContainerAtSameOffsetShouldBeReplacedByFreshOne`
RED, while `nonStaleDuplicateAtSameOffsetShouldStillBeDropped` correctly stays green.

**The standing guard** is an ArchUnit rule that mirrors Surefire's own patterns, in
`TestConventionRules.java:83-99`:

```java
private static final ArchCondition<JavaClass> HAVE_A_NAME_SUREFIRE_COLLECTS =
        new ArchCondition<JavaClass>("be named so surefire collects it") {
            @Override
            public void check(JavaClass javaClass, ConditionEvents events) {
                String name = javaClass.getSimpleName();
                // mirrors surefire's default includes
                boolean collected = name.startsWith("Test")
                        || name.endsWith("Test")
                        || name.endsWith("Tests")
                        || name.endsWith("TestCase");
                if (!collected) {
                    events.add(SimpleConditionEvent.violated(javaClass, javaClass.getName()
                            + " has test methods but its name matches none of surefire's default includes "
                            + "(Test*, *Test, *Tests, *TestCase), so it is never executed - rename it"));
                }
            }
        };
```

applied at `TestConventionRules.java:117-139` to concrete, non-`@Nested`, non-interface classes
outside `..integrationTest..` packages, with `.allowEmptyShould(true)`.

Each exemption earns its place:

- **`@Nested`** - JUnit discovers these through their enclosing class, so filename patterns never
  apply. Exempted by the *annotation*, deliberately not by `isNestedClass()`: a plain inner class
  holding `@Test` methods genuinely is uncollected, and must still be flagged.
- **Interfaces and abstract classes** - their implementors are what run.
- **`..integrationTest..` packages** - Failsafe selects these by **path**, `**/integrationTest*/**/*.java`
  (`pom.xml:747-751`), not by class name, so an `*IT` name there is correct and would be a false positive.
- **`allowEmptyShould(true)`** - some modules have every test in an exempted integration package, so an
  empty match set there is a legitimate pass rather than a rule that failed to load.

## Why This Works

Surefire decides what to run from the **file name**, before any annotation is read. A class it does
not match is not "skipped" - it is never considered, so there is nothing to report and no count to
look wrong. Putting the issue number before `Test` restores the suffix the pattern matches on, and
the ArchUnit rule turns the invisible condition into a build failure that names the class and tells
you to rename it.

The deeper reason this needed a guard rather than a fix is that **it had already recurred three
times**, and the convention had to be learned the hard way each time:

| Issue | Broken name | Renamed to |
| --- | --- | --- |
| `confluentinc#859` | `PCMetricsTest859` | `PCMetrics859Test` |
| `confluentinc#912` | `JStreamMemoryLeakTest912` | `JStreamMemoryLeak912Test` |
| `confluentinc#909` | `ProcessingShardStaleReplacementTest909` | `ProcessingShardStaleReplacement909Test` |

Plus the three that prompted the rule's creation in astubbs#101 - `MockConsumerTestWithCommitTimeoutException`
and siblings, all named after the exception they exercised, all never run. Six instances of one
mistake. The pattern is stable enough to name: **an issue number or an exception name tacked onto the
end of a test class silently disables it.**

Worth recording honestly: this instance was **not** discovered by the retarget. A repo-wide sweep on
2026-08-04 checked all 180 refs against the git object store and already listed it - *"carries
`ProcessingShardStaleReplacementTest909` ... 2 tests that have never run"* - and the deliberate
decision was to leave it (session history):

> "I think all these problems, now that we have this arc unit rule, are gonna fix themselves right
> once we come back up on them again. I think that's okay."

That bet was correct. The branch tripped the gate on its next merge, exactly as predicted. A guard on
the trunk beats a migration across every branch, because branches come to you.

## Prevention

- **Name issue-numbered tests `<Subject><Issue>Test`**, never `<Subject>Test<Issue>`. Same for
  exception-named tests: the class must still *end* in `Test`.
- **The ArchUnit rule is the enforcement** and lives in every module that has tests (core, vertx,
  mutiny, reactor, and both example modules). Nothing further is needed for the naming form.
- **Never treat `-Dtest=ClassName` as proof a test will run in CI.** It bypasses collection. Confirm
  with a real report: `ls parallel-consumer-core/target/surefire-reports/ | grep <ClassName>`.
- **To sweep for other instances**, check names against the same predicate rather than trusting the
  suite; the 2026-08-04 sweep did this across all refs via the git object store without checking any
  branch out. A repeat sweep on 2026-08-07 found **no remaining instances**: every module with tests
  carries the gate, and six candidate inner classes were inspected and correctly ruled out (helper
  `Function`/`Iterable` implementations, and deliberate reflection fixtures whose `.class` is passed
  as data).

**What this guard still does not cover** - a green gating suite is not the same as everything having
run:

- **`@Disabled`** - correctly named, correctly collected, still never executed. Four occurrences in
  core today, e.g. `ParallelEoSStreamProcessorTest.java:369` and `:596`. Nothing flags a stale one.
- **`@Quarantined`** - excluded from the gating lane by design, run only via `bin/quarantined-test.sh`.
  Not a bug, but another reason the green tick is narrower than it looks.
- **Assertions inside an un-awaited async callback** - structurally invisible to ArchUnit, which sees
  names and types, not happens-before. Mutation testing (PIT) is the real defense: a test that never
  truly asserts tends to let mutants survive.
- **A rule whose own predicate stops matching.** `allowEmptyShould(true)` is correct here for the
  integration-only modules, but it is the same shape of trap: if `.that(...)` were later narrowed to
  match nothing, the rule would pass vacuously and silently. There is no test-of-the-test verifying
  the predicate is non-trivially exercised per module.

## Related Issues

- `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md` - the same
  `confluentinc#909` / astubbs#31 subject matter, from the main-code side.
- `docs/solutions/test-flakiness/unit-tests-parallelise-by-forking-not-threading-2026-07-29.md` -
  the other doc that touches `TestConventionRules`, for its integration-test isolation rule.
- `docs/solutions/workflow-issues/compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md` -
  kindred principle from a different domain: a tool's clean or empty result is not evidence.
- `AGENTS.md` is currently **silent** on this naming convention. It documents the test-exclusion
  patterns (`:169`) but not Surefire's default *includes*, nor the rule that a misnamed test is
  silently never collected. The gate is therefore the only line of defense, and it fires after the
  fact rather than telling a contributor the rule up front.
