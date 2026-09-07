---
title: "An inert analysis config reads as a clean codebase - assert a count, not a green build"
date: 2026-08-25
category: workflow-issues
module: build-system
problem_type: workflow_issue
component: development_workflow
severity: high
root_cause: config_error
resolution_type: workflow_improvement
status: "SOLVED for the five instances found. Each is now verified by an asserted count. No mechanical guard enforces the effective-pom check - it is a habit, not a gate."
applies_when:
  - Adding or changing any static-analysis tool, exclude filter, or compiler flag in a pom
  - A tool reports zero findings, or few, and you are about to read that as a clean codebase
  - Running a negative control arm, before believing the red arm went red for your reason
  - Reading an exit code from a command you piped into head, tail, or grep
  - Reviewing a CI lane that can exit zero without having graded anything
symptoms:
  - A SpotBugs exclude filter scoped on a source path silences nothing, because sourcepath is package-relative
  - "`-Xlint:all` reports 6 warnings against a real 172, having landed in a profile CI never activates"
  - A negative control where both arms exited 1 for unrelated reasons, so it proved nothing
  - A mutation-testing lane green on 40 of 40 runs, having scored zero mutants on every one
  - An exit code read through a pipe reports the pipe's status rather than the command's
tags:
  - static-analysis
  - build-config
  - maven
  - effective-pom
  - false-negative
  - silent-green
  - control-arm
  - exit-codes
related_components:
  - tooling
  - testing_framework
---

# An inert static-analysis config reports the same green as a clean codebase

## Context

Expanding static analysis on `ci/static-analysis-expansion` turned up five separate changes that
looked applied, produced no findings, and were completely inert. In every case the build stayed
green, and the absence of output read as a clean codebase. None of them was caught by a red build,
because none of them could ever produce one.

This is not the same failure as the tools already written up here. `docs/solutions/workflow-issues/
a-check-that-reports-success-without-having-run.md` catalogues checks that could not run at all - a
401, a refused action, an unresolvable fork point. What is new in this batch is that the tool **did**
run, successfully, over the real tree, and produced a genuine and correct report; the *configuration*
never reached it. There is no error to notice and no step to gate on. The only distinguishing
signal is the size of the finding count, which nothing was asserting.

The five, each verifiable in this repo:

| Change | Why it was inert | What the run reported | What it reports applied |
|---|---|---|---|
| SpotBugs test-scoping filter | `<Source name="~.*src/test.*"/>` - SpotBugs records `sourcepath` package-relative, so `src/test` never appears in it | Rule still firing; XML valid, build green | Filter now scopes by class name; see the `SCOPING LIMITATION, MEASURED NOT ASSUMED` block in `spotbugs-exclude.xml` |
| `-Xlint:all` in `maven-compiler-plugin` | Landed in the *first* of two plugin declarations, the one inside the `intellij-idea-only` profile, which CI never activates | **6** warnings, all from Lombok and the Truth generator | **172** warnings |
| `forbidden-apis` control arm | Both arms run with `-pl parallel-consumer-core`, both exit 1 | Exit 1 both sides, read as a working control | Over the full reactor: mutated exits 1 with one forbidden-method error, reverted exits 0 with none |
| PIT mutation lane | Every exit path returned 0, so a correct skip and a real run were the same tick | Success on 40 of the last 40 PR runs | Exit code is now the verdict; see `THE EXIT CODE IS THE VERDICT` in `bin/ci-mutation-test.sh` |
| Measuring a gate's exit status | `bin/check-branch-self-reference.sh 2>&1 \| tail -15` then `$?` reports **tail's** status | A confident "exits 0" | The script exits **127** |

The first four share one property: **green and inert are indistinguishable from outside the run.**
The fifth is the same mistake applied to the measuring instrument rather than to the thing measured,
and it is the one that put a wrong number into a script header comment on master before being
corrected.

## Guidance

**Verify by asserting a COUNT, never by observing that the build went green.** This is the whole
rule; everything below is it applied to a particular surface. After changing a suppression, assert
the rule you excluded reports zero. After enabling a check, assert it reports more than zero, or
name the positive control that proves it can. A green build is compatible with the config having
been applied, ignored, misspelled, or dropped into a dead profile; a count is not. The two rule
registries now carry this as a standing instruction under the same heading, `Verify by count, never
by "the build went green"` - `docs/inflight/static-spotbugs-rule-registry.md` and
`docs/inflight/static-error-prone-rule-registry.md`.

**For build config, prove the setting reached the run before believing any result from it.** Run
`./mvnw -Pci help:effective-pom` for the module in question and `grep -c` for the flag, asserting the
count is greater than zero. This applies to any layered configuration where a plugin can be declared
more than once: profiles, `pluginManagement`, module-level overrides, or a parent that a child
silently re-declares. Reading the diff is not the check - the diff of the inert `-Xlint:all` change
was correct XML in a real `maven-compiler-plugin` block. Only the effective pom distinguishes the
declaration that runs from the one that does not. This is AGENTS.md's "verify your instrumentation
actually reached the run", arriving in a pom instead of a log config.

**A control arm must fail for the reason you think it failed. Check *what* went red, not that
something did.** An arm that exits non-zero is not a control until you have read the error. Two arms
that both exit 1 for a reason unrelated to your mutation look exactly like a control that could not
distinguish them, which is what they were. In this reactor the specific trap is that `./mvnw -pl
<module>` without `-am` fails the enforcer's `ReactorModuleConvergence` rule - already documented in
`docs/investigating.md` - so a single-module control arm reddens on the enforcer and never reaches
your check at all. Run the whole reactor for a control arm here.

**Measure exit codes by redirect, never through a pipe.** `cmd 2>&1 | tail` then `$?` gives you
`tail`'s status. Redirect to a file and read `$?` directly, or read `${PIPESTATUS[0]}`. The root
`AGENTS.md` already documents this trap for git under worktree ownership; this is the same trap in a
different costume, and it produced a false "exits 0" for a script that exits 127.

**Two smaller rules the batch also produced.** javac caps output at 100 warnings per compilation and
says nothing when it truncates, so a visible count can read as the total when it is a tenth of it -
`-Xmaxwarns` is set to 100000 in the parent pom's `<compilerArgs>` block for exactly this reason.
And "the tool could not run" must never share an exit code with "the tool ran and the code is clean";
`bin/ci-mutation-test.sh`, `bin/check-shell-lint.sh` and `bin/lib/node-gate.sh` all reserve `2` for
the first.

## Why This Matters

A check that goes red when broken gets fixed. A check that goes green when broken is worse than no
check at all, because it also removes the prompt to add a working one - and here it does something
further: it manufactures a *positive* claim. Six warnings instead of 172 is not silence, it is a
report, and a report of six reads as a codebase in good shape. Forty green PIT runs read as forty
runs of mutation testing. An exclude filter that matches nothing reads as a filter doing its job. In
each case the wrong belief is more confident than no belief would have been.

The cost compounds because these changes are the ones nobody re-checks. A new engine is enabled once,
its output is read once, and the number recorded then becomes the baseline everything after is
compared against. The `-Xlint` inventory would have been recorded as 6, and the next person to widen
it would have measured their own change against a figure that was wrong by a factor of nearly thirty.
`docs/inflight/ci-build-hardening-register.md` names the shape in its opening lines - "a check that
dies or is blind reports the same green as a check that passed" - and this branch found five more
instances of it in a single pass, which suggests the base rate for new analysis config is high rather
than exceptional.

It also matters for how *this* repo files things. The `mapfile` case was originally written up as a
silent pass, which put it in the wrong class: it is the loud, harmless failure that
`docs/solutions/workflow-issues/gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md` defines its
subject *against*. Misfiling a loud failure as a silent one inflates the class and teaches the wrong
lesson about it, so the measurement error mattered twice - once for the number, once for the
taxonomy.

## When to Apply

- Enabling, upgrading, or reconfiguring any static-analysis engine, compiler flag, or linter.
- Adding or editing a suppression filter, exclude file, or off-set. A suppression that matches
  nothing is indistinguishable from one that works.
- Editing a `pom.xml` where the same plugin appears more than once, or where a profile can supply a
  competing configuration. Check the effective pom, not the diff.
- Running a control arm, negative control, or mutation to prove a gate can fail.
- Reading a green check on the PR that introduced that check. That is the single case where green
  carries the least information.
- Any time you are about to write down what a command exited with.

## Examples

**The SpotBugs filter that could never match.** `spotbugs-exclude.xml` originally scoped its
test-only entries with a `<Source>` path regex. SpotBugs writes `sourcepath` package-relative -
`bz/stub/parallelconsumer/mutiny/MutinyPCTest.java` - so `src/test` is not in the string being
matched and the entry could not fire.
<!-- file-refs: N/A - that package-relative string is the VALUE SpotBugs records, quoted as data to show why the matcher could not fire; it is not a path in this tree --> The XML was valid, the build was green, and the only thing that
caught it was a sanity check that `PREDICTABLE_RANDOM` had gone to zero. It had not. The file now
scopes by class name and documents the leak that choice costs, under `SCOPING LIMITATION, MEASURED
NOT ASSUMED`; excluding those rules globally instead would have dropped the one real main-code
`PREDICTABLE_RANDOM` finding, which is why the weaker discriminator is the better trade.

**The flag in the dead profile.** The parent `pom.xml` declares `maven-compiler-plugin` twice. The
first is inside the `intellij-idea-only` profile, activated by the `idea.maven.embedder.version`
property, which CI never sets. `-Xlint:all` went there. The build was green and reported 6 warnings,
all of them Lombok and Truth-generator noise rather than lint, which is precisely what a clean
codebase looks like. `./mvnw -Pci help:effective-pom -pl parallel-consumer-core` piped through
`grep -c 'Xlint:all'` returned zero. Moved into the real declaration, the same reactor reports 172:
deprecated `KafkaContainer` (37), unchecked conversion and unchecked calls (50), raw types (23),
deprecated `Truth8` (10), deprecated `RandomUtils.nextInt` (6). Recorded in
`docs/inflight/static-spotbugs-rule-registry.md` under "javac's own analysis, and the inert first
attempt".

**The control arm that proved nothing.** Verifying that `forbidden-apis` was able to fail, both arms
were run with `-pl parallel-consumer-core` and both exited 1. That reads as a working control: the
mutated arm failed, and so should it have. Both had actually failed on the enforcer's
`ReactorModuleConvergence` rule, with **zero** forbidden-method errors on either side, so the
mutation was not what turned anything red. Re-run over the full reactor, the arms separate: mutated
exits 1 with one forbidden-method error, reverted exits 0 with none. One term changed, outcome flips.
The same trap is recorded from Error Prone's side in
`docs/inflight/static-error-prone-rule-registry.md`, which notes it "has already happened once on
this branch".

**Forty green runs, zero mutants.** A sweep of the last 40 `maven.yml` `pull_request` runs found the
`Mutation Tests (PIT, PR-scoped)` check reported success 40 times and scored zero mutants 40 times:
23 with no core main-source change, 14 with changes outside the decidable package, none that scored a
mutant. Every skip was individually correct. The defect is that "correct skip", "stale scope that can
never match again" and "185 mutants killed" were the same green tick, because every path out of the
script exited 0 - the exact shape `AGENTS.md` warns about under "Confirm the mutation lane scored
mutants rather than trusting its tick". `bin/ci-mutation-test.sh` now answers in its exit code, the
scope regex is validated against the classes that exist before it decides anything, and the verdict
is the generated-mutant count rather than the presence of a statistics block.
`bin/test-ci-mutation-test.sh` fails every case against the pre-change script.
Measurements: `docs/inflight/ci-mutation-testing.md`.

**And the self-test did not catch everything, which is worth keeping.** The first cut of the
full-sweep prefix guard read `TARGET_CLASSES` as one string when it is a comma-separated list, so it
rejected a target the PR path builds itself, and the lane exited 2 on a run that had scored 27
mutants minutes earlier. Nothing in the self-test saw it; running the real lane end to end did. A
guard written against the inert-config class is itself a config change, and gets the same treatment.

**The mismeasured exit code.** `bin/check-branch-self-reference.sh 2>&1 | tail -15` followed by `$?`
reports tail's status. A `mapfile` failure on macOS bash 3.2 was written into that script's header as
"the script still exited 0 - a local run reporting success having checked nothing". Measured on
Darwin 25.5.0 with bash 3.2.57 against the pre-fix version, reading `$?` directly, it exits 127: the
script sets `set -euo pipefail` well above that line, so the missing builtin takes it down. The
corrected header names the mismeasurement rather than just the number - grep
`reports tail's status rather than the script's` - so the next person measuring a gate reaches for a
redirect.

## Related

Three docs in this directory now describe a green signal that means nothing. They are not
restatements of each other, and the division is deliberate - **cite the owner rather than repeating
it**.

- [`a-check-that-reports-success-without-having-run.md`](a-check-that-reports-success-without-having-run.md)
  owns **the check could not run and said success anyway**: guard design, the third state, external
  guards. It already carries instance 5 of this doc, the piped `$?`. Its diagnostic question is
  "what does this check do when it cannot run", and the four config instances here answer
  "irrelevant - it ran fine". That is the seam between the two.
- [`negative-results-need-an-instrument-that-could-have-said-yes.md`](negative-results-need-an-instrument-that-could-have-said-yes.md)
  owns **an investigation's negative came from a mispointed or cached instrument**: positive
  controls, and preferring instruments that report their denominator.
- **This doc owns the remaining case**: the setting was applied to the file and never reached the
  run, so the tool executed correctly against a configuration that was not the one you wrote. There
  is no error to notice and no step to gate on - only the count.

Adjacent, same genus, different mechanism, and worth reading together rather than merging:
[`gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md`](gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md)
(a construct accepted on one platform and meaning something else on another) and
[`gh-run-view-log-truncation.md`](gh-run-view-log-truncation.md) (a truncated log read as complete
data, which is this shape applied to the evidence rather than the tool).

`docs/investigating.md` carries the parent rule, **"Verify your instrumentation actually reached the
run"**, and owns it. What is added here is that the rule extends to build configuration, where the
"instrumentation" is a plugin declaration and the check is the effective pom rather than the log.
