# Build-hardening register: additions ranked by what this month actually proved

<!-- inflight-type: register -->
<!-- inflight-impact: ci -->

Every entry in the first list is grounded in a failure observed in this repo in August 2026, not in
a tool catalogue. The recurring shape behind almost all of them: **a check that dies or is blind
reports the same green as a check that passed.** Rank order is proven-value first.

<!-- post-merge: checked-begin -->
**This register was written on astubbs#344, an encoder-race fix rather than a CI change, and moved
here so the CI work did not wait on it.** Both sides then held the path with no common ancestor, so
astubbs#344 hit the add/add conflict this file predicted and resolved it by taking the version here -
the superset, which is what the prediction said to do. The reconciliation is complete; it is recorded
because the deliberate choice not to reach onto another open PR's branch to pre-empt the conflict is
the part a reader would otherwise mistake for an oversight.
<!-- post-merge: checked-end -->

## What this register was, and where its work went

**All thirteen entries were struck through, which is the state this file said should end it:
"when somebody has read the struck entries, delete them and this file becomes short again."** They
are deleted here. The register did its job - it was a ranked backlog, every item of which has now
landed - and what it must not become is a monument to finished work that a future reader has to read
before discovering there is nothing in it.

The reasoning behind each entry did not go into the void; it went where the work is, so the notes
below are the successors rather than an index:

| What it covered | Where it lives now |
|---|---|
| SpotBugs detectors, the rule filter, `-Xlint` | [`static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md) |
| Error Prone and NullAway | [`static-error-prone-rule-registry.md`](static-error-prone-rule-registry.md) |
| The PIT mutation lane | [`ci-mutation-testing.md`](ci-mutation-testing.md) |
| RacerD | [`static-infer-findings.md`](static-infer-findings.md) |
| The ShellCheck lane | [`static-shell-lint-severity-tiers.md`](static-shell-lint-severity-tiers.md) |
| `forbidden-apis`, and the `parallelStream()` ban still to come | [`static-forbidden-apis-parallelstream.md`](static-forbidden-apis-parallelstream.md) |
| The `dependencyConvergence` tail | [`deps-convergence-tail.md`](deps-convergence-tail.md) |
| Running full rules on new code only | [`static-analysis-rule-profiles.md`](static-analysis-rule-profiles.md) |

The one thing that had no successor is what remains below: the list of tools already in place or
deliberately rejected. That is a **register** in the strict sense - consulted, never completed - and
it is why this file still exists at all.

## Already in place, and explicitly not proposed

**Already in place, so not re-proposed:** the chaos lane, the quarantine lane, dup-code and
similarity gates, CodeQL default setup, the copyright/issue-ref/file-ref/inflight-tag gates, the
Lincheck lane (astubbs#347) and jcstress probe module (astubbs#348) once merged.

**Not proposed, with reasons:** a jcstress CI lane (its only FORBIDDEN outcomes live in control arms
that cannot go red when product code changes - a green run would assert the JVM honours `volatile`);
Checker Framework (annotation burden across a Lombok-heavy codebase, poor fit); racing-double
unification tooling (already tracked with the doubles themselves).

**Ruled out by the survey, with reasons** - recorded so the names do not arrive a third time: LGTM
(shut down, folded into CodeQL, already here); Sonatype Lift (shut down); OWASP dependency-check
(duplicate of the OSS Index lane, Dependabot and `dependency-review`); SonarQube and SonarLint
(dashboards and organisation-wide trend visibility, wrong shape here, overlaps CodeQL); Coverity and
Checkmarx (commercial); Qodana (OSS tier exists, but largely repackages engines already listed);
Checkstyle and Spotless (EditorConfig covers the rules; unenforced in CI is a real gap but a
cosmetic one, and this register is about checks that report green while blind).
