---
title: "A clone detector cannot see code embedded in another language's file - and widening its scope does not help"
date: 2026-08-19
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: medium
root_cause: tool_capability_limit
resolution_type: workflow_improvement
applies_when:
  - The same logic is needed by a CI workflow step and by a local script
  - Reading a green duplication report as evidence that a change introduced no duplication
  - Deciding where to put logic that both a workflow and a script need
  - Choosing the scan scope for a duplication or static-analysis tool
symptoms:
  - A duplication report says "no new clones" on a PR that copy-pasted a block verbatim
  - The same routine exists in a workflow YAML and in a shell script, and drifts between them
  - Widening a scanner's directory list finds more clones but reports a LOWER duplication percentage
tags:
  - duplication
  - tooling-scope
  - tooling-limits
  - ci
  - dry
---

# A clone detector cannot see code embedded in another language's file

Companion to
[`duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md`](duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md),
which found the scanners pointed at the wrong **directories**. This is the limit that remains after
that is fixed, and it is a property of the instruments rather than their configuration.

## What happened

`astubbs/parallel-consumer#320` added a gate that needs to read a git tree two ways: once in a CI
workflow step, once in the local mirror script. The `cat-file --batch` plumbing was written out
twice - about 22 identical lines each side, including the part that actually matters, slicing each
object's body by the **byte** length git reports.

Both duplication engines reported the PR clean. Two automated review passes read the diff and did
not name it either. The repo owner spotted it by reading the change.

## Why both engines were structurally unable to see it

Not scope this time - `bin/` and `.github/` were in scope by then. **Format.**

- **PMD CPD parses per language.** It was analysing Java. A `.yml` file is not Java, and a `.sh`
  file is not Java, so neither side of the clone was ever tokenised.
- **jscpd keys format off the file extension** and compares within a format. One side lived in
  `.github/workflows/pr-checklist.yml`, the other in `bin/check-file-refs.sh`. A YAML file and a
  shell file are different formats, so their contents are never compared with each other.

The duplicated text was JavaScript **in both places** - embedded in a YAML string on one side and a
shell heredoc on the other. Neither engine has a notion of "the language inside this string", and
adding directories does not give them one.

## The rule

**Logic needed by both a workflow and a script belongs in a file of its own, in its own language.**
Not because duplication is untidy - because it is the one arrangement any tool can check, and every
review can see. In this repo that means `.github/scripts/*.js` required by both callers, which is
the shape the existing `issue-ref-gate.js` and `changelog-ref-gate.js` already use.

When a workflow step starts growing a `script:` body worth testing, that is the signal to move it
out, not to write it more carefully.

## The trap in the numbers

Widening the same scanners from four Java module directories to the whole repo produced this:

| | before | after |
|---|---|---|
| CPD clones | 24 | **27** |
| CPD duplication | 1.89% | **0.11%** |
| jscpd clones | 68 | **70** |
| jscpd duplication | 2.52% | **1.15%** |

**More clones, more duplicated lines, a much lower percentage** - because the denominator grew
faster than the duplication in it. Read carelessly, "duplication fell to 0.11%" is a story about
code quality; nothing about the code changed at all.

The consequence was a guard that stopped guarding: the absolute cap was 5%, which a 0.11% reading
can never reach. Two guards, and only one of them survives a corpus change:

- **increase vs base** is denominator-proof - both sides are measured with the same configuration -
  and it is what catches a regression in a PR;
- **an absolute cap** is corpus-relative, and its job is the slow accumulation the increase guard
  structurally cannot see (twenty PRs at +0.09% each all pass, and total +1.8%). It has to be
  re-argued whenever what is scanned changes materially.

## What to take from it

- **A green duplication report means "no clone of a kind these engines can express".** For anything
  embedded - code in YAML, SQL in a string, a script in a Dockerfile - it is not evidence.
- **Check whether the instrument could have said yes** before treating its silence as a result. The
  general form is in
  [`negative-results-need-an-instrument-that-could-have-said-yes.md`](negative-results-need-an-instrument-that-could-have-said-yes.md).
- **A percentage is a ratio, so widening scope moves it for reasons unrelated to quality.** Re-read
  any threshold expressed as a percentage after changing what is measured.
- **Scope fixes are cheap and worth doing anyway.** The widening in that PR was already a tracked
  follow-up from the 2026-08-12 write-up; it is just not a fix for this class.

## See also

- [`duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md`](duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md) -
  the scope half, and the paraphrase limit, which is the other thing these engines cannot see.
- [`docs/inflight/ci-untriaged-clone-backlog.md`](../../inflight/ci-untriaged-clone-backlog.md) -
  the clones the widening exposed, which nobody has read yet.
