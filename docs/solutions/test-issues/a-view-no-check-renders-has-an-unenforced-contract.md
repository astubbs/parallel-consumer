---
title: "A view no check renders has an unenforced contract, however carefully the query underneath it is tested"
date: 2026-09-07
category: test-issues
module: tooling
problem_type: test_gap
component: development_workflow
severity: high
applies_when:
  - "A command is split into a query layer that returns data and a view layer that renders strings, and the tests drive the query layer"
  - "Adding a subcommand, report, or formatter to an existing front door whose checks all call the library function"
  - "A check asserts on a data field (a flag, an enum, a relation) whose user-facing wording is written out somewhere else"
  - "A mutant dies, and you have not asked which line of it the assertion actually observed"
  - "Deciding whether a legibility defect - a missing next-step command, an empty section, an unqualified count - is worth a check"
symptoms:
  - "Rendered output contains a claim the data model does not make, and every check is green"
  - "A scoped query with no results prints nothing at all, indistinguishable from a dropped section"
  - "A whole-corpus count sits among scope-limited lines and reads as if it described the scope"
  - "A negative control passes because its mutation was observed by an unused data field rather than by the behaviour the check is named for"
root_cause: test_gap
resolution_type: test_fix
tags:
  - vacuous-control
  - mutation-testing
  - renderer-coverage
  - agent-harness
  - legibility
related_components:
  - bin/lib/views.mjs
  - bin/lib/rank.mjs
  - bin/test-inflight.mjs
---

# A view no check renders has an unenforced contract

## Context

`bin/inflight.mjs` splits cleanly: query libraries return data, `bin/lib/views.mjs` turns data into
strings, and the front door owns the process boundary. That split is good architecture and it created
a blind spot, because **every check drove the query layer and none rendered the view.**

## What happened

Three defects shipped in one command's output, and all three were invisible to a suite that was
otherwise dense with checks:

- The command's own help text states the front door's interface rule - *every level prints the next
  level's command* - and its rows did not. A reader who wanted a note had to already know the
  command that shows one, and retype the path.
- A **scoped** group with no rows printed nothing whatsoever, which is indistinguishable from a
  section that was dropped. In a tool organised entirely against confusing "nothing found" with
  "did not look", the view was committing that confusion.
- The exclusion counts are whole-corpus figures, identical scoped or not, and sat unqualified among
  scope-limited lines - so they read as describing the scope.

None is a bug in the query. All three are the contract the view is supposed to keep, and nothing
asserted any of it.

## The second, sharper case: coverage that appeared to exist

Adding rendered checks is not sufficient on its own. A control named
`rank-never-claims-a-branch-or-pull-request-fixes-a-note` - protecting the single most consequential
claim the command makes, that carriage is not ownership - serialised the **row objects** and asserted
`row.relation === 'carries'`. Its mutant flipped that field and the control went red, so it looked
healthy.

But the user-facing sentence is **hard-coded in the renderer and never reads `row.relation`.** A
change to the view rendering *"fixed by this pull request"* would have left the control green, and
its mutant was dying on a data field that no output consumes. The check was one layer below the
behaviour in its own name.

That was found by an adversarial reviewer, not by the suite - and it is the same failure as the three
above, wearing the appearance of coverage rather than the absence of it.

## Root cause

A layered design makes each layer easy to test in isolation and makes it easy to believe the layer
you tested is the one that matters. The layer a **user** sees is the view; a check that stops at the
data is asserting on an intermediate representation, and the mapping from that representation to the
words on screen is exactly the untested part.

## Resolution

- Checks that render the view and assert on its **text**: the next-step command beside each row, the
  sentence an empty scope prints, the qualification on a whole-corpus count.
- The ownership control re-pointed at `formatRank`'s output, with its **mutant moved to the
  renderer** - so the mutation restores the defect a reader would actually see, and the assertion
  that dies is the one the check is named for.

## Prevention

- **When a check protects a claim, mutate the line that makes the claim to a user.** If the mutant
  patches a different layer from the one the check's name describes, the check is not protecting
  that behaviour.
- **A mutant that dies is not automatically a control that works.** Ask *which assertion* observed
  it. A mutant dying by crashing, or by an assertion on a field nothing renders, proves the mutation
  reached the process - not that the behaviour is defended. This repository has now found two
  vacuous controls this way, both in mutants that were passing.
- **Legibility defects are contract defects.** A missing next-step command, a silent empty section
  and an unqualified count are not cosmetic in a tool whose product *is* its output.
- **When a new command joins a front door, ask what renders it** before asking what tests it. If the
  answer is "nothing", the view's contract is unenforced no matter how good the query's coverage is.

## Related

- [`../workflow-issues/a-harness-that-cannot-tell-never-ran-from-ran-and-agreed-2026-09-02.md`](../workflow-issues/a-harness-that-cannot-tell-never-ran-from-ran-and-agreed-2026-09-02.md)
  is the adjacent failure: a mutant scored as caught when it never executed. Here the mutant ran and
  was caught by the wrong assertion.
- [`../best-practices/a-source-that-can-disagree-with-itself-needs-a-reader-that-says-so.md`](../best-practices/a-source-that-can-disagree-with-itself-needs-a-reader-that-says-so.md)
  covers the defects in the query layer of the same command, found in the same rounds.
