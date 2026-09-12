---
title: A truncated search answer is a wrong answer wearing a complete one's clothes
date: 2026-09-12
category: workflow-issues
module: tooling
problem_type: process_failure
component: development_workflow
severity: high
root_cause: process_violation
resolution_type: process_fix
applies_when:
  - About to pipe a corpus query through `head`, `tail` or any line cap
  - Reporting "no prior art" from a tool that searches every ref
  - Answering an owner's question about whether an idea has been recorded before
  - Summarising a long tool output for someone who will not run it themselves
  - Reading a tool's output in a session where context is tight
symptoms:
  - The right command was run, its answer was correct, and the conclusion drawn from it was wrong
  - A peer session finds the missed document in one pass, with the same command
  - "Nothing in plans or solutions" reported from output that stopped inside the first result group
  - The missed document is the owner's own, and they recognise it immediately
  - No error, no warning, no non-zero exit anywhere in the sequence
tags:
  - prior-art
  - negative-claims
  - false-negative
  - search-method
  - output-truncation
  - agent-harness
---

# A truncated search answer is a wrong answer wearing a complete one's clothes

## What happened

Asked whether a cross-language tracing framework had any prior art here, a session ran the check
`AGENTS.md` prescribes - `node bin/inflight.mjs prior-art` with the term and its common
abbreviations, across every ref - piped it through `head` with a line cap, and reported to the owner
that there was nothing in plans or solutions on the subject.

There was a note. It is the owner's own idea, parked for after v6, carrying the architectural
argument for why this codebase is an unusually good fit and the recorded gap it is blocked on. It
sorts below where the output was cut. A peer session found it in a single pass with the same
command, and corrected the answer.

It is also on no baseline ref, so no working-tree read reaches it either. Retrieve it with:

```bash
node bin/inflight.mjs docs show docs/inflight/parked-opentelemetry-across-the-clients.md
```
<!-- file-refs: N/A - branch-only, which is part of why a narrower read misses it; the command
     above is how it is retrieved, and it resolves on no ref this branch can see. -->

**The tool was not wrong about anything.** It searched the whole corpus, grouped the hits, flagged
which were off the baseline, and printed the note's path. The caller threw part of that away and
reported the remainder as the answer.

## Why the cut landed where it did

Three properties of this pairing turn a rough edge into a reliable wrong answer, and none of them
is bad luck.

**The preamble eats the budget before any result.** `bin/lib/prior-art.mjs` opens with a banner
naming the pattern, the baseline and the size of the corpus searched, then warns about a stale
fetch, a main-checkout read or a HEAD behind the baseline. Those lines exist to stop a different
false negative - they are the sibling write-up's fix, working - and they are free only if you read
past them. Under a line cap they are spent budget: the cap pays for the disclaimers and then runs
out before the evidence.

**A cap does not sample the answer; it prefers whatever sorts first.** Results are grouped by area -
plans, solutions, in-flight notes, feature records, everything else under `docs/`, then commits,
open PRs, merged PRs, issues - and ordered within each group. Cutting at a line count takes a
prefix, not a sample. It reliably reports the early groups in full and the later ones not at all,
and within a group it reliably keeps the paths that sort early. The missed note sorts after several
siblings in its own group; nothing about its content put it there.

**The temptation to cap is strongest exactly where the answer is most likely to be below the cut.**
Output gets long because the corpus has something to say about the term. A term with genuinely no
prior art prints a short page of "nothing, across every ref" lines that nobody wants to truncate. So
the reflex fires precisely when it is most expensive, and the run that most deserved a full read is
the run that got cut. Researching *this* write-up reproduced it: `prior-art` on truncation itself
returned a first group long enough that a generous cap would still have stopped inside it.

## The same signature again, in the same week

Worth naming as a pattern rather than retelling, because the visible symptom is identical - a
confident negative answer from a correct-looking check:

- **A feature record reported as non-existent**, from a working-tree grep of `docs/features/`. The
  directory is on the baseline, so the grep found it, read it, and returned nothing - a
  working-tree read answering a cross-branch question. `node bin/inflight.mjs docs` shows that a
  substantial part of that area exists only off the baseline; the command's own summary line says
  so, in the output that was not read.
- **`docs header` refused `docs/features/` paths as outside its areas** at the moment a session
  needed one. That was an honest boundary, not a truncation - `DOC_AREAS` in `bin/lib/repo.mjs`
  did not list it, and the command said so. astubbs/parallel-consumer#510 closes it by making
  feature records a fourth corpus area, so the refusal is now a wrong answer only in the archive.

The first is the branch-only failure the sibling document owns. The second is a tool that declined
to answer, which is the benign case: a refusal cannot be mistaken for a result.

## This is not the branch-only failure, and it is not the log one

The signature is identical to documents already on the record, and a reader who knows any of them
will assume it covers this. It does not. They differ in *which* link in the chain broke:

| | What broke | Who can see it |
|---|---|---|
| [`prior-art-lives-on-branches-2026-09-01.md`](prior-art-lives-on-branches-2026-09-01.md) | The **corpus** was wrong - a working-tree read answering a cross-ref question | A tool can, and `bin/inflight.mjs prior-art` now does |
| [`gh-run-view-log-truncation.md`](gh-run-view-log-truncation.md) | The **tool** truncated its own output, silently | A completeness check on the output can, once you know to run one |
| This one | The **caller** truncated a complete answer and reported the remainder | Nothing |

The distinction that matters: in the branch-only case the answer never reached the terminal, so a
better tool fixed it. Here the answer reached the terminal in full and was discarded between the
tool and the reader. There is no artefact of the discard - no warning, no marker, no non-zero exit,
nothing in the session's own record to notice later. The closest relative in mechanism is
[`a-title-grep-is-not-a-search-2026-08-31.md`](a-title-grep-is-not-a-search-2026-08-31.md), where the
query was too narrow; the difference is that a narrow query is at least visible in the command you
ran, and a `| head` reads as tidiness.

## Why no gate catches it

**No check can see what a caller piped a command's output into.** The gate would have to observe a
shell pipeline, decide that the left-hand side was a corpus query, and judge whether the cut
mattered. `bin/check-file-refs.sh` can tell you a cited path is gone; nothing can tell you a path
you never cited was printed and skipped.

The failure also produces a confident wrong answer rather than an error, which removes the second
line of defence: a broken pipeline gets noticed, a truncated one gets believed. This document is
therefore **guidance, not a rule with an enforcer**, and says so rather than implying a check exists
- the same honesty
[`a-check-that-reports-success-without-having-run.md`](a-check-that-reports-success-without-having-run.md)
asks for about checks whose silence is ambiguous.

## What to do instead

1. **Do not cap a corpus query's output.** If it is long enough that you want to cap it, that length
   is the finding: the corpus has something to say about your term.
2. **Narrow the question, not the answer.** Give the tool a tighter term and let it print everything
   it has for that term. A wide question answered in a prefix is strictly worse than a narrow
   question answered in full, because only the second one tells you what it excluded.
3. **If you must filter, filter on a term and say what you filtered on.** `| grep -i <term>` is
   defensible where `| head` is not - it is reproducible, it is inspectable, and quoting it in your
   answer lets the reader see the shape of what was left out.
4. **Read the section summaries and the counts the tool prints.** Every group in `prior-art` states
   what it searched, and `node bin/inflight.mjs docs` states how much of each area lives off the
   baseline. Those lines are how you know whether what you are looking at is all of it.
5. **Report "nothing found" alongside what was searched** - which the sibling write-up already
   argues for the branch half, and which applies unchanged here. The denominator is the part of a
   negative result that carries information.

## Related

- [`prior-art-lives-on-branches-2026-09-01.md`](prior-art-lives-on-branches-2026-09-01.md) - the
  other half of the same wrong answer: the corpus searched was narrower than the question asked.
  Read both; neither covers the other.
- [`gh-run-view-log-truncation.md`](gh-run-view-log-truncation.md) - truncation by the tool rather
  than by the caller, with the diagnoses it cost.
- [`a-title-grep-is-not-a-search-2026-08-31.md`](a-title-grep-is-not-a-search-2026-08-31.md) - the
  query-shape version: an empty result from a search that could not have said yes.
- [`negative-results-need-an-instrument-that-could-have-said-yes.md`](negative-results-need-an-instrument-that-could-have-said-yes.md) -
  the general principle under all of these.
- [`docs/inflight-tool.md`](../../inflight-tool.md) - worked examples for `bin/inflight.mjs`, and why
  each working-tree answer is wrong.
