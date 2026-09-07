---
title: "A source that can disagree with itself needs a reader that says so: the default is an answer, and it is wrong exactly where the question was asked"
date: 2026-09-07
category: best-practices
module: tooling
problem_type: best_practice
component: development_workflow
severity: high
applies_when:
  - "Writing a tool that reads one logical item which exists in several versions - a document across git refs, a config across environments, a record across replicas"
  - "Choosing a tie-breaker, a preference order, or a 'canonical' copy when two sources disagree"
  - "Parsing a document written for people - a register, a checklist, a design doc - with a regex rather than a grammar"
  - "A report says 'nothing found' or 'everything agrees' and the reader cannot tell that from 'the parse reached nothing'"
  - "Reviewing a fix in a version-selection or document-parsing path where a defect of the same class has already been fixed once"
symptoms:
  - "An item vanishes from a listing entirely because one version of it was read and the others were not"
  - "A delta prescribes a disposition (deferred, closed, stale) that no ref actually carries, naming no ref"
  - "Prose that discusses an entry is parsed as an entry; a cross-reference becomes a phantom row"
  - "A count of recognised entries is reported with no denominator, and reads as a complete reading of the document"
  - "Zero recognised entries renders identically to a document everything agrees with"
root_cause: logic_error
resolution_type: code_fix
tags:
  - grep-is-not-a-parser
  - version-selection
  - found-nothing-vs-could-not-look
  - false-negative
  - inflight-registers
  - agent-harness
related_components:
  - bin/lib/rank.mjs
  - bin/lib/notes.mjs
  - bin/lib/views.mjs
---

# A source that can disagree with itself needs a reader that says so

## Context

`bin/inflight.mjs rank` reads two sources, and **neither is a database**.

The first is the git corpus: one note path exists as many versions across hundreds of refs, and two
branches can disagree about whether the work it describes is open. The second is
`docs/inflight/process-candidate-ranking.md`, a register **written for people** - list items,
continuation lines, and paragraphs of prose that discuss the very entries the list contains.

The command's entire purpose is to surface where those sources disagree. It got both of them wrong,
repeatedly, and the failures share one shape.

## What happened

**Version selection produced three defects of one class, each found *after* the previous was fixed
and reviewed.**

1. **First-sorted-live-ref.** A note open on the branch that owns a bug and closed on a branch that
   fixed something adjacent was read from whichever ref sorted first, and dropped from the backlog.
2. **Baseline preference.** The baseline's version was then preferred *unconditionally*, so a note
   the baseline calls deferred while a live branch carries it open vanished from its group **and**
   the delta told the register it was deferred - on the baseline's word, naming no ref anywhere.
3. **Placeholder beats impact.** With "still open beats the baseline" in place, a baseline copy
   carrying *no impact tag* still tied with a live copy carrying one, and the baseline tie-breaker
   took the placeholder. Both are open work, so both passed the new rule. The row landed in the
   catch-all group rather than its impact bucket, and the delta - which accepts only the impact
   scale - reported the register's entry as stale while a live ref carried it as ranked work.

The third was found by a cross-model review after the first two had been fixed, reviewed, and
written up. On this repository it was live on a large fraction of the corpus: tagging a note on the
branch that works it is the ordinary flow, not an exotic case.

**Parsing the human document was wrong in both directions at once, and both were live.**

- A whole-document scan for `astubbs#<n>` turned continuation lines, cross-references and the
  register's own *"What is NOT on this list"* paragraph into ranked entries - so **every** number the
  delta reported as resolving to nothing was a false positive, and a citation of a document outside
  the notes directory became a phantom entry rendering as an instruction to delete a live
  cross-reference.
- In the other direction, the sentence *"fixing astubbs#177 does not close it"* marked that note as
  ranked, suppressing it from the half of the report that lists unranked work.

And the reporting hid its own coverage twice. A count of recognised entries shipped with no
denominator, so *"11 entries recognised"* read as a complete reading of a register whose ready-picks
half this parse deliberately does not reach. On the other half, notes were counted as *"NOT named by
the register"* three lines below a sentence saying the unread items were outside the delta entirely.

## Root cause

Both are the same mistake at different layers: **a reader given an ambiguous source resolved the
ambiguity silently and reported the result as fact.**

A preference rule looks like a sensible default and quietly outranks the evidence. A regex looks like
a parser and quietly matches the vocabulary of prose *about* a thing as readily as the thing itself.
In both cases the failure is invisible in the output, and in both cases it fires hardest on exactly
the input the tool exists to handle - the versions that disagree, the register that is doing its job.

## Resolution

Two halves, and both are needed. Neither is sufficient alone.

**1. Anchor to whatever actually decides, not to what is convenient to read.**

- For versions: a still-open version beats any preference, an impact-bearing version beats a
  placeholder, and the preference only breaks ties inside that pool.
- For documents: an entry is a **list item including its continuation lines** - the document's own
  structure - never a line, and never the whole file. A line-scoped parse split every entry this
  register writes across two lines; a file-scoped one swallowed the prose between them.

**2. Report what could not be resolved, rather than resolving it.**

- Every row names the refs whose version disagrees with the one it was placed by - and names a
  **live** carrier of that disagreement wherever one exists, because an archival ref sends a reader
  to check out a tag when a branch is carrying the same state.
- Every count of recognised entries ships with its denominator and a sentence saying what the
  unrecognised items look like. Zero recognised entries says *the parse did not reach this register*,
  which is a different answer from *the register agrees*.
- Any claim the parse cannot support at partial coverage is labelled as the upper bound it is.

## Prevention

- **Before writing a tie-breaker, ask what it will answer when the sources disagree** - because that
  is the only case anyone is running the tool for. If the answer is "whichever one the rule happens
  to name", the rule is a coin flip wearing the authority of a decision.
- **Three fixes of the same class after review is not converging evidence of correctness.** It is
  evidence the case is hard, and it is where the next reviewer should push hardest. Say so in the
  pull request rather than counting review rounds as corroboration.
- **A found-nothing and a could-not-look must not render the same.** Every enumeration says what it
  excluded and what it failed to read, and the exit code agrees with the prose - a run that could not
  list a ref must not return the status that means "ran successfully".

## Related

- [`a-guard-that-greps-java-must-read-what-javac-decided.md`](a-guard-that-greps-java-must-read-what-javac-decided.md)
  owns the case where a **parser is available** and the guard should use it. This one is its
  complement: a markdown register written for people has no compiler to defer to, so the structure
  the document itself uses is all there is to anchor to.
- [`../workflow-issues/a-title-grep-is-not-a-search-2026-08-31.md`](../workflow-issues/a-title-grep-is-not-a-search-2026-08-31.md)
  is the same false negative one layer out - searching *for* a document rather than parsing one.
- [`silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md`](silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md)
  is the measurement-side sibling of the coverage rule above.
