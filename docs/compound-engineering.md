# Compound engineering - how a failure becomes a mechanism

**Compound engineering is the practice of doing work in a way that makes the *next* piece of work
cheaper** - each fix leaves behind a check, a document or a hook, so the lesson is paid for once and
never re-learned. The term and the workflow come from Every's
[Compound Engineering](https://every.to/chain-of-thought/compound-engineering-how-every-codes-with-agents),
and the `ce-*` skills used throughout this repo are theirs:
[EveryInc/compound-engineering-plugin](https://github.com/EveryInc/compound-engineering-plugin).

This page owns what the practice means **here**: why the fork can move at this volume without the
output being slop, and what has to be true before work counts as finished.
[`docs/investigating.md`](investigating.md) **owns the investigation techniques** (control arms,
stated predictions, negative controls) and wins where the two disagree. This is what happens *around*
them.

## The loop

> **A mistake is not finished when it is fixed. It is finished when it cannot happen again without
> something going red.**

1. **Fix the thing.**
2. **Write down what was established** - `docs/solutions/` if settled, `docs/inflight/` if open.
   Cite, never retell.
3. **Give it a mechanism.** If a rule was needed and a document would have carried it, ask whether a
   check, gate or hook can carry it instead. [`docs/agent-harness.md`](agent-harness.md) **owns that
   call**; its central claim is that `.claude/hooks/` is runtime programming, not tooling.
4. **Prove the mechanism can fail.** An untested guard is a guard-shaped comment.

Steps 3 and 4 are the compounding: the mechanism outlives the memory of the incident, so the next
person pays nothing for a lesson they never had to learn. Step 3 is the one usually skipped.

## Worked chain

One session, one bug fix. Each row was caused by the failure of the one above:

| What happened | What it became |
|---|---|
| astubbs#31 shipped the confluentinc#909 fix with a declared "no load-level evidence" gap | - |
| It merged ~10 min before a background agent closed that gap | `.claude/hooks/check-merge-outstanding-work.sh` - refuses a merge while the session has work in flight |
| Building the reproduction found a **third precondition** nobody had named | a solutions write-up, and the retirement of the note that had assumed bad luck |
| The guard's own review found it bypassable, and its self-test unable to fail | negative controls for every arm; the suite now exits non-zero when a case fails |
| None of it was findable without knowing to grep | `inflight-type`/`impact`/`state` on every open note, grouped by consequence at session start, with a gate that rejects a tag the index could not place (astubbs#324) |
<!-- file-refs: N/A - check-merge-outstanding-work.sh lands with astubbs#324, the tooling half of the astubbs#322 split -->

The bug fix is the smallest artefact in that table.

## What "finished" means

Not "the tests pass" but **"I can name how this would have gone red if it were wrong."** No answer
means the work is unverified however green it looks. Green that asserts nothing is the failure class
this repo hits most: a mutation lane scoring zero mutants, a self-test printing `FAIL` and exiting
`0`, a regression test that passed whether the code was fixed or broken.

## Three rules that follow from it

- **Anything that hides items must say how many it hid.** A filtered view - closed notes dropped from
  an index, findings capped at ten, a lane skipping a package - reads downstream as *the complete
  set*, and the omission is invisible precisely because the filter worked. One line of count restores
  it. Same failure as a green check that asserted nothing: silence reading as a result.
- **Measure, do not sample.** If the corpus is countable, count it. Asked whether note titles added
  anything over filenames, six files said no and the full seventy said the opposite - the sample had
  landed on all four exceptions.
- **Verify a delegate's claim; never relay it.** A subagent's report is a claim. Re-run the
  load-bearing assertion. Reports here have carried a test count from a stale tree, and a "resolved"
  thread whose reply had silently failed to post.

## Where this is enforced

Nowhere, by itself - which is the point of writing it down.
[`docs/agent-harness.md`](agent-harness.md) is where a rule from this page goes to acquire teeth, and
its *Known gaps* section is the honest state of which layers fire on their own.
