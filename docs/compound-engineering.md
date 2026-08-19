# Compound engineering - how a failure becomes a mechanism

**Owns the development model**: why this fork can move at the volume it does without the output
being slop, and what has to be true of a piece of work before it counts as finished. It is not a
list of investigation techniques - [`docs/investigating.md`](investigating.md) **owns those** (control
arms, stated predictions, negative controls on a guard) and wins where the two disagree. This
describes what happens *around* them: the loop that turns each failure into something that cannot
recur.

## The loop

> **A mistake is not finished when it is fixed. It is finished when it cannot happen again without
> something going red.**

Four steps, and the third is the one usually skipped:

1. **Fix the thing.**
2. **Write down what was actually established** - in `docs/solutions/` if it is settled, in
   `docs/inflight/` if it is open. Cite, never retell.
3. **Give it a mechanism.** If a rule was needed and a document would have carried it, ask whether a
   check, a gate or a hook can carry it instead -
   [`docs/agent-harness.md`](agent-harness.md) **owns that call**, and its central claim is that
   `.claude/hooks/` is runtime programming rather than tooling.
4. **Prove the mechanism can fail.** An untested guard is a guard-shaped comment.

The compounding is step 3 and 4 together: the mechanism outlives the memory of the incident, so the
next person pays nothing for a lesson they never had to learn.

## Worked chain

One session, one bug fix, and what fell out of it - each item caused by the failure of the one
before:

| What happened | What it became |
|---|---|
| astubbs#31 shipped the confluentinc#909 fix with a declared "no load-level evidence" gap | - |
| It merged ~10 min before a background agent closed that gap | `.claude/hooks/check-merge-outstanding-work.sh` - refuses a merge while this session has work in flight |
| Building the reproduction found a **third precondition** nobody had named | `docs/solutions/logic-errors/909-needs-a-saturated-pipeline-the-third-precondition-2026-08-19.md`, and the retirement of the note that had assumed bad luck |
| The new guard's own review found it bypassable, and its self-test suite unable to fail | negative controls for every arm; the suite now exits non-zero when a case fails |
| None of this was findable without knowing to grep | `inflight-class` on all 70 notes, grouped by consequence at session start |

The bug fix is the smallest artefact in that table.

## What "finished" means

Not "the tests pass". **"I can name how this would have gone red if it were wrong."** If there is no
answer, the work is unverified regardless of how green it looks - and green that asserts nothing is
the failure class this repo has hit most often (a mutation lane scoring zero mutants, a self-test
suite printing `FAIL` and exiting `0`, a regression test that passed whether the code was fixed or
broken).

## Three techniques not owned elsewhere

The investigation techniques live in [`docs/investigating.md`](investigating.md). These three are
about *how the work is produced* rather than how a cause is settled, so they live here:

- **Measure, do not sample.** A conclusion drawn from the handful of cases you happened to look at
  is an anecdote wearing a conclusion's clothes. Deciding whether note titles added anything over
  filenames: six files said "no", the full seventy said the opposite - the sample had landed on all
  four of the exceptions. If the corpus is countable, count it.
- **Verify a delegate's claim; never relay it.** Work handed to a subagent comes back as a report,
  and a report is a claim. Re-run the load-bearing assertion yourself. Reports here have carried a
  test count from a stale tree, and a "resolved" thread whose reply had silently failed to post.
- **Prove reachability, not just mechanism.** Reading which object holds which field tells you what
  a race *would* look like; it says nothing about whether two threads can ever be in those
  positions at once. Two confident, wrong root causes for confluentinc#909 were both produced this
  way. The check that settled it was four greps - name the threads, then look for what serialises
  them.

## Where this is enforced

Nothing here is self-enforcing, which is the point of writing it down:
[`docs/agent-harness.md`](agent-harness.md) is where a rule from this page goes to acquire teeth.
The honest state of that is on that page under *Known gaps* - some layers fire on their own, and
some are still only available.
