---
title: "A repair to a review finding is unreviewed code - three rounds to fix one sentence, and the enumeration that could not be fixed"
date: 2026-08-18
category: workflow-issues
module: parallel-consumer-core
problem_type: workflow_issue
component: code_review
severity: medium
root_cause: process_gap
resolution_type: workflow_improvement
applies_when:
  - "Fixing a finding raised by a code review, human or automated"
  - "A claim in a comment or document enumerates the callers, senders or subclasses of something"
  - "A second review round finds the previous round's fix was itself wrong"
  - "Deciding whether to correct a list or replace it with the rule that generates it"
symptoms:
  - "The same paragraph is corrected in consecutive review rounds and is still wrong"
  - "A repair makes a vague claim more precise, and the precision is what is false"
  - "A fix ships with less scrutiny than the code it fixes, because the reviewer 'already checked that area'"
  - "Each correction adds a missing item to a list rather than asking why the list keeps being incomplete"
tags:
  - code-review
  - review-findings
  - enumeration
  - invariants
  - documentation-accuracy
  - process
related_components:
  - development_workflow
---

# A repair to a review finding is unreviewed code

Reviews of astubbs#296 found a false claim in a code comment and in a solutions write-up. The claim
was repaired three times. **The first two repairs were themselves wrong, and each took another full
review round to catch.** After the original defect, every subsequent defect in that paragraph was
introduced by a fix, not by the author's first draft.

The mechanism the paragraph is about - one interrupt bit carrying four meanings, and why its senders
are not an enumerable set - is owned by
[`waking-a-thread-by-interrupting-it-2026-08-17.md`](waking-a-thread-by-interrupting-it-2026-08-17.md). **That document owns the concurrency defect; this one owns the repair
sequence** - the three superseded wordings, which round produced each, and what the sequence says
about how fixes to review findings get scrutinised. Read that one for why the set is open; nothing
about the interrupt design is restated here.

## The three rounds, verbatim

**The original claim**, in a code comment and in the document:

> any worker finishing work re-arms the flag through `notifySomethingToDo` in that gap

Review found it false. Worker threads do not re-arm the flag: `addToMailbox` only enqueues
(`workMailBox.add(ControllerEventMessage.of(wc))` in
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java`),
and nothing on the completion path interrupts.

**Repair 1** replaced the wrong sender with a list of the right ones:

> The re-armers are the rebalance listener in `onPartitionsAssigned` and two public API methods,
> `requestCommitAsap` and `resumeIfPaused`, called from whatever thread the embedding application
> uses.

Next review round: `transitionToDraining` - reached by `close(DRAIN)` - was missing.

**Repair 2** added the missing route and conceded the shape of the problem without acting on it:

> The re-armers are the rebalance listener in `onPartitionsAssigned`, `transitionToDraining` by way
> of a `close(DRAIN)` arriving after the self-close, and two public API methods, `requestCommitAsap`
> and `resumeIfPaused` - all called from whatever thread the embedding application uses.
> `notifySomethingToDo` is itself `public`, so the list is open-ended by API rather than closed by
> this enumeration.

Next review round found two things. Plain `close()` was still missing - `public void close()` calls
`closeDontDrainFirst()`, which reaches `case DONT_DRAIN ->` and `transitionToClosing`. That is the
**likeliest** route of the lot: the class is `implements ParallelConsumer<K, V>,
ConsumerRebalanceListener, Closeable`, so try-with-resources, a container teardown and a JVM shutdown
hook all call the no-arg `close()`, never `close(DRAIN)`.

**Repair 3** stopped enumerating and stated the rule: anything that reaches `public void
notifySomethingToDo()` re-arms the flag; every state transition and both forms of `close()` do; the
method is public, so the senders are not an enumerable set.

## Why the repairs were the weak point

**A fix for a review finding arrives with borrowed confidence.** The reviewer found the bug, so the
repair feels validated by association - it is the answer to a question someone else already
verified. It is nothing of the sort: it is fresh, unreviewed prose or code, written under time
pressure, by the one person just shown to be wrong about this exact area. The original claim had a
draft, a self-check and a review; repairs 1 and 2 had none of the three. Reviewers compound it from
the other side - having already read that region closely, the next pass skims the delta.

**An enumeration is the failure-prone form.** Three people, each having just read the class, each
produced an incomplete list of the same set. The lists were not careless; the misses were a
`ConsumerRebalanceListener` callback, a drain-mode branch and the plainest `close()` in the API - all
one grep away, and all missed anyway. **When a list has been wrong twice, the fix is not a more
careful list.** Wrong three times is evidence about the *shape*, not the effort: state the invariant
that generates the list, and let the reader derive the members. Repair 2 is the instructive one - it
noticed the set was open-ended by API and still shipped the enumeration, adding the caveat next to
the list instead of replacing it.

**The precision trap: making a vague claim more definite can make it false, and it reads as an
improvement.** Repair 2 asserted the re-armers were "all called from whatever thread the embedding
application uses". `onPartitionsAssigned` is not - it is the `ConsumerRebalanceListener` callback
(`usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsAssigned(partitions))`) and runs on the
broker poll thread. The pre-repair wording said nothing about threads and was merely vague; the
repair made it specific and wrong. Reviewers reward added specificity, so this class of regression
passes review more easily than the vagueness it replaced.

## What to do

- **Treat a repair to a review finding as new code.** Same scrutiny, same verification, same
  red-proof. Do not let "the reviewer already looked here" stand in for having checked.
- **Re-derive the claim from source; do not patch the previous wording.** Repairs 1 and 2 both edited
  the sentence in place, inheriting its structure - a list - which is what kept being wrong. Repair 3
  went back to `notifySomethingToDo` and asked what reaches it.
- **On the second wrong list, replace the list with its invariant.** If you cannot state the
  invariant, say the set is open and name why (here: the method is `public`); an honest "not
  enumerable" beats an enumeration that will be silently invalidated by the next caller.
- **Verify a repair's *added* precision hardest.** The claim the repair makes that the original did
  not make is the one nobody has checked - here, which thread each caller runs on.
- Say in the PR which round each fix belongs to. Two consecutive rounds finding a defect in the
  previous round's fix is the signal that the *form* is wrong, and it is only visible if the rounds
  are distinguishable afterwards.
