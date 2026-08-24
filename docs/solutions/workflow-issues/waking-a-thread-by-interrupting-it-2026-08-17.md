---
title: "One bit, four meanings - waking a thread by interrupting it, and the defensive clears that accumulate"
date: 2026-08-17
category: workflow-issues
module: parallel-consumer-core
problem_type: design_issue
component: concurrency
severity: medium
root_cause: design_limitation
resolution_type: documentation
status: "Not fixed. Recorded because the defect recurs on every new close path, and the only thing stopping it is a reader noticing."
applies_when:
  - Adding a state transition, wakeup, or close path to a thread that is signalled by interruption
  - Reviewing code that calls Thread.interrupt() on a long-lived worker
  - A close, commit, or lock acquisition throws InterruptedException for no apparent reason
  - Deciding whether a defensive Thread.interrupted() is a fix or a symptom
symptoms:
  - The same class clears the interrupt flag by hand in several unrelated places
  - A comment explains that the flag must be cleared "or the commit lock will throw"
  - Code logs a warning that a thread was interrupted and carries on, because it cannot tell why
  - A new wakeup near a shutdown path causes an unrelated operation to fail
tags:
  - concurrency
  - interrupt
  - signalling
  - shared-mutable-state
  - shutdown
  - design-smell
---

# One bit, four meanings

`AbstractParallelEoSStreamProcessor` wakes its control thread by **interrupting** it
(`notifySomethingToDo` -> `interruptControlThread` -> `Thread#interrupt`). That is a normal idiom for
unblocking a thread parked on a queue. The problem is that the same bit is also the shutdown signal,
and the JDK gives it a third meaning nobody chose.

Four messages, one bit:

| Meaning | Sent by |
|---|---|
| "you have mail, wake up" | `notifySomethingToDo`, from five call sites |
| "stop blocking on the mailbox" | the mailbox poll and the spin-avoidance sleep, which catch `InterruptedException` and treat it as *woke up* |
| "shut down" | `supervisorLoop`'s `catch (InterruptedException)` -> `doClose` |
| "your next commit-lock acquisition will throw" | nobody - a side effect of the other three |

**A receiver cannot tell which message it got.** They are the same bit.

## The symptom to recognise: defensive clears accumulate

The failure does not present as a bug in the signalling. It presents as unrelated operations
throwing, and it gets fixed locally each time - so the tell is a **population of hand-clears** rather
than any one of them:

```java
Thread.interrupted(); // clear interrupted flag as during close need to acquire commit locks and
                      // interrupted flag will cause it to throw another interrupted exception.
```

```java
if (Thread.interrupted()) { // clear interrupted flag
```

```java
log.warn("control thread interrupted - may lead to issues with transactional commit lock acquisition");
```

That third one is the most informative: it does not clear the flag, and it does not know whether the
interrupt meant *wake up* or *shut down*. **The code is admitting it cannot tell.** A signalling
channel every consumer must manually reset is not a channel - it is a shared mutable global with no
owner, and every clear is correct in isolation while none of them can be checked.

## How it recurred, which is the point

astubbs#296 added a path where a running instance whose worker pool had been destroyed closes itself:

```java
transitionToClosing();   // -> notifySomethingToDo -> interruptControlThread
```

That call runs **on the control thread**, one step before `controlLoop`'s switch reaches
`doClose`. So the thread interrupted itself and then ran the entire close sequence with the flag set -
exactly the condition the existing comment above warns about. Code review caught it; no test could,
because the unit tests drive the method directly without starting `supervisorLoop`, so
`blockableControlThread` is null and `interruptControlThread` no-ops. An assertion written for it
**passed under mutation** and had to be deleted as vacuous.

Nothing was violated. The author added a state transition - the most ordinary thing in the class - and
inherited a shutdown hazard from a wakeup.

## The clear cannot hold, which is the thesis rather than an exception to it

Review of astubbs#296 found the follow-on, and it is worth more than the original defect as evidence.

The first fix for the self-interrupt put `Thread.interrupted()` immediately after the transition -
at the point of **cause**. That left two log statements, the loop-end hooks and the state switch
between the clear and `doClose` - the hooks are a test seam and empty in production, so the real gap
was smaller than the list suggests - and another thread can re-arm the flag through
`notifySomethingToDo` in that gap. **Not a worker thread**, which is worth stating because it is the
first guess and it is wrong: `addToMailbox` only enqueues. Nearly everything else is a re-armer:
every state transition calls `notifySomethingToDo`, **both** forms of `close()` reach it through
`transitionToClosing` or `transitionToDraining`, the rebalance listener reaches it from the broker
poll thread, and the method is `public`, so an embedding application can call it directly.

**This section enumerated that set three times and was wrong three times** - first naming worker
threads, which never call it; then omitting `close(DRAIN)`; then omitting plain `close()`, the
likeliest route of all, since a shutdown hook calls `close()` and not `close(DRAIN)`. Each correction
was made carefully, by someone who had just read the code, and each was still incomplete.

That is worth more than the enumeration it replaces, because it is the thesis restated as a fact
about this document: **a channel with no owner has no enumerable set of senders.** Any list is a
snapshot that the next transition invalidates silently, exactly as the defensive clears are local
fixes that the next wakeup invalidates. State the rule instead - anything that reaches
`notifySomethingToDo` re-arms the flag - and do not maintain a list. The guard that would suppress those
calls, `awaitingInflightProcessingCompletionOnShutdown`, is not set until `innerDoClose`, later
still.

**Moving the clear to the point of need fixes most of it** - one statement before `doClose`, in the
state switch, covering every route into `CLOSING` rather than each site that might cause an interrupt.
That is what the pre-existing clears already do, and it takes the window from *arbitrary user code*
down to *one statement*.

**What it cannot do is close the window**, because clear-then-call is not atomic: a worker can still
interrupt between the two. That residue is irreducible while the channel is shared.

**It is not new and it is not worse.** Both pre-existing clears have the same shape - clear, then
call `doClose` - and clear-then-call is never atomic. The second of them is the path this codebase
already used for exactly this scenario. What the third clear adds is a slightly wider gap, not a new
hazard.

**The consequence is bounded and inside the delivery contract**, which is why it is recorded rather
than fixed: the flag surviving into `doClose` risks the final commit, which is caught
(`log.warn("failed to commit during close sequence", e)`), so those offsets are redelivered on the
next assignment. At-least-once already permits that.

**The point is what it says about the mechanism.** A defensive clear is a *local* fix to a *shared*
channel, so it can only ever hold for the instant between the clear and the next reader - and anyone
may re-arm it in that instant, legitimately, for an unrelated reason. That is why the count of clears
grows rather than the problem shrinking: each one is correct, each one is temporary, and none of them
can be made permanent while the channel carries more than one meaning. Four clears is not four
oversights; it is the same unfixable thing four times.

## What to do

**Now, when touching one of these paths:** if you call anything that reaches `notifySomethingToDo`
from the control thread itself, clear the flag afterwards, and say which of the four meanings you
intended. Grep the class for `Thread.interrupted()` before adding a fifth.

**Properly:** move *every* meaning into messages, including shutdown - not just the wakeup. Leaving
"shut down" on the interrupt is the same category error one step quieter: an interrupt has no payload,
no sender and no reason, so anything read from it is a guess. Under the rule *an interrupt carries no
meaning*, a woken thread learns nothing from the wakeup itself; it reads its mailbox. The interrupt
survives only as the mechanism for unparking a thread the mailbox cannot reach.

**Do not start this from scratch.** A 2022 micro-actor branch family already exists - manifest entry
`sweep-2023-actor-ipc`, editorially owned by `docs/refactoring.md`'s *Actor / IPC message bus*
section - and it went through a ranked ideation pass on 2026-08-17:
[`docs/inflight/core-actor-revival.md`](../../inflight/core-actor-revival.md). The framework proper is
537 lines in 4 files, coupled to PC by one 16-line marker interface.

Two of its six survivors bear directly on this write-up. **Survivor 6, the concurrency mass budget**
(an ArchUnit ratchet on primitive counts, conversions graded on locks removed), is the generalisation
of what is measured here - four clears for one bit is a mass reading, and the counting rule below is
the instrument. **Survivor 5, the skeleton-first strangler**, is where a payload-free nudge would
land as a per-seam swap rather than a rewrite.

The narrow contract debts this sits among are in
[`docs/inflight/core-control-thread-contract-debts.md`](../../inflight/core-control-thread-contract-debts.md).

## The transferable shape

**When one primitive carries several meanings, the cost is not confusion at the send site - it is that
every receiver must guess, and each guess is written as a local defensive fix.** Count the defensive
fixes. If the same clear, reset or re-check appears three or more times for one piece of state, the
state is overloaded, and the next feature will add a fourth. The clears are not the bug being fixed;
they are the bug being paid for, repeatedly.
