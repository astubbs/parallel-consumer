# Concepts

Shared domain vocabulary for this project — entities, named processes, and status concepts with
project-specific meaning. Seeded with core domain vocabulary, then accretes as ce-compound and
ce-compound-refresh process learnings; direct edits are fine. Glossary only, not a spec or catch-all.

Grown one learning at a time, so its coverage is uneven by design: the concurrency and
transactional-commit area, test-reliability vocabulary, and the issue-reference conventions are
described here. Other areas of the project are not yet.

## Relationships

The **produce lock** and the **commit lock** are the two sides of one readers-writer lock, and that
pairing is the load-bearing structure of the transactional commit path: many workers may hold the
produce side at once, but the controller's commit side excludes them all. Everything else in the
commit cluster — **dirty**, **eager processing during commit**, **commit lock timeout** — is a rule
about when one side may be taken or must wait.

## Parallel consumption

**Control loop**
The project's single controller thread: it drains completions, decides when to commit, and hands work
out to the worker pool. It is the only thread that commits, which is why commit decisions are
described from its point of view.

**Broker poller**
The thread that fetches records from the broker and keeps the consumer's group membership alive,
separately from the control loop. Keeping it distinct from the control loop matters: a stalled
controller and a stalled poller are different failures with different symptoms.

**Shard**
The unit of ordering. Records are distributed to shards by key or by partition depending on the
configured ordering, and a shard processes its records in order while different shards run in
parallel. This is how the project gets concurrency without adding partitions.

**In-flight work**
Records handed to the worker pool and not yet resolved as succeeded or failed. Distinct from records
merely fetched: in-flight work is what a commit must wait for, and what a shutdown must drain.

**Run state**
The broker poller's lifecycle stage, and the single authority on where an instance sits between
running and closed. Components that need to know read a signal derived from it rather than keeping
their own copy, so there is no second record that can disagree with this one.

**Draining**
The stage in which no new records are fetched but the instance keeps polling the broker while
existing in-flight work finishes. The distinction from closing is load-bearing rather than
cosmetic: a draining instance must remain a live, polling group member, because polling is what
services commit responses and what keeps the group from treating the member as failed. Only closing
may stop the poll loop.

**Drain zombie**
An instance that stopped polling while draining. It holds its partition assignment without consuming
from it and cannot answer a rebalance, so the whole group stalls until the coordinator evicts it.
The symptom presents as consumption pausing after a rebalance, which is why it is easily mistaken
for a different defect with the same surface.

**Revoke-time commit**
The attempt to commit offsets from inside the partition-revoked callback, before the partitions are
handed to their new owner. Nothing depends on it succeeding: offsets it does not commit are simply
redelivered to the new owner, which the at-least-once contract already allows.

That makes it *safely* discretionary, which matters because the callback runs on the broker poller
inside the poll call and blocks the whole group's rebalance while it runs - so waiting there on
anything the controller might hold risks deadlocking the pair, and declining costs only bounded
reprocessing. Treating it as discretionary is nonetheless a change still in flight rather than
current behaviour: as shipped, the revoke-time commit waits.

## Transactional commit

**Produce lock**
The shared side of the transactional lock, taken by a worker for as long as it may still produce
records into the current transaction. Many workers hold it concurrently. Whether it is taken before
the user's function runs or only when the result is produced depends on eager processing during
commit.

It is released only once the completed work has been handed back to the controller — releasing it
earlier would let a commit collect offsets that do not yet account for work already produced, which is
the exactly-once invariant this lock exists to protect.

**Commit lock**
The exclusive side of the transactional lock, taken by the control loop before it collects offsets, so
that no further records can enter the transaction while the offsets for it are being gathered. Because
it is exclusive, acquiring it means waiting for every held produce lock to be released. Not to be
confused with the lock guarding commit *execution* on the rebalance path - see the Flagged
ambiguities.

Acquisition is bounded: if the wait exceeds the configured limit, the attempt fails rather than
blocking forever, and the failure is fatal to the instance. That deliberate bound is what turns "a
user function is taking too long to allow a commit" into a visible error instead of a hang.

**Eager processing during commit**
Whether workers may begin processing new records while a commit is in progress. When disallowed, a
worker takes the produce lock before running the user's function, so a slow function blocks commits;
when allowed, the lock is taken only at produce time, and processing overlaps the commit. The choice
trades commit latency against throughput.

**Dirty**
A partition has at least one successfully completed record whose offset has not yet been committed.
Only a *success* makes a partition dirty — a failure does not — and the controller attempts a commit
only when something is dirty.

The asymmetry is load-bearing: a partition whose records are all failing is never dirty, so no commit
is attempted for it, and anything waiting on a commit-time behaviour will wait indefinitely.

## Test reliability

**Load-tightness flake**
A test that fails because a deadline or assertion margin is too tight to survive a contended machine,
rather than because the code under test is wrong. Named as a family here because its members look
identical to real product stalls and must be told apart before either is "fixed".

**Unforceable trigger**
A test that awaits a consequence whose precondition it cannot guarantee will occur. Distinct from a
load-tightness flake: the margin is not too small, the awaited event may simply never happen in some
interleavings, so no timeout is long enough to make the test reliable.

**Ambient probe**
The always-on recorder attached to broker integration tests, which annotates a failure with
consumer-group progress evidence so the contention-versus-product-bug question is answered before
manual diagnosis starts. Its verdict is only informative when its detectors could have fired for the
test in question — a short, low-volume test cannot trip them, and a clean reading there means nothing.

## Issue references

**Fork mirror**
An issue in this repository that stands in for an upstream issue, carrying a summary and a link back
to the original. Every upstream issue has one, so a reader of this repository always has a number
they can act on locally. A mirror is one person's reading of the upstream issue rather than the issue
itself, so its summary is checked against the original before being relied on.

**Qualified reference**
An issue reference that names the repository it belongs to rather than standing as a bare number.
Required below the point where this fork's numbering overlaps the upstream project's, because in that
range a bare number exists in both repositories meaning different things — so it resolves to
something plausible and wrong rather than failing visibly. The repository is named by owner, not by
role: "upstream" describes a relationship and is not stable, since this fork is itself upstream to
anyone who forks it.

**Verbatim quoted artifact**
Text reproduced in documentation because it is exactly what a program emitted or consumes — log
output, an error message, a stack trace, a tool transcript, a fixture a test compares against. Its
value is that it is byte-exact, so editing one does not improve it, it forges it. Distinct from
ordinary prose that merely mentions the same subject, and the distinction only matters when something
rewrites text automatically.

Such an artifact survives a mechanical sweep only inside an inline code span, which the reference
convention's checker ignores in any file. A fenced block confers no protection, because the check
reads one line at a time and cannot see a fence opened above it.

**Mechanical sweep**
A scripted pass that rewrites every match of a pattern across the repository. Preferred over hand
editing at scale, and the standing risk is that it treats every match as the same kind of text: the
recurring failure is not a wrong substitution but a correct substitution applied to something that
was never a reference. A sweep's own count of substitutions is not evidence any of them were right.

## Flagged ambiguities

- **"Stall", "load-tightness flake" and "unforceable trigger" had been used loosely for the same red
  test.** They are distinct: a stall is the product failing to make progress, a load-tightness flake is
  a real deadline missed under contention, and an unforceable trigger is an awaited event that never
  occurred. All three present as the same expired await, and the whole diagnostic difficulty of this
  area is telling them apart.
- **References had been qualified by role ("upstream") as well as by owner.** The owner form is the
  agreed one; the role form is retired, because "upstream" names a relationship rather than a
  repository and does not survive this fork being forked in turn.
- **"Commit lock" names two different locks, and a timeout report is ambiguous without saying
  which.** One is the exclusive side of the transactional produce/commit pair, whose acquisition is
  bounded and fatal on expiry. The other guards commit *execution* so that the revoke path can
  decline it rather than block. They are contended by different threads for different reasons, so a
  failure in one says nothing about the other. The collision is not hypothetical: both names live in
  the same class, a few hundred lines apart. Say which one is meant.
