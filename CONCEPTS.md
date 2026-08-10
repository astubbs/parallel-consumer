# Concepts

Shared domain vocabulary for this project — entities, named processes, and status concepts with
project-specific meaning. Seeded with core domain vocabulary, then accretes as ce-compound and
ce-compound-refresh process learnings; direct edits are fine. Glossary only, not a spec or catch-all.

Seeded from the transactional-commit learning of 2026-08-07, so it covers the concurrency and
transactional-commit area, and since extended into the Kafka Streams dispatch area. Other areas of the
project are not yet described here.

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
it is exclusive, acquiring it means waiting for every held produce lock to be released.

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

**Frontier**
The highest offset up to which *everything* on a partition is contiguously complete — the boundary
between settled territory and territory with unfinished work in it. It advances only when the gap
behind it closes: with 10 and 12 still running, completed 11 and 13 do not move it. The frontier is
what the project commits as the consumer-group offset, which is why a crash-time commit can never
cover an in-flight record: loss is impossible by construction, not by care. In the code it is
`getOffsetHighestSequentialSucceeded() + 1`.

**Frontier semantics** (or **frontier plus holes**)
The commit design built on the frontier: commit the safe resume point, and encode the exceptions —
records completed *beyond* the frontier — into the commit's metadata field, so a restart resumes at
the frontier without losing the in-flight records and without repeating the completed ones. The same
shape as TCP's cumulative ACK plus SACK blocks. Contrast with a **high-water mark**, a single
per-partition number that assumes sequential completion and silently loses in-flight records when
completion is out of order — the defect class the Kafka Streams module's U9 exists to remove, and the
reason that fix is a deletion rather than a repair: no amount of locking lets one number express
"12 done, 10 and 11 still running".

## Kafka Streams dispatch

**Split poll wait**
The replacement for a Streams thread's single long poll: a short poll that collects whatever the broker
has already fetched, followed by a wait on a condition this project owns for the remainder of the
configured poll budget. Taken only while that thread has work outstanding; with nothing in flight the
thread takes the stock full-budget poll, because nothing could end the wait early and shortening the
poll would only delay broker records.

**Wake-on-work**
The policy the split poll wait exists to serve: end the wait the moment work becomes dispatchable,
rather than sitting out a poll budget while workers complete in the background. It is what removes the
throughput ceiling a poll interval imposes once something other than the consumer can make work
available.

The wait must release once per raised signal, not once per pass. Its natural predicate reads live
state - is an outcome waiting to be fed back - which cannot lose a signal, because the thread that
would clear that state is the one parked on it. But a topology can be paused, and a paused thread keeps
polling while skipping processing, so the state is never cleared and a predicate that only reads it
would return instantly forever, converting the wait into a busy-spin. Counting which raises a waiter has
already been released on keeps the no-lost-signal property and removes the spin. Abandoning the wait
outright, for shutdown or for the last dispatcher going away, is a separate signal that deliberately
does not count as work arriving.

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

## Flagged ambiguities

- **"Stall", "load-tightness flake" and "unforceable trigger" had been used loosely for the same red
  test.** They are distinct: a stall is the product failing to make progress, a load-tightness flake is
  a real deadline missed under contention, and an unforceable trigger is an awaited event that never
  occurred. All three present as the same expired await, and the whole diagnostic difficulty of this
  area is telling them apart.
