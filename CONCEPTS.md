# Concepts

Shared domain vocabulary for this project — entities, named processes, and status concepts with
project-specific meaning. Seeded with core domain vocabulary, then accretes as ce-compound and
ce-compound-refresh process learnings; direct edits are fine. Glossary only, not a spec or catch-all.

Seeded from the transactional-commit learning of 2026-08-07, so it covers the concurrency and
transactional-commit area. Other areas of the project are not yet described here.

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

## Health and run state

**Run state**
Where a subsystem sits in its lifecycle: `UNUSED`, `RUNNING`, `PAUSED`, `DRAINING`, `CLOSING`, `CLOSED`.
The **control loop** and the **broker poller** each have their own, and they legitimately diverge — pausing
moves only the controller, and before the first poll the controller is still `UNUSED` while the poller
already reads `RUNNING`. Each run state also has a fixed integer published through the `pc.status` and
`pc.poller.status` gauges, so the numbers are a contract and are deliberately not the enum's ordinal.

**Health verdict**
The single boolean a container platform acts on: the control loop is not shutting down *and* no failure
cause has been recorded. Liveness-scoped — it answers "does this instance need restarting", never "is
this instance consuming". A **stall** leaves the run state at `RUNNING`, so the verdict reads healthy
throughout one; that is the deliberate limit of the concept, not a defect in it. Progress is a metrics
question, not a health-verdict one.

**Failure cause**
The exception recorded when an instance died, and the only thing that distinguishes a crash from a clean
shutdown — both leave the control loop `CLOSED`. It is unattributed: there is one cause, sourced from the
control loop, not one per subsystem.

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
