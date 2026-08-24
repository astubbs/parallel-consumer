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

## Naming

**AK core** (never bare "core")
The Apache Kafka client itself - `KafkaConsumer`, `KafkaProducer` - as opposed to anything in this
project. Say **AK core**, because bare "core" collides with **`parallel-consumer-core`**, the module,
and the two turn up a sentence apart whenever the engine is being compared against the client it
wraps. "For Java, core is the native client" and "build the core/sleep demo" were written in the same
document meaning opposite things.
<p>
"AK" is already the repo's abbreviation for Apache Kafka. The comparison demo names its serial lane
`AK_CORE` for the same reason.

**Bundling** (never "batching", for the boundary)
How many records cross the language boundary in one hop. A transport concern, invisible in the
user's function signature: a client can bundle a hundred records per hop and still hand the user one
at a time. Borrowed from Apache Beam, which calls exactly this unit a bundle.
<p>
Say **bundling**, because "batching" already means something else here and the collision was
actively costing us: the two were being treated as one decision when they are independent axes with
different owners, different difficulty, and different answers for Parallel Consumer and for Kafka
Streams. `docs/language-bindings.md` maps the axes;
`docs/inflight/next-batching-modes-for-clients.md` owns both definitions.

**Batching** (the user's signature, never the wire)
How many records the user's function receives per call. Core's API is already batch-shaped - `poll`
hands the user a context of records - so a batch size of one is the degenerate case rather than a
separate API. Says nothing about how many hops those records took to arrive.

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

**Admission target**
The number of records the engine allows in flight at once — the control variable that governs
concurrency. Today it is derived statically from the user's configured concurrency limit; the self-scaling
work makes it adaptive. Distinct from thread count: threads are capacity, admission is the throttle, so the same
mechanism governs the thread-pool and async engines alike.

**In-flight work**
Records handed to the worker pool and not yet resolved. Distinct from records merely fetched:
in-flight work is what a commit must wait for, and what a shutdown must drain.

**Verdict**
The outcome a delivered record reports back: succeeded, or failed. Distinct from the *return* itself,
because work can come back with no verdict at all — when the process holding it disappears before
reporting on it. A verdict-free return is not a failure: it consumes no retry attempt and earns no
retry delay. Returns are matched to a delivery, so a late one arriving after the record has already
been redelivered is recognised as superseded and ignored rather than acted on twice.

**Commit frontier**
The offset a partition would resume from if consumption restarted — the highest offset committed for
it. It is *exclusive*: it names the next record expected to be polled, not the last one completed.

The frontier advances only across a contiguous run of completed records, so a completed record sitting
above an in-flight one does not move it. A partition can therefore have most of its work done and still
be committing its starting offset. That property is also what makes the frontier the thing worth
asserting about a commit: which intermediate offsets a partition commits on the way there depends on
when the periodic commit happens to fire, but where it ends up does not.

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

## Streams proxy

**Handle** (and **typed handle**)
In the experimental Streams protocol, the engine performs each of the host's builder calls against a
real topology builder and answers with a handle - a server-minted opaque integer the host may only
name back in later calls. The host never holds the builder object, only its number.

A *typed* handle travels with what it is: its kind (stream, grouped stream, table) and the key and
value types it carries. The type matters because an operator can mint a value the host never supplied
- a count produces a table of longs - and without the type on the wire, what a handle carries is
engine-side convention the host has to know rather than be told.

## Test reliability

**Load-tightness flake**
A test that fails because a deadline or assertion margin is too tight to survive a contended machine,
rather than because the code under test is wrong. Named as a family here because its members look
identical to real product stalls and must be told apart before either is "fixed".

**Unforceable trigger**
A test that awaits a consequence whose precondition it cannot guarantee will occur. Distinct from a
load-tightness flake: the margin is not too small, the awaited event may simply never happen in some
interleavings, so no timeout is long enough to make the test reliable.

**Tick-path assertion**
A test assertion that pins the intermediate observations a system passed through on its way to a settled
state, rather than pinning the settled state itself. Because those intermediates are only captured when
a periodic action happens to fire, the assertion's truth is a property of the machine's speed rather
than of the code under test — green on an idle box, red under load, with both readings correct.

Distinct from a load-tightness flake and an unforceable trigger: no deadline is too tight, and no
awaited event is missing. The awaited condition is one of several legitimate outcomes, and once a
different one has occurred it can never become true, so the wait always expires in full.

**Ambient probe**
The always-on recorder attached to broker integration tests, which annotates a failure with
consumer-group progress evidence so the contention-versus-product-bug question is answered before
manual diagnosis starts. Its verdict is only informative when its detectors could have fired for the
test in question — a short, low-volume test cannot trip them, and a clean reading there means nothing.

**Red-proof**
The verification that a new or extended test fails against the code as it was before the fix it
guards — a regression test that has never failed proves nothing.

The proof requires a deliberately mismatched pair: old code, new tests. Any procedure that reverts
both together produces a matched pair and a vacuous pass, so a red-proof that does not go red is
first evidence against the method, not for the code.

## Flagged ambiguities

- **"Stall", "load-tightness flake" and "unforceable trigger" had been used loosely for the same red
  test.** They are distinct: a stall is the product failing to make progress, a load-tightness flake is
  a real deadline missed under contention, and an unforceable trigger is an awaited event that never
  occurred. All three present as the same expired await, and the whole diagnostic difficulty of this
  area is telling them apart.
- **"Batching" was doing two jobs.** It meant both the user-facing API shape and the number of
  records per boundary crossing, and because one word covered both, they were being weighed as a
  single decision. They are independent: see **bundling** under Naming. Resolved 2026-08-24, after
  the crossing cost was measured and the two answers turned out to differ.
- **A tick-path assertion presents as that same expired await, and is the fourth member of the
  confusion.** It is told apart by asking whether what the test actually saw is *also correct*: the
  other three all mean the expected thing did not happen, while a tick-path assertion means something
  equally valid happened instead.
