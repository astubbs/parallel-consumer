# The retry queue's waiting methods have a declared contract and no runtime guard

<!-- inflight-type: task -->
<!-- inflight-impact: reliability -->
<!-- inflight-labels: concurrency -->
<!-- inflight-state: deferred - the static half has landed; this half needs a decision on what "fail loudly" does inside consumer.poll() -->

`RetryQueue.remove`, `add`, `removeAll` and `clear` take the write lock unconditionally, so a caller that is
not allowed to wait must never reach them - which in this engine means the controller thread and nothing else.
That contract is now DECLARED, by
[`@ControllerThreadOnly`](../../parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/ControllerThreadOnly.java),
and CHECKED statically, by `ArchitectureTest.rebalanceCallbacksMustNotBlock`, which reports a reach into an
annotated method exactly as it reports a reach into a deny-listed JDK blocking call. This note is the other
half: the runtime guard that would hold the same contract where a static walk cannot follow.

## The shape, copied from a guard this repo already runs

astubbs/parallel-consumer#393 did it for the consumer, and the pattern is two pieces:

- `ConsumerOffsetCommitter` holds a `volatile Optional<Thread> owningThread`, set once by `claim()` when the
  poll thread's control loop starts, and read by `isOwner()` - `Thread.currentThread().equals(owningThread
  .orElse(null))`. The claim is a LIFECYCLE step taken by the owning thread itself, not a constructor
  argument, because the object is built before the thread that will own it exists.
- `PCModule.consumerManager()` wraps the user's consumer in `ThreadConfinedConsumer` (grep
  `thread-confinement`), whose own comment states the rule this note copies: ownership is claimed when the
  loop starts, and calls before that are allowed from any thread.

Applied to `RetryQueue`: the controller claims the queue when the control loop starts; every
`@ControllerThreadOnly` method asserts that `Thread.currentThread()` is the owner and fails loudly instead of
taking the lock; and the guard is UNARMED while no controller has claimed, so unit tests that drive a
`RetryQueue` directly - and any init-time use - are unaffected. Unarmed-by-default is the part that makes this
cheap to land: it changes nothing until a real control loop exists.

## Which annotation, and why the pairing rule makes this note necessary

`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md` owns the rule - "Declare thread
confinement with `@ThreadConfined`, and assert it at the entry point". Infer's `@ThreadConfined` is CONSUMED by
RacerD and never checked, so an unpaired one silences a detector and is worse than no annotation at all. The
two existing patterns are named there: `RetryQueue.RetryQueueIterator` carries `@ThreadConfined(ThreadConfined
.ANY)` plus an `assertOnOwningThread` at every entry point with `RetryQueueIteratorConfinementTest` failing
when the two disagree, and `ThreadConfinedConsumer` is the older hand-rolled version of the same idea for the
poll thread. Both were established by astubbs/parallel-consumer#433.

**The guard this note describes is that shape with a NAMED thread rather than `ANY`** - the control thread,
which the rule says is the right value when the code really does pin one, and which is what gives the
assertion something specific to compare against. `@ControllerThreadOnly` is deliberately not that annotation:
no analyser reads it, so it silences nothing and the pairing rule's rationale does not reach it. When the
runtime guard lands, the marker may fold into the `@ThreadConfined` + assertion pair.

## What it covers that the static rule cannot

The ArchUnit rule's own javadoc enumerates its blind spots, and each one is a way for a poll-thread call to
arrive at a waiting acquire with the rule green:

- **A stored reference.** ArchUnit's model gives a reference invoked now (a stream stage) the same shape as
  one invoked later (a metrics gauge, an executor task), so the rule cannot say WHEN a reach happens - it is
  conservative about immediate reaches and silent about deferred ones.
- **A user-supplied `ConsumerRebalanceListener`.** Dynamic dispatch through an interface is out of reach of any
  deny list, and a user listener is exactly the code the walk cannot start from.
- **A `synchronized` block.** A `MONITORENTER` is not an access, so it is invisible at any depth - the reason
  the rule would not have caught confluentinc#857 itself.

A runtime owner check does not care how the call arrived. It costs one reference compare per call on a path
that is already taking a lock.

## Open design questions - these are what defer it

- **What "fail loudly" means on the poll thread**, which is the binding one. The call is inside
  `consumer.poll()`, so a thrown exception leaves a Kafka rebalance callback abnormally and its blast radius
<!-- post-merge: checked-begin -->
  is the group, not the caller; "log an error and decline the removal" is the alternative, and it is the
  behaviour the queue-first sweep already treats as safe. Throwing is the better signal in a test and the
  worse one in production, which is the trade to settle - and astubbs/parallel-consumer#431 settled the same
  trade for the static half by declining rather than throwing, which is the precedent to weigh rather than a
  decision already taken here.
<!-- post-merge: checked-end -->
- **Claim and release across a restart.** `ConsumerOffsetCommitter.claim()` is called once and never released.
  A controller that stops and starts again, or a second `ParallelEoSStreamProcessor` in the same JVM, needs a
  decision on whether a claim can be replaced, refused, or dropped at close - and on what an assertion does in
  the window between them.

## Where to look when picking this up

- `ConsumerOffsetCommitter`, grep `owningThread` and `isOwner` - the reference implementation, including why
  the claim is invisible to a grep for `.claim(`.
- `PCModule`, grep `thread-confinement` - where a wrapper is wired, and the "claimed when the loop starts"
  rule stated in a comment.
- `docs/inflight/static-archunit-main-code-rules.md`, "the rule now enforces a contract the CODEBASE declares"
  - what the static half does and what it measured.
