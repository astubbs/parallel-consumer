---
title: "A type gate is a claim about a hierarchy you did not write"
date: 2026-08-11
category: architecture-patterns
module: parallel-consumer-streams
problem_type: architecture_pattern
component: service_object
severity: critical
applies_when:
  - "A refusal list, allowlist or validation layer decides what it accepts by testing `instanceof` against a third-party interface"
  - "The interfaces being keyed on belong to a dependency whose type hierarchy you do not own and cannot see the whole of"
  - "A second route reaches the same machinery past the layer above it (a Processor API alongside a DSL, a builder alongside a facade)"
  - "The cost of a missed case is a silent wrong answer or a dropped write rather than an exception"
  - "A plan enumerates what to refuse from the operators a user would name, rather than from the types the framework actually instantiates"
symptoms:
  - "`VersionedKeyValueStore` extends `StateStore` directly, so an `instanceof WindowStore` gate never sees it"
  - "The missed store is reachable with no refused DSL call at all, unlike every other refused store"
  - "The store drops writes older than its observed stream time minus the grace period, so the miss costs data"
  - "The gap was found by the branch's own review pass, not by the implementation plan that designed the gate"
related_components:
  - PcSupportedEnvelope
  - PcUnsupportedConstruct
  - kafka-streams state stores
tags:
  - type-gating
  - instanceof
  - allowlist-completeness
  - third-party-hierarchy
  - kafka-streams
  - state-stores
  - silent-data-loss
  - fail-closed
---

# A type gate is a claim about a hierarchy you did not write

> Extracted from `origin/feats/ks-streams-refuse-unsupported-surface` @801c87424, `docs/solutions/architecture-patterns/a-type-gate-is-a-claim-about-a-hierarchy-you-did-not-write.md`.

## Context

`parallel-consumer-streams` runs an Apache Kafka Streams topology on Parallel Consumer's dispatch:
records on different keys go to a worker pool instead of one at a time down the `StreamThread`. Several
Kafka Streams constructs are correct only because that dispatch used to be single-threaded and in order,
and when it stops being either, they do not fail - they answer wrongly. Stream time never advances
because `PartitionGroup.nextRecord()` is bypassed, so every window close, join emission and suppression
emission that gates on it gates on a number that is frozen
(`docs/plans/2026-08-10-001-feat-refuse-unsupported-streams-surface-plan.md:25-40`). Nothing throws and
nothing logs. The module's answer is to **refuse** those constructs rather than document them as caveats.

The refusal is built in layers. Layers 1 and 2 sit on the DSL methods themselves: patched bodies in
`KStreamImpl`, `KTableImpl`, `KGroupedStreamImpl` and friends call
`PcUnsupportedConstruct.<CONSTRUCT>.refuse()` on entry
(`parallel-consumer-streams/src/main/patch/pc-streams.patch:71-225`). Layer 3 is the backstop, and it
exists because the DSL is not the only way in. `topology.addStateStore(Stores.windowStoreBuilder(...))`
attaches a window store to a plain `Processor` without ever constructing a `KStream`, so no patched DSL
method is called and layers 1 and 2 are never consulted
(`parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcSupportedEnvelope.java:22-27`).
The backstop runs once, in the patched `StreamTask` constructor, over `ProcessorTopology.stateStores()`
plus the task config's exactly-once flag, and it runs *before* the PC dispatcher is created so a refused
task never allocates a worker pool nothing will shut down.

**The backstop gates on type, and that was the right call.** The stores that reach `stateStores()` are
wrapped several layers deep - a metered store over a change-logging store over the bytes store - and
every wrapper implements the interface it wraps. `instanceof` sees through the whole stack; matching on
class names sees only the outermost wrapper and breaks the first time Kafka adds one
(`PcSupportedEnvelope.java:33-37`). So classification became a chain of `instanceof` checks with a
default of "supported" - as first written:

```java
private static PcUnsupportedConstruct classify(final StateStore store) {
    if (store instanceof TimeOrderedKeyValueBuffer) {
        return PcUnsupportedConstruct.SUPPRESSION_BUFFER;
    }
    if (store instanceof SessionStore) {
        return PcUnsupportedConstruct.SESSION_STORE;
    }
    if (store instanceof WindowStore) {
        return PcUnsupportedConstruct.WINDOW_STORE;
    }
    return null;
}
```

That default is not laziness. A plain key-value store is the one *supported* stateful case for this
module - non-windowed aggregation is inside the envelope, and refusing it would silently delete the
module's own stateful proof (`PcSupportedEnvelope.java:124-128`). Something had to fall through, and a
key-value store is what falls through.

`VersionedKeyValueStore` also fell through. In the Kafka this module builds against - **3.9.2**,
`pom.xml:116` - it is declared

```java
public interface VersionedKeyValueStore<K, V> extends StateStore {
```

It extends `StateStore` **directly**. It is not a `WindowStore`, not a `SessionStore`, not a
`TimeOrderedKeyValueBuffer`. Every check in the chain missed it and the default let it through, because
by every signal the gate was reading, a versioned store is an ordinary key-value store.

It was caught by this branch's own review pass, the same way as the findings in
[Fresh work needs an independent reviewer](../best-practices/fresh-work-needs-independent-review.md).
That is worth one sentence and no more, because **the review is how it was found; the hierarchy is why it
was missable**. A second, fully independent reviewer holding the same mental model - "the dangerous
stores are the windowed ones, and windowed things extend `WindowStore`" - misses it exactly as the plan
did. Independence does not supply the type graph. The plan records the miss as a property of Kafka's
types rather than of anyone's attention
(`docs/plans/2026-08-10-001-feat-refuse-unsupported-streams-surface-plan.md:508-513`, detection-table row
at `:146`).

### Where this sits next to its closest neighbour

[Kafka Streams task lifecycle callbacks do not mean what they are named](../integration-issues/kafka-streams-task-lifecycle-callbacks-do-not-mean-what-they-are-named.md)
is the same principle one axis over, and the two are worth reading together. That doc audits **who calls
in** to a hook: a callback's name is not its contract, its contract is the set of its call sites. This
one audits **what satisfies out** of an interface: an interface is not the set of things that behave like
it, the set is its implementations. Opposite directions along the same edge, and the same technique
settles both - unzip the framework's sources jar, enumerate the real set, pin the version.

The failure geometry is inverted, which is what makes this rule non-derivable from that one. There, a
hook **over-fires**: it runs when its name implies it cannot, and work is acked as committed when it was
not. Here, a gate **under-fires**: a hole in a denylist lets an unsupported store through. An over-firing
hook is found by anyone who traces the call sites. An under-firing gate is found only by someone who
enumerates the types, because nothing about the code that *is* there points at the type that is not.

## Guidance

### 1. Enumerate the implementations. Do not reason from the interface you happen to know.

When you write `store instanceof WindowStore`, you are not making a statement about `WindowStore`. You
are making a statement about *the set of types that could arrive here*, and you are asserting that you
know which of them are dangerous. In a hierarchy you wrote, that assertion is checkable by reading your
own code. In a third-party hierarchy it is a guess, and the sibling that skips the interface you keyed
on is invisible to it.

The concrete move: before writing the chain, list every implementation and every sibling interface of
the supertype you are actually receiving. Here the received type is `StateStore`, so the question is not
"which window-ish stores exist" but "what implements `StateStore` in Kafka 3.9.2, and which of those
break under concurrent dispatch". Asked that way, `VersionedKeyValueStore` is impossible to miss; asked
as "have I covered the windowed constructs", it is invisible. In this Kafka the four refused interfaces
all extend `StateStore` and none extends another, which is exactly why none of them can be reached by
reasoning downward from any of the others.

**State the framework version next to the claim.** The chain's own comment does this, and the reason is
that the claim expires:

```java
// The four store interfaces are disjoint in Kafka 3.9.2, so this order does not currently disambiguate
// anything. It is written most-specific-first anyway, so that a future Kafka in which one of them
// starts extending another reports the narrower construct rather than the vaguer one.
```

`PcSupportedEnvelope.java:130-132`. A hierarchy assertion pinned to a version is auditable at the next
dependency bump. An unpinned one is folklore.

### 2. A safety gate must default to refuse, not to allow

This is the compounding factor, and it is worth more than the enumeration itself.

`classify()` returns `null` for anything it does not recognise (`PcSupportedEnvelope.java:149`). Under
that policy, every type you failed to enumerate becomes a **silent pass** - the failure mode of an
incomplete list is exactly the failure mode you built the gate to prevent. Under a default-refuse
policy, an unenumerated type becomes a loud `UnsupportedOperationException` naming a store the author
has never heard of: annoying, immediately visible, and fixed in one commit by someone who now knows
about it.

**Why "enumerate it once" is not enough here, and this is the part that generalises.**
[A progress signal must count work consumed, not work accepted](a-progress-signal-must-count-work-consumed-not-work-accepted.md)
reaches a rule that sounds identical: enumerate every exit, and make the check structural so a fourth
exit added later cannot forget it. The difference is **who owns the set**. An exit set is one you wrote
and can see in a single file, so hoisting the increment above the branches genuinely closes it -
permanently. An implementation set belongs to the framework and *grows between releases*: Kafka can add
a sibling store interface in a patch bump, and your enumeration silently goes stale without a line of
your code changing. So when the set is not yours, "structural" cannot mean hoisting the check. It has to
mean **inverting the default**, because that is the only form of the rule that survives a dependency
upgrade you did not review.

The plan records the stronger rule as a real, deferred option: every public Kafka Streams API starts
refused until Kafka's own suite proves it with the seam on, rather than refusing a named set. It is
deferred because it needs the whole suite running first, "or the refused surface is defined by what
nobody has looked at"
(`docs/plans/2026-08-10-001-feat-refuse-unsupported-streams-surface-plan.md:493-496`, OQ1). That was
written as a preference with no incident behind it. Read alongside OQ4, it now has one: OQ1 does not
predict a missed store type, but a missed store type is exactly what the named-set approach costs, and
that is the argument OQ1 was missing. If you cannot flip the default yet, say so in the code and say
what would let you flip it.

### 3. "Reachable with no refused call" is its own axis of exposure

Check it separately from "is this type on my list", because a type can be absent from the list for
reasons that have nothing to do with how a user reaches it.

Layers 1 and 2 only fire for someone who called `windowedBy`, `join` or `suppress`. A versioned store
needs none of them. `Materialized.as(Stores.persistentVersionedKeyValueStore(...))` reaches one through
the DSL, and it does so *through the plain key-value overload*: `VersionedBytesStoreSupplier` extends
`KeyValueBytesStoreSupplier`, so the call binds to `Materialized.as(KeyValueBytesStoreSupplier)` and the
DSL then switches internally on `supplier instanceof VersionedBytesStoreSupplier` to build a versioned
store rather than a plain one. Note the two halves are ordinary for different reasons: the supplier
really *is* a `KeyValueBytesStoreSupplier` subtype, whereas the store is not a `KeyValueStore` subtype at
all and is merely indistinguishable from one to a chain that keys on the windowed interfaces. Both look
unremarkable at the call site, which is what matters. The Processor API route
(`topology.addStateStore(...)`) bypasses the DSL entirely and does not need `Materialized` at all.

So the exposure question is not "did I list this type" but "how many refused calls stand between a
naive user and this construct". When the answer is zero, the type gate is the *only* defence, and its
completeness is the whole safety story.

### 4. Keep the `instanceof`. Fix the enumeration.

The fix here was one more clause, not a redesign. Type-based classification remains correct for the
reason it was chosen - it sees through the wrapper stack, and the wrapper that reaches the gate does
implement the interface: `MeteredVersionedKeyValueStore` is declared
`implements VersionedKeyValueStore<K, V>`. The defect was never the mechanism. Do not let "our type gate
had a hole" turn into a rewrite of the gate.

## Why This Matters

**This one loses writes with no refused call in front of it.** Be careful with the tempting version of
this claim: a window store *also* discards records past its retention, with the same WARN and the same
dropped-records sensor. Dropping is not what makes the versioned store special. What makes it special is
that reaching a window store through the DSL means calling `windowedBy`, which layers 1 and 2 refuse,
whereas a versioned store discards records the same way with nothing refused in front of it.

`RocksDBVersionedStore` keeps a plain, non-volatile `long observedStreamTime` and advances it to the
maximum timestamp it has seen. On the way in, inside `synchronized (position)`:

```java
if (timestamp < observedStreamTime - gracePeriod) {
    expiredRecordSensor.record(1.0d, context.currentSystemTimeMs());
    LOG.warn("Skipping record for expired put.");
    StoreQueryUtils.updatePosition(position, stateStoreContext);
    return PUT_RETURN_CODE_NOT_PUT;
}
observedStreamTime = Math.max(observedStreamTime, timestamp);
```

`delete` guards on the same condition and drops just as quietly, returning `null` rather than the put
sentinel, which is indistinguishable from "there was no prior value". `gracePeriod` is the store's
history retention. The interface
documents the behaviour as designed, not as an implementation quirk: "A versioned store will not accept
writes (inserts, updates, or deletions) if the timestamp associated with the write is older than the
current observed stream time by more than the grace period."

Under sequential dispatch this drops only genuinely late data. Under concurrent dispatch, *processing
order stops tracking record order*, so a worker that gets ahead advances `observedStreamTime` on behalf
of records that have not been written yet, and a slower worker's older record can land outside the
window and be thrown away.

**Be precise about "silently".** The drop is silent *to the topology*, which is the part that matters:
`put` returns a sentinel rather than throwing, so nothing propagates into the processor and no user code
gets a chance to react. It is not traceless - it emits a WARN and increments the task's dropped-records
sensor. A topology that is not watching that metric sees a store that is simply missing data. Whether a
given deployment loses records depends on how far reordering runs against the configured history
retention, which is the user's choice, so the risk is configuration-dependent rather than certain, and
that is precisely why refusal at construction beats a runtime check.

Two smaller consequences worth carrying:

**The read side has a genuine visibility gap too, but do not lead with it.** `put` and `delete` mutate
`observedStreamTime` inside `synchronized (position)`, so those two paths are mutually consistent -
which means the dominant defect is dispatch *ordering*, not memory visibility. Reordering loses writes
even with perfect synchronisation. That said, `get(key, asOfTimestamp)` reads the same non-volatile field
with no lock at all, as does the range read, so readers can decide "history retention exceeded" from a
stale value. Do not let the presence of one `synchronized` block convince you the field is safe, and do
not let the field's non-volatility distract you from the ordering problem that would remain if it were
`volatile`.

**This class of bug is invisible to the tests you would naturally write.** Every test in the refusal
suite exercises a construct someone had already thought of. A test suite built from the plan's
enumeration confirms the enumeration back to you, at full green, and says nothing about the member you
never named. That is not a gap in test rigour that more tests fix - the gap is in the *list*, and only
enumerating the hierarchy or flipping the default to refuse closes it. The reviewer who found this was
not running a better test; they were reading the hierarchy instead of the list.

## When to Apply

- **Apply when** a gate, filter, allowlist or dispatch table branches on `instanceof`, `isinstance`,
  a class object, or a type tag from a hierarchy defined in a dependency. You cannot see the whole of it
  and the dependency's authors are free to add siblings in a patch release.
- **Apply especially when** the gate exists for *safety* - refusing what would corrupt, drop or leak -
  rather than for dispatch. A dispatch table that misses a type usually throws; a safety gate that
  misses a type lets it through.
- **Apply when** the gate's default branch is "allow". Write down what the default costs you when the
  enumeration is incomplete, and if you cannot flip it, record what would let you.
- **Apply at every dependency bump** that moves the gated hierarchy. Re-run the enumeration; a type
  hierarchy claim is version-scoped evidence with an expiry date, not a fact.
- **Also check** how many refused calls stand between a user and each dangerous construct, separately
  from whether the construct is listed. A construct reachable with zero refused calls has no second line
  of defence.
- **Do not apply** to hierarchies you own and can enumerate mechanically, where a sealed type, an
  exhaustive `switch`, or an abstract method that every subtype must implement gives you a
  compiler-enforced version of the same guarantee. Prefer that when it is available.
- **Do not apply** by replacing type checks with name checks. Name matching is strictly worse here: it
  sees only the outermost wrapper and cannot see through the metered and change-logging stack that Kafka
  actually hands you (`PcSupportedEnvelope.java:33-37`).

## Examples

**Before.** The chain as first written, covering the windowed constructs the plan enumerated, with a
default of "supported":

```java
if (store instanceof TimeOrderedKeyValueBuffer) {
    return PcUnsupportedConstruct.SUPPRESSION_BUFFER;
}
if (store instanceof SessionStore) {
    return PcUnsupportedConstruct.SESSION_STORE;
}
if (store instanceof WindowStore) {
    return PcUnsupportedConstruct.WINDOW_STORE;
}
return null;
```

**After.** One clause, and a comment that says why it is the one people miss
(`PcSupportedEnvelope.java:142-149`):

```java
// Easy to miss, and the most dangerous of the four: VersionedKeyValueStore extends StateStore directly
// rather than WindowStore, so it looks like an ordinary key-value store and is reachable with no refused
// DSL call anywhere - Materialized.as(Stores.persistentVersionedKeyValueStore(...)) is enough. Its
// observedStreamTime does not merely mis-order under concurrency, it silently discards puts.
if (store instanceof VersionedKeyValueStore) {
    return PcUnsupportedConstruct.VERSIONED_KEY_VALUE_STORE;
}
return null;
```

**The reason travels with the construct, not the throw site.** The enum carries the mechanism, so the
message a user reads names the actual failure rather than "unsupported"
(`PcUnsupportedConstruct.java:93-97`):

```java
VERSIONED_KEY_VALUE_STORE(
        "a versioned key-value store",
        "the store keeps a non-volatile observedStreamTime and silently DROPS any put older than "
                + "observedStreamTime minus the grace period, so concurrent dispatch loses writes rather than "
                + "merely reordering them - and reads outside history retention are rejected off the same field"),
```

**The test that states the lesson in its own name.**
`aVersionedKeyValueStoreIsRefusedEvenThoughItIsNotAWindowStore` (`PcSupportedEnvelopeTest.java:118-127`)
asserts classification directly, and its comment records why the store slipped through. Its pair,
`aVersionedKeyValueStoreBuiltThroughTheProcessorApiIsRefused` (`ProcessorApiBackstopTest.java:87-98`),
proves the patched `StreamTask` constructor actually calls the classifier, using a Processor API topology
with no `KStream` anywhere. The split is deliberate: a single end-to-end test would conflate "we do not
detect a session store" with "we never got asked", and those have different fixes
(`PcSupportedEnvelopeTest.java:25-28`).

**Real stores, not stubs - this is what makes the type gate actually tested.** The fixtures build
genuine Kafka stores, because a hand-rolled stub would satisfy the `instanceof` chain while telling you
nothing about the wrapper stack the backstop is really handed (`RefusedStoreFixtures.java:21-24`, which
makes the point with `WindowStore`). The versioned fixture (`:62-66`):

```java
static StoreBuilder<?> versionedKeyValueStoreBuilder() {
    return Stores.versionedKeyValueStoreBuilder(
            Stores.persistentVersionedKeyValueStore("versioned", Duration.ofDays(1)),
            Serdes.String(), Serdes.String());
}
```

Versioned stores have no in-memory variant in Kafka 3.9, and the fixture note explains why that is still
safe for a unit suite: building a store is not opening one, RocksDB is touched at `init`, and nothing
here initialises (`RefusedStoreFixtures.java:26-28`).

**The positive control is what keeps a default-refuse instinct honest.**
`aPlainKeyValueStoreStaysSupported` (`PcSupportedEnvelopeTest.java:161-170`) and
`aPlainKeyValueStoreTopologyStillConstructsWithTheSeamOn` (`ProcessorApiBackstopTest.java:187-195`)
assert that the one supported stateful case is *not* refused. Without them, a gate that refused every
state store would pass every other assertion in both classes. If you do flip a gate to default-refuse,
these are the tests that stop the flip from quietly deleting your supported surface.

**And the control arm that proves the gate is conditional at all.**
`everythingRefusedAboveConstructsNormallyWithTheSeamOff` (`ProcessorApiBackstopTest.java:171-185`) builds
every refused store with the seam off and asserts none of them throws. The refusal has to be a property
of the dispatch switch, because this module runs Apache Kafka's own unmodified suite with the seam off as
its behaviour-preservation evidence, and that suite builds EOS-enabled tasks and window stores among
them (`PcSupportedEnvelope.java:47-53`).

## Related

- [Kafka Streams task lifecycle callbacks do not mean what they are named](../integration-issues/kafka-streams-task-lifecycle-callbacks-do-not-mean-what-they-are-named.md) -
  the same principle on the other axis: that doc enumerates a hook's callers, this one enumerates an
  interface's implementations. Its audit checklist generalises with one word changed.
- [A progress signal must count work consumed, not work accepted](a-progress-signal-must-count-work-consumed-not-work-accepted.md) -
  the near-identical prevention rule over a set you *own*. Read the ownership distinction in Guidance 2
  before treating the two as one rule.
- [A high-water mark cannot express out-of-order completion](a-high-water-mark-cannot-express-out-of-order-completion.md) -
  the companion defect from the same module, and the doc that establishes why a missed store on this path
  is data loss rather than mis-ordering.
- [Fresh work needs an independent reviewer](../best-practices/fresh-work-needs-independent-review.md) -
  how this was found, though not why it was missable.
- [Kafka Streams couples polling and processing on one thread](../integration-issues/kafka-streams-couples-polling-and-processing-on-one-thread.md) -
  why single-threaded assumptions are load-bearing throughout Kafka Streams in the first place.
- `parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcSupportedEnvelope.java` -
  the backstop, the classify chain, and the reasoning for classifying by interface rather than by name.
- `parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcUnsupportedConstruct.java` -
  the refused set, each with the mechanism that breaks it.
- `docs/plans/2026-08-10-001-feat-refuse-unsupported-streams-surface-plan.md:508-513` - OQ4, the review
  finding that added versioned stores; `:493-496` - OQ1, the deferred default-refuse rule this finding
  now argues for.
- `astubbs/parallel-consumer#271`, issue `astubbs#255` - the open PR that introduces the module this
  backstop guards, and the tracking issue every refusal message carries.
