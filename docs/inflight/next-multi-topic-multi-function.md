# Next: per-topic processing functions, and fair capacity across topics

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->

**Tracking: [astubbs#254](https://github.com/astubbs/parallel-consumer/issues/254)** (mirror of
`confluentinc#372`), *"Have processing functions attached to topics instead of a global one for all
subscribed topics"*. Open.

PC already subscribes to **many** topics - `subscribe(Collection<String>)` and `subscribe(Pattern)`,
both with an optional `ConsumerRebalanceListener`. What it does not do is run a **different function
per topic**: one user function is applied to every subscribed topic, so a multi-topic consumer must
switch on the topic inside its own handler, losing type safety and any per-topic configuration.

## The related issues, which are really one design cluster

None of these is independent, and answering them separately would produce four incompatible answers:

| Issue | Ask | Why it belongs here |
|---|---|---|
| **astubbs#254** (`confluentinc#372`) | Per-topic processing functions | The core of this note |
| **astubbs#243** (`confluentinc#175`) | Separate consume and produce key/value types | `upstream-map.yaml` (`sweep-2023-api-shape`) says these two **must be designed together**: per-topic handlers pay off precisely when topics carry different types, which is what #175 blocks. Both are breaking, so one major release, not two |
| **astubbs#236** (`confluentinc#50`) | Prioritise some topics over others when subscribed to several | Once functions are per-topic, "how much capacity does each get" is immediately askable - see the scheduling section |
| **astubbs#150** (`confluentinc#314`) | With KEY ordering, combine queues from different partitions **or topics** | Cross-topic key identity: does the same key in two topics share a queue? |
| **astubbs#245** (`confluentinc#187`) | Change topic subscription before or after PC has started | A dynamic subscription needs a place to attach a function |
| **astubbs#244** (`confluentinc#183`) | Unordered processing for null-key records in KEY mode | Adjacent: what the concurrency key is when there isn't one |

## Capacity across topics: work-conserving fairness, not reservation

**Recorded from the owner, 2026-08-21**, as the design position:

> You would not isolate topic function running, and you would not reserve capacity per topic. You
> limit them when they are all at full capacity so they get a fair share each - but when one is not
> running, of course you distribute the work better.

That is **work-conserving max-min fairness**: guarantee a share only under contention, and give the
whole budget to whoever can use it when others are idle. Reservation is the naive alternative and it
wastes the reserved capacity of an idle topic. This is a well-understood scheduling shape - weighted
fair queueing and deficit round-robin are the standard forms - and it is not novel or hard.

**PC is unusually well placed to do this**, which is worth saying because it is not obvious: fair
traversal already exists. `LoopingResumingIterator` and the fair shard/partition traversal work
(`docs/features/fair-partition-traversal.yaml`) exist precisely to stop one shard starving others.
Extending that traversal to be topic-aware is the same mechanism at another level, not a new
subsystem.

**For contrast, llingr's answer is deliberately not this.** Its `WithOverflowGuard` is a shared
buffered channel passed to several single-topic consumers, giving them a common burst allowance. Its
own FAQ is candid that it is not a scheduler:

> *"An overflow channel, not a scheduler. If a surge on one topic fills the core concurrency, the
> engine spills the excess into a secondary overflow channel, and the ingestion loop blocks briefly
> until either channel frees an execution slot... The gate is simple and fast; it caps what a runaway
> topic can take, so a single heavy stream cannot permanently starve the rest of the application."*

The implementation is a plain `select` race between two channels - **no priority, no weighting, no
fairness guarantee**, and Go-only. It bounds the damage a runaway topic can do; it does not allocate
shares. So it is a useful reference for the *problem statement* and an explicit non-answer for the
mechanism. See [`market-analysis-llingr.md`](market-analysis-llingr.md).

## A user-supplied concurrency key, and the co-partitioning limit

**Recorded from the owner, 2026-08-21.** A related capability, wanted independently of multi-topic:
let the user supply a function that extracts the **concurrency key** from anywhere - typically a field
in the message body - rather than always using the Kafka record key.

**The limitation is real and must be explained to the user, not buried.** Kafka co-partitions by
*record key*. If a user extracts a key from the body, records sharing that extracted key can land on
**different partitions**, and different partitions may be assigned to **different consumer
instances**. A given PC instance therefore cannot see all records for that extracted key, and
**cannot guarantee ordering for it**. The guarantee silently degrades from "ordered per key" to
"ordered per key, per instance" - which is not a useful guarantee and is the kind of thing that looks
correct in testing and fails in production under a rebalance.

**The exception, and it is the important half:** if the extracted key is a **function of the record
key** - that is, if identical record keys always yield identical extracted keys - then co-partitioning
still holds, because everything that shares an extracted key already shares a record key and therefore
a partition. Coarsening is safe (extracting `customer` from a record keyed `customer:order`);
re-keying to an unrelated field is not.

**Implementation requirements that follow:**

1. The API must make the safe case easy and the unsafe case *loud*. Options, not yet decided: accept
   only a derivation from the record key; or accept an arbitrary extractor while requiring the caller
   to declare that co-partitioning holds; or degrade explicitly to unordered when it does not.
2. **PC cannot verify the property at runtime** - it only sees its own assigned partitions, so it
   cannot know whether a key appears elsewhere. This is a documentation and API-shape problem, not a
   validation one.
3. It interacts with **astubbs#150**, which asks for exactly this across topics - the same
   co-partitioning question with an extra dimension, since two topics need not even share a partition
   count.
4. It interacts with **astubbs#244**: what the concurrency key is for a null-key record.

### Prior art: searched exhaustively, and the caveat appears to be new

Searched 2026-08-21 across both issue trackers (`--state all`), all upstream discussions, every remote
branch's Java sources for `keyExtractor|KeySelector|shardKeyFunction` (**zero hits on every branch**),
and all markdown/YAML for `concurrency key|grouping key|user-defined key|pluggable key`.

**No prior art for a user-supplied key-extraction function.** What exists is adjacent:

- **astubbs#150** (`confluentinc#314`) is the *inverse* problem - combining shards that co-partitioning
  already split - and its thread is the only place the co-partitioning objection has been argued, by
  the then-upstream maintainer: *"there is really no easy and bulletproof way to guarantee
  deterministic ordering of messages with same key when consumed from different topics across
  rebalances, subscription changes - even poll batching / sizing may play a role"*.
- **`ShardKey.KeyOrderedKey` includes the partition in the shard key** -
  `this(new TopicPartition(rec.topic(), rec.partition()), rec.key())` - which is the mechanism any
  such feature has to modify. `ShardKey`'s own javadoc calls the class *"extendable"*, but no
  user-facing seam exists.
- The nearest *derived*-key proposal in writing is `upstream-map.yaml` id `sweep-2023-null-key-ordering`,
  for astubbs#244: *"key the unordered case by record offset (already unique within the partition)
  behind an option defaulting to today's behaviour"*.

**The subset rule appears to be genuinely new, and it is the answer to that maintainer objection.**
Nothing in either repository states that a body-extracted key is safe when it is a *function of* the
record key. That is the argument which converts "no bulletproof way" into "bulletproof exactly when
the extractor coarsens rather than re-keys" - so it should be written into astubbs#150 and astubbs#254
rather than left here.

**One cheap follow-on found in passing:** astubbs#150 names two defects in `ShardKey` - an inverted
javadoc (since fixed by `52b4a1061`/`a057b6c3d`) and a dead assertion in `ShardKeyTest.keyTest()`,
which builds `reck4` then asserts on `reck2`. The dead assertion is worth re-checking.

## Two things the prior-art search settled

**The upstream issue claims it was implemented. It was not.** `confluentinc#372`'s body says
*"Implemented in: #390"*, and upstream PR #390 (`astubbs/features/streams`, head `e2f1d53e4`) was
**closed unmerged** in the 2023-06-15 sweep. The mirror astubbs#254 already records this. The commit
is reachable, so the draft is recoverable - but read it as a draft, not as shipped work.

**Branches worth reading before designing:** `origin/features/extend-functional`,
`origin/refactor/function-runner` ("START: major: Function Runner refactor"), and
`origin/improvements/multi-topic-test`. Also relevant: `confluentinc#184` was a multi-topic KEY-ordering
**bug**, fixed by #315 *"adding topic to shard key"* - so the shard key already carries the topic, which
is what makes per-topic behaviour tractable.

## Open design questions

- **Function attachment** - per topic at subscribe time, a map, a builder, or a router? Type safety is
  the main prize, so a shape that preserves per-topic key/value types is worth more than a `Map<String,
  Function>`.
- **Does each topic get its own ordering mode, concurrency and retry policy?** If per-topic functions
  arrive without per-topic configuration, the next issue is immediate.
- **Cross-topic key identity** (astubbs#150) - is `key=A` in topic X the same shard as `key=A` in
  topic Y? Defaults matter: sharing is surprising, not sharing is also surprising.
- **Pattern subscriptions** - a `Pattern` subscription cannot enumerate its topics in advance, so
  per-topic functions need a fallback or a resolver.
- **What is the fair-share unit** - in-flight records, or records-per-second? The measurements in
  [`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md) show
  throughput is non-monotonic in the in-flight ceiling, so a share expressed in in-flight slots does
  not translate linearly into a share of throughput.

## Related

- [`market-analysis-llingr.md`](market-analysis-llingr.md) - llingr is single-topic by hard
  constraint, because its offset trackers are keyed by partition number alone with no topic in the
  key. PC keys by `TopicPartition`, which is what quietly bought multi-topic support.
- [`next-feature-data-cross-reference-llingr.md`](next-feature-data-cross-reference-llingr.md) - the
  multi-topic row and its correction.
- [`next-auto-scaling.md`](next-auto-scaling.md) - fair shares under contention and adaptive total
  concurrency are the same control problem seen from two sides.
