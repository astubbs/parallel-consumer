---
title: Give a single-slot metadata field one owner and let the other side ride inside it as an opaque blob
date: 2026-08-10
category: architecture-patterns
module: parallel-consumer-streams
problem_type: architecture_pattern
component: service_object
severity: high
applies_when:
  - Two independently-versioned systems both need to write the same single-slot field (one string, one column, one header)
  - The two systems ship on different release schedules and cannot coordinate a version bump
  - One of the two layers owns correctness for whatever the field primarily exists to protect
  - You are about to design a merged format that each side must partially parse
  - You are about to alternate writers, so the field's meaning depends on who wrote last
tags:
  - data-contract
  - single-writer
  - opaque-payload
  - commit-metadata
  - versioning
  - kafka-streams
  - interoperability
---

# Give a single-slot metadata field one owner and let the other side ride inside it as an opaque blob

## Context

A Kafka consumer-group commit carries exactly one metadata string per partition. Two
systems in `parallel-consumer-streams` both have a legitimate claim on it.

Kafka Streams writes its own structure there. In the 3.9.2 sources
(`org/apache/kafka/streams/processor/internals/TopicPartitionMetadata.java`, from
`~/.m2/repository/org/apache/kafka/kafka-streams/3.9.2/kafka-streams-3.9.2-sources.jar`)
`encode()` at line 53 lays down a magic byte (`LATEST_MAGIC_BYTE = 2`, line 34), then the
partition time as a long, then a serialised processor-metadata map, base64-encoded.
`decode()` at line 63 switches on that leading version byte.

Parallel Consumer writes something else entirely: the frontier plus its holes. The
consumer-group offset is the frontier (the lowest incomplete offset), and the metadata
field carries the encoded set of offsets completed *beyond* it, so a restart resumes
without losing in-flight records and without repeating completed ones. See `CONCEPTS.md:74`
("Frontier") and `CONCEPTS.md:82` ("Frontier semantics"). That encoding is not a nicety.
It is the entire reason crash safety survives out-of-order completion, and no single
number can replace it.

Both writers are correct for their owner. There is one slot. This is the shape of problem
KTD-S7 settled in `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:250-286`, for
astubbs/parallel-consumer#271 (issue astubbs#255).

## Guidance

**Do not interleave. Do not merge. Pick one owner, and give the other side an opaque
rider inside the owner's format.**

The decision recorded as KTD-S7 is that PC owns the commit metadata field wholesale on the
PC path. `committableOffsetsAndMetadata()` in the patched `StreamTask` returns PC's map
directly rather than building a Streams structure
(`parallel-consumer-streams/src/main/patch/pc-streams.patch:370-381`), sourced from
`PcTaskDispatcher.collectCommitData()`
(`parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcTaskDispatcher.java:369-372`),
which delegates to
`WorkManager.collectCommitDataForDirtyPartitions()`
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/WorkManager.java:201-203`).
Offset, metadata and the too-large fallback all come from one place. Streams'
`TopicPartitionMetadata` is simply not written for input partitions on that path.

The second half of the decision is what makes it a reusable pattern rather than a
one-off exclusion. When something on the embedder's side genuinely needs to persist across
restarts, it does **not** become a second writer. PC's own codec grows one generalised
extension slot: the embedder hands PC a byte blob, PC carries it inside its versioned
payload, and hands it back on read
(`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:275-286`, restated as the settled
direction at `:1242-1243`). PC never interprets the blob. It needs to know the rider's
**length**, never its meaning.

Generalising it was deliberate. The slot is not "the Kafka Streams field" - the Streams
bridge is merely its first customer, and any future embedder gets the same rider on the
same terms.

Three properties follow, and they are the reason to reach for this shape:

- **One decoder.** Exactly one component parses the field, so there is exactly one place
  where a malformed value is diagnosed.
- **Independent versioning.** The rider's schema can change on the embedder's release
  schedule without touching the owner, because the owner's parse never descends into it.
  The boundary is a data contract (bytes plus a length), not a shared format.
- **Compatibility becomes a decision, not an emergent property.** What a foreign reader
  sees when it opens the field is now something the owner chooses and can test, rather
  than a property that falls out of whichever writer happened to run last.

Ownership goes to **whichever side's correctness depends on the field**, not to whichever
system wrote it first. Here, PC's frontier-plus-holes encoding is what makes crash safety
possible at all; Streams' partition time is a watermark that is recoverable by other
means. Seniority in the stack is not the tie-breaker. Consequence of loss is.

## Why This Matters

The two rejected alternatives both fail in ways that only show up later, which is why
they are worth naming explicitly.

**Merging the two schemas into one field.** Each side must then understand enough of the
other's format to skip past it. That makes every version bump on either side a coordinated
release across two projects that do not share a release train, and the coupling is
permanent: it never gets smaller, only more entangled as both formats grow. The plan's
rejection at `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:272-273` is blunter -
two decoders would each read the other's bytes as corruption, and the field would carry
two owners forever.

**Alternating or interleaving writers.** The field's meaning then depends on who wrote
last. That is unreadable in an incident (you cannot tell a stale foreign value from a
current one) and untestable in CI (the interesting states are timing-dependent
interleavings you cannot enumerate).

The rider is strictly better than both because it converts a *format* coupling into a
*length* coupling, and length is the one thing that does not change meaning when a schema
evolves.

## When to Apply

- A single-slot field (one string, one column, one HTTP header, one filename suffix) that
  two independently-versioned systems both need to write.
- The systems ship on different schedules, so "just coordinate the version bump" is a cost
  paid forever rather than once.
- One of them is the layer that owns correctness for the field's primary purpose. Give
  that side ownership, even if it is the newer arrival.
- Not applicable when the field is genuinely multi-slot (a map, a list of headers, a table
  with room for another column). Add a slot instead. This pattern is for when you cannot.

## Examples

### The cost, stated honestly

Single ownership means the field stops being interchangeable. A stock Kafka Streams
instance reading a group whose offsets PC committed will not find its own structure there.
Partition time is not persisted, processor metadata is not persisted, and two of Kafka's
own `StreamTaskTest` cases that assert Streams' metadata encoding stay red by design
(`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:269-270`).

The verified behaviour is that it **degrades** rather than corrupts. PC's payload is valid
base64 whose leading magic byte is a printable letter
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/OffsetEncoding.java:28-43`
gives the set: `L`, `l`, `a`, `n`, `J`, `o`, `s`, `e`, `p`), never `1` or `2`. So Streams'
`decode()` takes its version-switch default branch, logs "Unsupported offset metadata
version found", and returns UNKNOWN. When PC's too-large fallback strips the payload and
commits a bare offset
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/PartitionState.java:512-525`),
`decode()` returns early on the empty string with no warning at all.

"Degrades gracefully" is a weaker promise than "interoperates", and it is only a promise
if it is tested. It is:
`stockRestartOnPcCommittedGroupDegradesGracefully()` in
`parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/CommitFrontierCrashRestartTest.java:201-254`
runs a PC-dispatched topology, crashes it so the group's **last** commit is a
holes-bearing PC payload (an orderly close would leave a bare offset with empty metadata
and prove nothing, per the comment at `:218-222`), then takes the same group over with
stock dispatch and asserts stock Streams resumes and produces. The assertion is
behavioural rather than a pinned log line.

Any design taking this route owes itself that test. Assuming the foreign reader is lenient
is exactly the assumption that turns a graceful degradation into a silent corruption.

### What the two-owner world already looked like, before this decision

PC's core carries scar tissue from the reverse direction, and it is the best available
argument for the pattern. `OffsetEncoding` reserves two magic bytes purely to *recognise*
Kafka Streams' format
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/OffsetEncoding.java:45-50`),
and `EncodedOffsetPair` has a dedicated branch for them
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/EncodedOffsetPair.java:123-130`)
whose only two outcomes are "warn, discard the offset map, possibly reprocess" or "throw".
That branch is governed by a user-facing option,
`InvalidOffsetMetadataHandlingPolicy`, defaulting to `FAIL`
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/ParallelConsumerOptions.java:334-363`).

An entire enum, a decoder branch, and a public configuration option exist because one
field had two possible authors. None of that gets better with time; it is what "the field
carries two owners forever" costs in practice. The rider makes the same coexistence cost a
length prefix.

### Choosing the owner

The tie-break question is not "who was here first" or "who is lower in the stack". It is:
*if this field is wrong or missing, which side breaks unrecoverably?*

- PC without its encoding: records that completed out of order are silently replayed or
  silently lost. Unrecoverable, and invisible.
- Streams without its partition time: stream time restarts as UNKNOWN and re-derives from
  incoming records. Degraded, and self-healing.

That asymmetry picks the owner. It also predicts the rider's first customer: both of the
displaced tenants are time watermarks (partition time, and emit-final's per-processor
last-emitted-window-close timestamps), so the natural moment to build the rider is the
stream-time work that needs somewhere to persist a low-water mark anyway
(`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:280-284`).

One budget note for whoever builds it: the broker caps commit metadata
(`offsets.metadata.max.bytes`, default 4096), and every rider byte competes with PC's own
hole encoding. The too-large fallback at
`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/PartitionState.java:512-525`
must account for both, or the rider will quietly evict the encoding that is the reason the
field has an owner in the first place.

## Related

- `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:250-286` - KTD-S7, the decision,
  its accepted consequences, and the rider direction. Restated at `:1242-1243`.
- `docs/solutions/architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md` -
  how the `StreamTask` change that carries this decision is delivered.
- `CONCEPTS.md:74` and `CONCEPTS.md:82` - "Frontier" and "Frontier semantics", the encoding
  this field owns.
- `docs/inflight/pr-strategy-doc-merge-triggers.md:68-79` - why the shape of this answer is
  strategic, and the condition under which it would count as a real limit.
- astubbs/parallel-consumer#271, issue astubbs#255.
