---
title: Give a single-slot metadata field one owner and let the other side ride inside it as an opaque blob
date: 2026-08-10
updated: 2026-09-07
category: architecture-patterns
module: parallel-consumer-core
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

> **Written 2026-08-10 on the Kafka Streams spike branches, migrated to `master` on 2026-09-06 with the
> slot it describes.** Three things were corrected on the way and are marked where they occur: the
> unreadable-metadata policy's default, which moved from `FAIL` to `IGNORE` in astubbs#207 after this
> was written; the enumeration of Parallel Consumer's magic bytes; and the budget note's account of
> what a rider costs, which is now measured rather than feared. Every `file:line` citation became a
> greppable anchor, and every `io.confluent` path a `bz.stub` one, per `docs/citations.md`. The `module`
> field moved from `parallel-consumer-streams` to `parallel-consumer-core`, because that is where the slot
> was built; the Streams module remains its first customer and is not on `master`. **What shipped** is a
> section of its own below - the rest of the document is the 2026-08-10 record and says what was known then.

## Context

A Kafka consumer-group commit carries exactly one metadata string per partition. Two
systems in `parallel-consumer-streams` both have a legitimate claim on it.

Kafka Streams writes its own structure there. In the 3.9.2 sources
(`org/apache/kafka/streams/processor/internals/TopicPartitionMetadata.java`, from
`~/.m2/repository/org/apache/kafka/kafka-streams/3.9.2/kafka-streams-3.9.2-sources.jar`)
`encode()` lays down a magic byte (`LATEST_MAGIC_BYTE = 2`), then the
partition time as a long, then a serialised processor-metadata map, base64-encoded.
`decode()` switches on that leading version byte.
<!-- file-refs: N/A - the Kafka Streams class is named as it appears inside a sources jar, not as a path in this repo -->

Parallel Consumer writes something else entirely: the frontier plus its holes. The
consumer-group offset is the frontier (the lowest incomplete offset), and the metadata
field carries the encoded set of offsets completed *beyond* it, so a restart resumes
without losing in-flight records and without repeating completed ones. See `CONCEPTS.md`,
anchors `**Commit frontier**` and `**Rider**`. That encoding is not a nicety.
It is the entire reason crash safety survives out-of-order completion, and no single
number can replace it.

Both writers are correct for their owner. There is one slot. This is the shape of problem
KTD-S7 settled in the Kafka Streams spike plan, which lives only on its branch -
`git show origin/feats/ks-on-pc-spike:docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md`,
anchor `KTD-S7` - for astubbs/parallel-consumer#271 (issue astubbs#255).
<!-- file-refs: N/A - the spike plan is read through git show and lives only on that branch; the gate's revision grammar does not recognise a branch name containing a slash -->

## Guidance

**Do not interleave. Do not merge. Pick one owner, and give the other side an opaque
rider inside the owner's format.**

The decision recorded as KTD-S7 is that PC owns the commit metadata field wholesale on the
PC path. The patched `StreamTask`'s prepare-commit path returns PC's map
directly rather than building a Streams structure (the patch and the dispatcher live only on the
Streams branches - `git show origin/feats/ks-streams-stream-time-lowwater:parallel-consumer-streams/src/main/patch/pc-streams.patch`,
anchor `Prepared {} task for committing (PC frontier)`; the document originally named that method
`committableOffsetsAndMetadata()`, which is not what the patch hooks), sourced from
`PcTaskDispatcher.collectCommitData()`
(`git show origin/feats/ks-streams-stream-time-lowwater:parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcTaskDispatcher.java`,
anchor `collectCommitData`), which delegates to `WorkManager.collectCommitDataForDirtyPartitions()`
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/WorkManager.java`, anchor
`collectCommitDataForDirtyPartitions`).
Offset, metadata and the too-large fallback all come from one place. Streams'
`TopicPartitionMetadata` is simply not written for input partitions on that path.
<!-- file-refs: N/A - the Streams module and its patch are read through git show and live only on that branch; the gate's revision grammar does not recognise a branch name containing a slash -->

The second half of the decision is what makes it a reusable pattern rather than a
one-off exclusion. When something on the embedder's side genuinely needs to persist across
restarts, it does **not** become a second writer. PC's own codec grows one generalised
extension slot: the embedder hands PC a byte blob, PC carries it inside its versioned
payload, and hands it back on read (the spike plan again, anchor `KTD-S7`, restated there as the
settled direction). PC never interprets the blob. It needs to know the rider's
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
permanent: it never gets smaller, only more entangled as both formats grow. The spike plan's
rejection (anchor `KTD-S7`) is blunter - two decoders would each read the other's bytes as
corruption, and the field would carry two owners forever.

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
own `StreamTaskTest` cases that assert Streams' metadata encoding stay red by design (the
spike plan records both, anchor `KTD-S7`).

The verified behaviour is that it **degrades** rather than corrupts. PC's payload is valid
base64 whose leading magic byte is a printable letter
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/OffsetEncoding.java`,
anchor `RunLengthV2Compressed`, gives the set), never `1` or `2`. So Streams'
`decode()` takes its version-switch default branch, logs "Unsupported offset metadata
version found", and returns UNKNOWN. When PC's too-large fallback strips the payload and
commits a bare offset
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/PartitionState.java`,
anchor `stripPayloadForSize`),
`decode()` returns early on the empty string with no warning at all.

**Corrected 2026-09-06 - the enumeration was incomplete.** This paragraph originally listed the
set as `L`, `l`, `a`, `n`, `J`, `o`, `s`, `e`, `p`. Two bytes were missing then or have been added
since. `ByteArrayCompressed` carries `0xEE`, which is not a printable letter at all; nothing writes
it, because that constant and its `ByteArray` sibling have no encoder and no decoder
(`docs/inflight/core-bytearray-encodings-have-no-codec.md`), so the claim holds for every byte PC
can actually emit rather than for every constant in the enum. `RiderEnvelope` carries `X`, added by
the work described under "What shipped" below, and `X` is a printable letter, which is why it was
chosen: the degradation above is unchanged by it.

"Degrades gracefully" is a weaker promise than "interoperates", and it is only a promise
if it is tested. It is:
`stockRestartOnPcCommittedGroupDegradesGracefully()` in `CommitFrontierCrashRestartTest`, on the
Streams branches only -
`git show origin/feats/ks-streams-stream-time-lowwater:parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/CommitFrontierCrashRestartTest.java`,
anchor `stockRestartOnPcCommittedGroupDegradesGracefully` -
runs a PC-dispatched topology, crashes it so the group's **last** commit is a
holes-bearing PC payload (an orderly close would leave a bare offset with empty metadata
and prove nothing, per the comment beside the crash), then takes the same group over with
stock dispatch and asserts stock Streams resumes and produces. The assertion is
behavioural rather than a pinned log line.
<!-- file-refs: N/A - the test is read through git show and lives only on that branch; the gate's revision grammar does not recognise a branch name containing a slash -->

Any design taking this route owes itself that test. Assuming the foreign reader is lenient
is exactly the assumption that turns a graceful degradation into a silent corruption.

### What the two-owner world already looked like, before this decision

PC's core carries scar tissue from the reverse direction, and it is the best available
argument for the pattern. `OffsetEncoding` reserves two magic bytes purely to *recognise*
Kafka Streams' format
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/OffsetEncoding.java`,
anchor `KafkaStreamsV2`), and `EncodedOffsetPair` has a dedicated branch for them
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/EncodedOffsetPair.java`,
anchor `KafkaStreamsEncodingNotSupported`)
whose only two outcomes are "warn, discard the offset map, possibly reprocess" or "throw".
That branch is governed by a user-facing option, `InvalidOffsetMetadataHandlingPolicy`
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/ParallelConsumerOptions.java`,
anchor `invalidOffsetMetadataPolicy`).

**Corrected 2026-09-06 - the default.** This paragraph said the option defaults to `FAIL`. It does
not, and has not since astubbs#207: the default is `IGNORE`, which logs, discards the metadata and
resumes from the committed offset. The change was forced by that PR making the policy govern *every*
unreadable path - the reporter who first hit this (astubbs#118, confluentinc#326) had configured
nothing, so a `FAIL` default would have made their exact report fatal again. `FAIL` remains available
and now means what it says. `docs/inflight/pr-207-offset-encoding-policy.md` owns that reasoning.

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
stream-time work that needs somewhere to persist a low-water mark anyway (the spike plan,
anchor `KTD-S7`).

One budget note for whoever builds it: the broker caps commit metadata
(`offsets.metadata.max.bytes`, default 4096), and every rider byte competes with PC's own
hole encoding. The too-large fallback in `PartitionState` (anchor `stripPayloadForSize`)
must account for both, or the rider will quietly evict the encoding that is the reason the
field has an owner in the first place.

**Measured 2026-09-07 - "competes with the hole encoding" was the wrong shape, and the cost is now
stated from measured strings rather than feared.** They compete for the broker's cap and nowhere
else. The single measurement this paragraph assumed is two: the hole encoding alone is judged
against the back-pressure threshold, and the assembled string, rider included, against the cap.
Rider bytes do not shrink as work completes, so charging them against back pressure would make a
rider a floor the mechanism can never relieve - and on a caught-up partition a permanent block -
which is why the split is the design and not an optimisation. The rider's cost against the cap is
closed-form, because the outer codec is Base64 with padding: `n` raw bytes occupy `4*ceil(n/3)`
characters, so an envelope around a hole map costs `4*ceil((3 + rider + hole map)/3)` characters and
the rider's own footprint is that minus `4*ceil(hole map/3)` - a step function of the rider's length
that never exceeds the cost of encoding the envelope on its own.

Two consequences follow, and both are measured over the whole rider domain and over a corpus of
hole-map shapes and densities against every encoding PC ships - `OffsetRiderOverheadTest` is the
reproduce command, and the numbers are deliberately left there rather than copied here. **The point
at which back pressure engages does not move at all** when a rider is configured: it is the same
incompletes count, for every shape and every encoding, because the threshold never sees the rider.
**The point at which the cap engages moves earlier by exactly the rider's Base64 footprint and no
further** - at the shifted point the bare hole map is already within that footprint of the cap.
Below it the ladder sheds the rider, then the drop marker, then the envelope, and only then does the
pre-existing strip touch the hole map, so the eviction this paragraph feared cannot happen. The
table is Base64-only and says so: astubbs/parallel-consumer#306's Z85 outer codec changes the closed
form for payloads from 22 bytes up, and the test gains a column there.

### What shipped, 2026-09-06

The rider is no longer a direction. The core half was built against
`docs/plans/2026-09-05-001-feat-offset-metadata-rider-plan.md` (astubbs#255), which inherits KTD-S7
without reopening it; the Streams consumer of the slot is a later rung and is not part of it.

- **The envelope.** Magic byte `X`, an unsigned 16-bit length, the rider bytes, then today's hole
  encoding unchanged - or nothing at all inside it, for a caught-up partition. Three header bytes in
  total. `parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/OffsetRiderEnvelope.java`
  (anchor `MAGIC_BYTE`) owns the format; the byte is registered as an `OffsetEncoding` constant
  (anchor `RiderEnvelope`) so nothing later can claim it. The envelope never nests, its length is
  validated against the bytes remaining before anything is allocated, and a payload written with no
  rider is byte-for-byte what this build produced before the slot existed.
- **The budget rule, which is the paragraph above answered.** The rider is charged against the
  metadata cap and **never** against the back-pressure threshold - back-pressure exists so a payload
  can shrink as work completes, and rider bytes do not shrink, so charging them there makes a rider a
  floor the mechanism cannot relieve and, on a caught-up partition, a permanent block. When the
  assembled payload will not fit, a ladder sheds the rider, then the three-byte dropped marker, then
  the envelope altogether, and only below all three does the pre-existing strip touch the hole map
  (`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/RiderBudgetRung.java`). So
  configuring a rider can never cost a partition metadata it would otherwise have committed - the
  hazard this write-up flagged. What it costs instead is measured over the whole rider domain rather
  than argued: the 2026-09-07 note under "Choosing the owner" states the shape and names the test.
- **The write seam.** `ParallelConsumerOptions.riderSupplier` (anchor `riderSupplier`), a function
  from a context naming the partition, the offset the commit is paired with and the byte budget
  available. It is user code on an engine thread, so it is guarded the way the retry-delay provider
  is: a throw, a `null`, or a rider over its cap is caught, logged once through a rate limiter, and
  treated as no rider for that call. Failing the commit instead would turn a persistent supplier
  fault into a partition that never commits.
- **The read seam.** The rider comes back through the codec's single decode choke point, which takes
  the unreadable-metadata policy explicitly - there is no overload without one - and reports the
  rider as one of four states: never configured, dropped for size, present, or unreadable because
  the policy discarded the payload. A zero-length array is not a representable rider, precisely so
  an embedder's decoder cannot read "dropped" as a real value.
- **The compatibility rule, and it is more than a javadoc.** A rider must not be configured until
  **every member of the consumer group** already runs a Parallel Consumer carrying the
  unreadable-metadata policy. Every previously released build throws from inside the rebalance
  callback when it meets a magic byte it does not know, before any policy gets a say; the metadata is
  durable in `__consumer_offsets`; so one such member, or a rollback to one, crash-loops on every
  restart and rebalance. **Unsetting the option does not heal it** - a partition with nothing
  outstanding never commits, so the offending metadata is never overwritten. The recovery is
  external and preserves the committed position: stop the members, then
  `kafka-consumer-groups --reset-offsets --to-current` against the group. What it costs is the hole
  map, so records that completed beyond the frontier are replayed. The option's javadoc, a one-time
  `INFO` line at configuration, and `docs/features/offset-metadata-rider.yaml` all carry it.

One consequence for anything that reads the metadata field from outside PC: a caught-up partition can
now carry an envelope where it carried nothing, so tooling that reads "metadata present" as "PC has
holes in flight" is wrong on the steady-state path.

## Related

- `git show origin/feats/ks-on-pc-spike:docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md`,
  anchor `KTD-S7` - the decision, its accepted consequences, and the rider direction. Lives only on
  that branch.
- `docs/plans/2026-09-05-001-feat-offset-metadata-rider-plan.md` - the core half as built: the
  requirements, the key technical decisions behind the format and the budget, and the open release
  question about when the write side ships.
- `git show origin/feats/ks-streams-stream-time-lowwater:docs/solutions/architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md` -
  how the `StreamTask` change that carries this decision is delivered. Also branch-only.
- `CONCEPTS.md`, anchors `**Commit frontier**` and `**Rider**` - the encoding this field owns, and
  the vocabulary for what rides beside it.
- `docs/inflight/pr-strategy-doc-merge-triggers.md`, anchor
  `**Watch the commit-metadata field as these progress.**` - why the shape of this answer is
  strategic, and the condition under which it would count as a real limit.
- `docs/inflight/pr-207-offset-encoding-policy.md` - the policy this pattern's compatibility rule
  depends on, and why its default is `IGNORE`.
- astubbs/parallel-consumer#271, issue astubbs#255.
<!-- file-refs: N/A - two entries above are read through git show and live only on their branches; the gate's revision grammar does not recognise a branch name containing a slash -->
