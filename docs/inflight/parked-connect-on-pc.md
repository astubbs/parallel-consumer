# Parked: Kafka Connect sinks driven by Parallel Consumer

Issue: astubbs/parallel-consumer#240 (mirror of confluentinc/parallel-consumer#119)
Branch: `feats/connect-on-pc-spike`
Parked: 2026-08-08, resuming the week of 2026-08-10.

## State

One commit: `docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md`, carrying a
superseded-direction header. No code. Nothing pushed.

## Direction

**Rejected:** a new `parallel-consumer-connect` module on `connect-api` that reimplements a reduced
`WorkerSinkTask`. Review established two problems. Concurrency caps at the assigned partition count -
a `SinkTask` is not thread-safe and owns partitions, so `PARTITION` ordering is forced, which is the
same ceiling a Connect worker reaches at `tasks.max = <partitions>`. And every runtime feature the
module skipped (SMTs, DLQ/`errors.tolerance`, `ConfigProvider`, plugin isolation) was skipped only
because rebuilding it is expensive - which is the wrong reason.

**Adopted:** patch Connect so `WorkerSinkTask` sources records from Parallel Consumer, using the
build-time patch strategy proven in `feats/ks-on-pc-spike` - `bin/apply-patch.sh` / `bin/regen-patch.sh`,
generated classes gitignored, a shadowed-classloading test proving the patched classes win, a
stock-baseline fixture and control arm proving the harness is behaviour-neutral, and Kafka's own tests
as the regression oracle. Stack on that branch; assume it lands.

## The question the PoC exists to answer

Does key-sharding across more `SinkTask` instances than partitions preserve correctness?

`preCommit()` returns a watermark, which no task can honestly give for a partition whose records are
split across tasks. The proposed resolution: interpret each task's watermark against **that task's own
record stream** as a durability barrier, and let PC compose the sparse completion set across tasks and
encode it in commit metadata. Sparse out-of-order completion tracking is the one thing PC has that
Connect does not, and this is the first use that genuinely requires it.

Note the inversion: `ProcessingOrder.KEY` had to be *rejected* under the embed design, and becomes the
*natural* mode here. Keyed upsert sinks - JDBC upsert, Elasticsearch by document id, Mongo - are exactly
where key-level concurrency beyond partition count is worth money.

## Known constraint: not every connector tolerates losing whole-partition ownership

S3 and HDFS sinks name output files by topic-partition-offset, so two tasks each holding part of
partition 0 would collide. Same class of problem for any connector doing partition-scoped batching or
partition-keyed state.

**Follow-up to investigate (user, 2026-08-08):** because the extension owns the worker, the operating
mode does not have to be global - it can be selected **per connector, dynamically, inside one process**.
Different connectors would then impose different restrictions on PC's mode. Worth scoping: what signal
picks the mode (a registry of known connector classes, an explicit user override, or an opt-in capability
interface a connector author can implement), and what the safe default is when the connector is unknown.
Partition-affine is the obvious default - it degrades to the ceiling above, which is still
Connect-without-a-worker.

## Carried forward from the shelved plan

The offset analysis transfers unchanged and is the expensive part:

- The committed offset and its encoded incomplete-offset payload are both anchored to
  `getOffsetHighestSequentialSucceeded() + 1`, and the read side decodes against
  `OffsetAndMetadata.offset()`. Lowering one without re-encoding the other shifts the whole decoded set.
- `PartitionState.onOffsetCommitSuccess()` calls `setClean()`, and the commit gate is
  `isTimeToCommitNow() && wm.isDirty() && !isRebalanceInProgress`. A clamped partition that goes clean is
  never committed again.
- `preCommit()` runs on `pc-broker-poll`, not the controller thread, and on the revoke path inside
  `synchronized (commitCommand)` - the monitor of the open deadlock in astubbs/parallel-consumer#29.
- Connect's `preCommit()` contract: lower offsets honoured, higher rejected, an omitted partition means
  leave it where it was, an empty map means skip the commit entirely.

## Unrelated defect found while reviewing

`AGENTS.md` said `**/*IT.java` is included in failsafe. The root pom's failsafe `<includes>` lists only
`**/integrationTest*/**/*.java`, so a `*IT.java` outside an `integrationTest` package runs in neither
suite and reports nothing. `TestConventionRules` has it right. Not yet fixed.
