# Which Kafka Connect sink connectors work on Parallel Consumer?

**Nothing in this table has been tested yet.** Every entry below is a *prediction* derived from how the
connector is known to behave, recorded so the question is not forgotten and so the first real test runs
have something to falsify. Predictions are not evidence, and a wrong prediction here is a useful result.

Revisit before merge. The intent is that entries move from Predicted to Verified as
`parallel-consumer-connect` grows a live delivery path, and that **Verified means the connector's
own published test suite ran green against the patched Connect runtime** - see "The bar for Verified".

## Why compatibility is not uniform

Connect assigns whole topic partitions to a task. A sink may therefore rely on owning a partition
outright: buffering per partition, naming output files by partition and offset, or keeping
partition-keyed state. Parallel Consumer's value here is running more task lanes than partitions by
sharding on key - which takes whole-partition ownership away.

So the axis is not "good connector / bad connector". It is **what the connector assumes about partition
ownership**, and it produces three groups:

- **Key-affine sinks** - the write is idempotent per key (an upsert, a document id, a primary key).
  Splitting a partition across lanes is safe, because each key still lands in one lane in order. These
  are the connectors the whole idea is aimed at.
- **Partition-affine sinks** - output identity or state is derived from the partition. Splitting a
  partition across lanes collides. These can still run, but only in partition-affine mode, where the
  concurrency ceiling is the partition count - the same ceiling Connect already has.
- **Unknown** - no reason yet to place them.

Partition-affine is the safe default for any connector not positively known to be key-affine.

**But note what that default costs.** Per `STRATEGY.md`, the reason to run a connector on Parallel
Consumer at all is key-level concurrency above the partition count - the Streams work measured 57x for a
record that would otherwise be stuck behind a slow one, 8x for the typical one. A partition-affine
connector gets none of that: its ceiling is the partition count, which is what `tasks.max` already
delivers. So this table is not a compatibility footnote. **It is the list of connectors the strategy's
claim actually applies to**, and a connector landing in the partition-affine column is one this work
cannot help, however correctly it runs.

## Predicted compatibility - UNTESTED

| Connector | Predicted mode | Basis for the prediction | Status |
|---|---|---|---|
| `FileStreamSinkConnector` (Kafka) | Partition-affine | Appends to one stream; ordering across the whole assignment is observable in the output file. Useful as a first end-to-end fixture precisely because it does *not* override `preCommit`, so it exercises the default flush path | **Predicted, untested** |
| JDBC sink (upsert mode, PK from key) | Key-affine | Writes are per-key upserts; two lanes touching different keys do not interact | **Predicted, untested** |
| JDBC sink (insert mode) | Partition-affine | Plain inserts are not idempotent, and batching is per `put()` | **Predicted, untested** |
| Elasticsearch sink (document id from key) | Key-affine | Indexing by document id is idempotent per key | **Predicted, untested** |
| MongoDB sink (upsert by `_id`) | Key-affine | Same shape as the JDBC upsert case | **Predicted, untested** |
| S3 sink | Partition-affine | Output objects are named by topic-partition-offset; two lanes holding one partition would write colliding objects | **Predicted, untested** |
| HDFS sink | Partition-affine | Same partition-named-output problem as S3, plus a write-ahead log keyed by partition | **Predicted, untested** |
| Iceberg / Delta sinks | Unknown | Commit coordination and file naming need reading before guessing | **Unknown** |
| BigQuery sink | Unknown | Streaming-insert idempotency depends on configuration | **Unknown** |
| Debezium *sinks* (JDBC) | Key-affine | Same as JDBC upsert; note Debezium is mostly *source* connectors, which are out of scope entirely | **Predicted, untested** |

Connectors relying on any of the deliberately unsupported features - SMTs, `errors.tolerance` / DLQ,
`ConfigProvider` secret resolution - are out of scope regardless of which group they fall into, because
the module rejects those configs rather than silently ignoring them.

## The bar for Verified

A row moves to Verified when **that connector's own published test suite passes against the patched
Connect runtime**, not when we write a test we like the look of. This is the standard the Kafka Streams
work already set - Kafka's own 188 tests run against its patched classes - and it is the only evidence an
adopter should accept. We would not adopt someone else's patched runtime on their say-so either.

Practically that means, per connector: resolve its test artifacts, run its suite against the fork, and
record the result including which tests do not apply and why. Expect that to be where most of the real
work and most of the real findings are.

## What would make an entry move

- **To Verified (key-affine):** the connector's suite passes, and a multi-lane run over one partition
  produces the same final sink state as stock Connect.
- **To Verified (partition-affine):** the connector's suite passes with lanes bound one-per-partition.
- **To Incompatible:** a concrete mechanism, named. "It felt risky" is not a result.
