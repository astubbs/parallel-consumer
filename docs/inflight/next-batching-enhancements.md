# Batching enhancements - ideation done, direction decision pending

> Extracted from `origin/docs/ideate-batching-enhancements` @98a5ea8f5, `docs/inflight/next-batching-enhancements.md`.

Full ranked ideation with verified evidence:
[`docs/ideation/2026-08-17-batching-enhancements-ideation.html`](../ideation/2026-08-17-batching-enhancements-ideation.html)
(on `docs/ideate-batching-enhancements` until merged). 46 raw ideas cut to 7 by a fresh-context
verifier that spot-checked every citation against the source.

**The pending decision (idea 1): the composition-API shape, before confluentinc#915 merges.** Five
candidate shapes for the same one-way door - single integer `maxRecordsPerShardPerBatch`, the PR's
enum as-is, a declarative batch spec, a compatibility predicate, a shard-yield x batch-grouping
split. Everything else queued behind batching (confluentinc#266, confluentinc#560) lands as config,
plugin, or core PR depending on this choice.

**Shippable now, independent of any decision:** `calculateQuantityToRequest` rounds up with
`target - modulo` where filling the tail batch needs `batchSize - modulo`
(`AbstractParallelEoSStreamProcessor.java:1093`) - roughly a 2x work over-request whenever the
deficit is not a batch multiple. Verified with worked numbers in the doc.

**Blockers found for confluentinc#915 (not yet raised on the PR):**

- `PollContext` hands batches over as `HashMap`/`HashSet` - no offset order, so `BATCH_BY_SHARD`
  would break KEY ordering inside a batch via a hash bucket. Prerequisite fix, cheap now, breaking
  later.
- No batch telemetry exists (all metrics per-record), so a composition change cannot be judged in
  production; and `validate()` accepts `batchSize=0`, which silently zeroes the in-flight target - a
  config typo yields a healthy-looking consumer that processes nothing.

Other verified leads in the doc: per-record batch results were started and abandoned
(`intermediateResults` built, commented, returned empty) - so the partial-batch-failure ask
(confluentinc#887) is "finish a seam", not deep surgery; and the answer to the question that stalled
confluentinc#560 since 2022 is encoded in the project's own test comment ("partition ordering
restricts the batch sizes to a single element").
