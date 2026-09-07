# The metrics counter maps in `WorkManager` and `PartitionStateManager` are plain `HashMap`s

<!-- inflight-type: bug -->
<!-- inflight-labels: concurrency -->
<!-- inflight-impact: crash -->

`slowWorkCounters` in `PartitionStateManager`, and `succeededRecordsCounters` /
`failedRecordsCounters` in `WorkManager`, are plain `HashMap`s registered and deregistered from the
rebalance callbacks. Same defect class as `PCMetrics.registeredMeters` - an unguarded collection on a
metrics path - and **not** covered by the fix for it.

**A fourth, found by sweeping the class rather than by a report:**
`OffsetMapCodecManager.encodingCounters`, also a plain `HashMap<OffsetEncoding, Counter>`. It is on a
different path from the other three - the encode path rather than the rebalance callbacks - and that
is what makes it worth naming separately: the commit path is the one this repo has already
demonstrated runs on two threads, which is how `registeredMeters` was reproduced tearing. Nobody has
reproduced this one; it is listed because the sweep found it, not because it has evidence.

The command that finds all four, so this list does not have to be trusted:

    grep -rnE 'private (final )?(List|Map|Set|HashMap|ArrayList|HashSet)<' \
      parallel-consumer-core/src/main/java --include=*.java | grep -iE 'meter|metric|counter|gauge'

<!-- post-merge: checked-begin -->
**Why this note exists separately.** astubbs/parallel-consumer#57 made `registeredMeters` a
`LinkedHashSet` and put every mutation of it behind a private `metersLock` monitor. That closes the
`PCMetrics` field and nothing else: `gh pr view 57 -R astubbs/parallel-consumer --json files` shows
`WorkManager.java` untouched, and its `PartitionStateManager.java` edit caches the
`OffsetMapCodecManager` rather than making `slowWorkCounters` concurrent. These three fields need
their own pass.
<!-- post-merge: checked-end -->

## The evidence, and its limit

Two concurrent rebalances - `onPartitionsRevoked` then `onPartitionsAssigned`, both threads - throw
`ArrayIndexOutOfBoundsException` out of the counter maps those callbacks register and deregister.
The report carried **no stack**, so which collection tore is not established; the class of defect is.

**That scenario is not reachable in production, and is not the argument for fixing this.**
`ConsumerRebalanceListener` callbacks are invoked by the consumer from inside one `poll`, so two
rebalances never overlap. It is recorded because it is an independent demonstration that the metrics
collections on these paths are unguarded - and the *other* sighting from the same harness, two
concurrent `createOffsetAndMetadata` calls tearing `registeredMeters`, is on a path that genuinely
does run on two threads.

Found by the Lincheck proof of concept (`docs/plans/2026-08-25-001-test-lincheck-poc-plan.md`), from
a harness aimed at commit-path torn reads. Neither sighting is one of the four that hunt had already
named, which is the PoC's evidence that it finds things nobody pointed it at. Adjacent but distinct
from the torn-read dossier's "unsynchronized cross-thread `HashMap`s" straggler - that entry is these
same three fields, and this note is where the reproduction for them now lives.

## Delete when

The three fields are made concurrent, or guarded, and the change carries a test that goes red
against the current code. A fix with no red control proves nothing here: the production path does
not overlap the callbacks, so the fix cannot be validated by the scenario that found the defect.
