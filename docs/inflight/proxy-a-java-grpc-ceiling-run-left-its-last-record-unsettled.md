# A `java-grpc` ceiling run left its last record unsettled, once

<!-- inflight-type: bug -->
<!-- inflight-impact: reliability -->

Seen while merging the Wagon A stack into astubbs/parallel-consumer#293, on the first full-reactor
run in which the `java-grpc` conformance cell had a live engine behind it - which is to say, the
first run in which this cell has ever executed at all.

**The cell**: `ConformanceSuiteTest.conforms[15]`, binding `java-grpc`, scenario
`the-in-flight-ceiling-bounds-unresolved-records`, behaviour `hold-until-ceiling-full`, six seeded
records over six distinct keys, `maxConcurrency` 2.

**What failed** is the suite's own end-of-run invariant, not the ceiling assertion:

```
every delivery was settled: an unresolved record at the end would make the peak above
a measurement of a run that never finished
  expected : [0, 1, 2, 3, 4, 5]
  but was  : [5, 4, 3, 2, 1]
```

The transcript shows the pairs going out and coming back cleanly until the last one: `a`(offset 0)
and `b`(offset 1) are both dispatched, `b` settles, and `a` never does. The runner still exited 0.

## Why this is worth a note rather than a shrug

- **The two bindings that do not cross a wire were green in the same run.** `core` (the control arm)
  and `java-direct` both passed all five scenarios. Only the binding that goes over a real gRPC
  stream to a real `ProxyProcessor` failed, and only on the scenario that deliberately holds records
  to fill the ceiling - so the suspect region is the engine's settle path under a full ceiling, not
  the scenario.
- **It is the last record of the run.** That shape - everything settles except the final one - is
  what an end-of-run race looks like: a report in flight when the session is torn down, or a settle
  recorded after the observation window closed.

## Observed rate, and the conditions

| Arm | Result |
|---|---|
| Full default reactor `test`, this merge, whole suite in one run | red **1 of 2** - the second run of the identical command was green, whole reactor |
| `ConformanceSuiteTest` alone (`-pl :parallel-consumer-proxy-conformance -am -Dtest=ConformanceSuiteTest`) | green, 3 of 3 |

So it is **intermittent and load-sensitive**, which is why it is a note rather than a revert: one
sighting under load, and the cell is otherwise green. It is emphatically **not** a reason to weaken
the scenario - the invariant it failed is the one that stops the ceiling measurement being taken
from a run that never finished, and deleting it would make the cell report a number nobody could
trust.

## What to do with it

Reproduce under load first - run the full reactor rather than the module, since that is the only arm
that has produced it. If it reproduces, the question is which side dropped the settle: the engine's
`InFlightRegistry` at the moment the session ends, or the client's report never leaving. The
transcript is per-record and already distinguishes dispatch from settle, so a run with the engine's
own logging up is likely to answer it without new instrumentation.

Add sightings here rather than rewriting this note.
