# astubbs#202 - `LongPollingMockConsumer` moved into the main artefact

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- post-merge: exempt-file - this is astubbs#202's own PR note; the merge deletes it. -->


Only what a reviewer or the next session cannot get from `gh pr view 202 -R astubbs/parallel-consumer`
or the diff. The durable reasoning lives in [`docs/refactoring.md`](../refactoring.md) - the rehome
entry in the breaking-change queue, and the `NN_NAKED_NOTIFY` entry - so this file states the
decisions and points at them rather than restating them. It goes away when astubbs#202 does.

The fork<->upstream mapping is **not** recorded here or in `upstream-map.yaml`: astubbs#159 *is* the
`upstream-mirror` issue for confluentinc#526, and the manifest maps upstream **PRs** only
([`docs/upstream.md`](../upstream.md) owns that split).

## Decisions taken, so they are not re-litigated

- **The package moves with the class and nothing else** - `src/test/java` to `src/main/java` on the
  same `bz.stub.parallelconsumer.internal.utils` path. What survives from the original reasoning is
  the part that made the move safe: that package **already ships in the main jar** (`Range`,
  `JavaUtils`, `TimeUtils` and others), so nothing internal-only is promoted to public here.
  What does *not* survive is the argument that keeping the fully-qualified name makes the downstream
  migration a pom edit with no code change - the fork's `io.confluent.*` -> `bz.stub.*` rename has
  already broken every downstream import, so that saving no longer exists to be protected. The
  rehome out of `internal.utils` is therefore a live question for 0.6.0.0 rather than a deferral;
  `docs/refactoring.md`'s breaking-change queue carries it, and the reasoning for doing it in the
  same release as the rename.
- **The `NN_NAKED_NOTIFY` SpotBugs finding is a false positive, and astubbs#202 does not surface it.**
  It is already reported on master with the class under `src/test`, because the root pom turns
  `includeTests` on. Deliberately not fixed in astubbs#202, so it stays reviewable as a pure relocation.
  Full entry, including the actual fix and the measurement, in `docs/refactoring.md`.

## This does NOT eliminate the test-jar dependency

confluentinc#162 / confluentinc#861 are cited on astubbs#159 as consequences of the test-jar, and
this move is a prerequisite, **not a fix**. Every module that consumes core's `tests` classifier
still needs it for `KafkaTestUtils`, `AbstractParallelEoSStreamProcessorTestBase` and
`LongPollingMockConsumerSubject`. That last one is the binding constraint: it is Truth-generator glue
extending `com.google.common.truth.Subject`, so moving it would promote `com.google.truth` from
`test` to `compile` for every downstream user - exactly the cost this change was careful not to
incur, so it was rejected. The README's "`mvn compile` fails, use `mvn test-compile`" note therefore
still stands and was left alone.

If someone wants to actually close confluentinc#162 / confluentinc#861, the remaining question is
`KafkaTestUtils` - it is the next-largest reason the classifier is still there, and unlike the Truth
subject it has no test-scope-only dependencies.
