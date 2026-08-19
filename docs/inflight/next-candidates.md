# Next candidates, ranked

Collisions are in `pr-blockers-and-collisions.md`. The ranked backlog and full verdicts live in
`src/docs/development/upstream-pr-analysis.adoc`; these are the ready picks:

- **`confluentinc#912` vertx leak** - branch done, needs rebase + PR (`branch-912-vertx-leak.md`). Best
  immediate pick.
- **Auto-scaling (astubbs#227)** - runtime-discovered per-instance concurrency; candidate killer
  feature alongside key ordering, priority raised 2026-08-18 (`next-auto-scaling.md`). Spec
  stage; two bitrotted prototypes to mine; async-timing metrics fix is the prerequisite.
- **Logging-verbosity cleanup** - batch `confluentinc#629` / `#631` / `#640` into one PR
  (`ConsumerOffsetCommitter`, `RemovedPartitionState`, `AbstractParallelEoSStreamProcessor`). Low
  effort, high return.
- **Contributor-friction build fixes** - `confluentinc#162` (mvn compile without test-jar),
  `confluentinc#861` (`ManagedTruth` not found), `confluentinc#906` (pom version mismatch).
- **Security dependency bumps** - `confluentinc#851` (postgres), `confluentinc#913` (assertj); pom-only.
- **[#40](https://github.com/astubbs/parallel-consumer/issues/40)** - dedup the `MockConsumer*` test
  classes (test-only; the duplication bot keeps flagging them).
- **`confluentinc#915` batch construction strategy** - cherry-pick, closes the 4-year-old
  `confluentinc#266`. Medium effort.
- **Point ArchUnit at main code** (`next-archunit-main-code-rules.md`) - the harness is already
  wired into all four modules with a shared rule library, but polices only three test conventions.
  Post-v6: it is what would hold the boundaries the God-class decomposition creates.
- **DLQ** (`confluentinc#310`, or revive `confluentinc#366`) - the most-demanded missing feature. Large, and
  spec-stage only.

## Compounding ideas from the confluentinc#857 chaos measurement work, 2026-08-19

Each of these came out of an instrument being wrong rather than the product being wrong, which is
why they generalise past this investigation.

- **Truth probes for internal state, made routine** (`next-truth-probes-for-internal-state.md` owns
  this) - the chaos suite judged PC from outside, via committed offsets read by an admin client,
  while `WorkManager` and `ShardManager` expose the real answer publicly
  (`getNumberOfWorkQueuedInShardsAwaitingSelection`, `isRecordsAwaitingProcessing`,
  `isNoRecordsOutForProcessing`, `getNumberOfIncompleteOffsets`, and `pc.getWm()` is public). A test
  that infers internal state from an external signal will eventually infer it wrongly. Where a
  component knows the answer, ask it.
- **Measure both ends of anything you count.** A completion counter alone cannot distinguish
  "nothing is finishing" from "nothing is happening"; a fleet inside a 20s user function reads as a
  flat line while fully busy. Counting entry as well as exit made in-flight work visible and turned
  an apparent stall into an obvious back-pressure pause. Any counter used to judge liveness needs
  its partner.
- **Assert the property, report the timing.** A correctness suite gating on a duration turns every
  slow-but-correct run into a failure and every threshold into an argument
  (`test-class2-probe-asserts-timing-not-correctness.md`). Gate on completion, loss, duplicates and
  ordering; publish recovery time and peaks as measurements.
- **Granularity is part of a liveness check's correctness.** The existing `NO_PROGRESS` probe has
  the right SHAPE - while work remains, completions must advance - but is fleet-wide, so one wedged
  shard hides behind seventy-nine healthy ones. A check at the wrong granularity is not a weak check,
  it is a check for a different property.
- **A scale knob turns a stress test into an experiment.** `-Dperf.scale` on the capacity profiles
  exists because a measurement welded to one size can only answer the question that size happens to
  ask. The same applies to any workload constant that was chosen for one machine.
- **The harness cannot model a crash** (`parked-chaos-crash-fidelity-variant.md`) - every stop is an
  orderly close, so the most-reported confluentinc#857 shape is the one no scenario produces.
- **Run-mode experiments belong in the demo app** (`branch-polyglot-demo-ideation.md`) - the
  assignor x stop-mode matrix is a user-facing result, and the harness that produced it is a
  ready-made engine for the bring-your-own-topic direction.
