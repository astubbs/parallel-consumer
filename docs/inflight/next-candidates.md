# Next candidates, ranked

<!-- inflight-type: feature -->


Collisions are in `pr-blockers-and-collisions.md`. The ranked backlog and full verdicts live in
`src/docs/development/upstream-pr-analysis.adoc`; these are the ready picks:

- **Commit-failure seam ([astubbs#317](https://github.com/astubbs/parallel-consumer/issues/317))** -
  **highest-demand item on this list, and the only one with a user shipping a patched build to get
  it.** On confluentinc#833 `ndqvinh2109` reported patching `controlLoop` with a try/catch so the
  exception would not reach `supervisorLoop` and close PC. That is not a feature request in a
  backlog - it is someone maintaining a private fork of the library because the decision PC makes
  for them is the wrong one for their deployment. Kafka's client throws a retriable exception and
  lets the caller choose; PC only terminates. Research, both sides of the upstream argument, and why
  fixing astubbs#177 does not close it: `next-commit-failure-seam.md`.
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
