# Next candidates, ranked

Collisions are in `pr-blockers-and-collisions.md`. The ranked backlog and full verdicts live in
`src/docs/development/upstream-pr-analysis.adoc`; these are the ready picks:

- **`upstream #912` vertx leak** - branch done, needs rebase + PR (`branch-912-vertx-leak.md`). Best
  immediate pick.
- **Logging-verbosity cleanup** - batch `upstream #629` / `#631` / `#640` into one PR
  (`ConsumerOffsetCommitter`, `RemovedPartitionState`, `AbstractParallelEoSStreamProcessor`). Low
  effort, high return.
- **Contributor-friction build fixes** - `upstream #162` (mvn compile without test-jar),
  `upstream #861` (`ManagedTruth` not found), `upstream #906` (pom version mismatch).
- **Security dependency bumps** - `upstream #851` (postgres), `upstream #913` (assertj); pom-only.
- **[#40](https://github.com/astubbs/parallel-consumer/issues/40)** - dedup the `MockConsumer*` test
  classes (test-only; the duplication bot keeps flagging them).
- **`upstream #915` batch construction strategy** - cherry-pick, closes the 4-year-old
  `upstream #266`. Medium effort.
- **DLQ** (`upstream #310`, or revive `upstream #366`) - the most-demanded missing feature. Large, and
  spec-stage only.
