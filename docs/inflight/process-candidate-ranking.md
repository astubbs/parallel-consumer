# Next candidates, ranked

<!-- inflight-type: register -->


## Decisions waiting on the maintainer, ranked

**These outrank everything below, and the reason is cost rather than importance.** Each is a note
whose engineering is already done or already unnecessary - what is left is one reply, and until it
arrives the note cannot close and the work behind it cannot be scheduled. Ranked by how little input
each needs, not by the size of what it unblocks: an item needing a yes/no beats one needing a policy,
and a policy beats a bound that has to be argued for.

They came out of the 2026-08-20 mirror triage, which produced one note per issue - each carrying the
verification, the draft answer, and the collision list. **Do not re-derive any of it here**; the note
named on each line owns it.

1. **astubbs#161** (confluentinc#543), `upstream-161-reactor-scheduler-rationale.md` - the reply is
   written and postable as-is, so the only decision is to post it and close. Two code findings ride
   along that nothing else records: the scheduler supplier is resolved per wrapped invocation rather
   than once, so a factory-shaped supplier leaks a `Scheduler` and its threads every batch, and the
   two-argument `ReactorProcessor` constructor has no test.
2. **astubbs#181** (confluentinc#862), `deps-181-java-24-compatibility.md` - close it on the
   kafka-clients 3.9.2 rationale and let astubbs#128 carry the CI proof, or hold it open until that
   lane exists. The note has the evidence and states the one caveat (`MockConsumer` cannot exercise
   SASL, which is the path that broke).
3. **astubbs#163** (confluentinc#550), `core-163-poll-path-has-no-error-seam.md` - post the drafted
   answer, then close as a duplicate of astubbs#153 with astubbs#148 as the contained step. **This
   one has a deadline the others do not**: it corrects open question 6 of the DLQ prior-art report on
   astubbs#313, which currently assumes deserialization failures can ride along with the DLQ work.
   They cannot, on the mechanism. Deciding DLQ requirements before this is answered settles them on a
   false premise.
4. **astubbs#189** (confluentinc#887), `core-189-batch-failure-granularity.md` - go/no-go on jitter in
   the default retry delay. A small change, but it moves retry timing for every existing deployment,
   which is why it is a call rather than a commit. Nothing else in the poison-isolation ladder waits
   on the answer.
5. **astubbs#162** (confluentinc#546), `bug-162-offset-state-truncation.md` - should absent commit
   data warn at all? It is the normal state of a new group or an expired offset, so the honest
   handling is a quieter distinct message and no truncation branch - against which operators alert on
   the current line.
6. **astubbs#241** (confluentinc#144), `core-241-tx-commit-failure-taxonomy.md` - agree the issue's
   stated premise died in confluentinc#355, then keep it open with a rewritten `## Fork status` and
   relabel `bug` to `feature`. No defect is demonstrated; what survives is a policy design.
7. **astubbs#173** (confluentinc#777), `upstream-173-revocation-duplicate-processing.md` - should PC
   offer a revocation grace period at all? Upstream declined it. **If the answer is no, confluentinc#777
   is a documentation obligation rather than a defect** and the close is unblocked - at the cost of a
   README section and one chaos cell that must be run rather than predicted.
8. **astubbs#178** (confluentinc#843), `core-178-key-order-across-a-rebalance.md` - is an undrained
   old-epoch delivery a violation of the README's "strong ordering by key", or legitimate
   at-least-once? Last because it is the only one that needs a *bound* argued for rather than a
   yes/no, and `KeyOrderLedger`'s javadoc already says picking that number is the whole job.

**What is NOT on this list, from the same triage, and why:** astubbs#139 is a 1.0 blocker with a
four-step definition of done in `core-139-public-api-thread-safety-contract.md` - real work, not a
call. astubbs#175 has no decision left in it; its one live strand is the AB-BA wedge owned by
astubbs#29. astubbs#155, astubbs#169 and astubbs#170 have their fixes written and correctly linked -
what they need is the three draft PRs de-conflicted and merged, which is scheduling.

## Ready picks

Collisions are in `pr-blockers-and-collisions.md`. The ranked backlog and full verdicts live in
`src/docs/development/upstream-pr-analysis.adoc`; these are the ready picks:

- **Commit-failure seam ([astubbs#317](https://github.com/astubbs/parallel-consumer/issues/317))** -
  **highest-demand item on this list, and the only one with a user shipping a patched build to get
  it.** On confluentinc#833 `ndqvinh2109` reported patching `controlLoop` with a try/catch so the
  exception would not reach `supervisorLoop` and close PC. That is not a feature request in a
  backlog - it is someone maintaining a private fork of the library because the decision PC makes
  for them is the wrong one for their deployment. Kafka's client throws a retriable exception and
  lets the caller choose; PC only terminates. Research, both sides of the upstream argument, and why
  fixing astubbs#177 does not close it: `core-commit-failure-seam.md`.
- **`confluentinc#912` vertx leak** - branch done, needs rebase + PR (`branch-912-vertx-leak.md`). Best
  immediate pick.
- **Auto-scaling (astubbs#227)** - runtime-discovered per-instance concurrency; candidate killer
  feature alongside key ordering, priority raised 2026-08-18 (`core-auto-scaling.md`). Spec
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
- **Point ArchUnit at main code** (`static-archunit-main-code-rules.md`) - the harness is already
  wired into all four modules with a shared rule library, but polices only three test conventions.
  Post-v6: it is what would hold the boundaries the God-class decomposition creates.
- **DLQ** (`confluentinc#310`, or revive `confluentinc#366`) - the most-demanded missing feature. Large, and
  spec-stage only.
