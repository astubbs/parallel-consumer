# Decompose `AbstractParallelEoSStreamProcessor`: the seams, the old attempts and their ideas, and what to merge first

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->

Recorded 2026-09-08, from a session that read every branch that ever attempted this, the upstream
drafts they were proposed as, every open PR's own changes to the class, and ranked what has to land
before a cut is safe. **This note owns the decision; [`docs/refactoring.md`](../refactoring.md)
keeps the heading "Decompose the God class" as a pointer here**, because other notes and the manifest
cite it by that name. The fork-to-upstream registration is the manifest's
`refactor-thread-model-god-class` entry, which stays the source of truth for which branch maps to
which upstream PR.

The five classes below it in size have their own note,
[`core-shrink-the-five-next-largest-classes.md`](core-shrink-the-five-next-largest-classes.md); the
two are separate because their blockers are different PRs.

## What is in the class, and why the size is not written here

The class holds the control loop, the lifecycle state machine, the rebalance callbacks, commit
orchestration, worker-pool construction and its rejection guard, the user-function runner, and the
mailbox the workers post into. Read the method list rather than a description of it:

```bash
grep -nE '^\s{4}(public|protected|private)[^=;]*\(' \
  parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java
```

**Size is deliberately not written down.** It was recorded as a line count once, and was still being
cited as that after the class had grown by most of a thousand lines - a stale figure reads as current
state forever, and nothing goes red. Ask `wc -l` on the path above instead.

**One argument built on that stale figure is falsified, and the falsification is the reason to act.**
`docs/ideation/2026-08-17-actor-collection-revival-ideation.html` reasons from "the file has moved one
line in 3.5 years, which is the clearest evidence that branch-shaped goals don't move it", and
proposes tracking progress by the line count. Measured 2026-09-03: the class was the size that
ideation quotes on the ideation's own date and was over a thousand lines larger a fortnight later.
The dated record is left as written, per [`docs/citations.md`](../citations.md); the correction lives
here, where the live decision is. It does not weaken the case for decomposing - it inverts the reason.
**The class is not inert, it is accreting**, and every engine change of the last month landed inside
it: the revoke-commit request protocol, the worker-pool shutdown guards, task accounting, direct pull
as a bolt-on pool.

**Landing this unblocks whole-file static analysis, one file at a time.** The new-code analysis
profile is scoped to changed *lines* rather than changed *files* purely because of size: touching a
class this large would otherwise inherit every latent finding in it. Line scoping misses a finding
reported away from the edit that caused it. Each file that comes down to a reviewable size can be
promoted to file scoping on its own; [`static-analysis-rule-profiles.md`](static-analysis-rule-profiles.md)
owns the profiles and carries the promotion list, which is empty until the first cut lands.
[`static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md) holds a detector gated on the
same event.

## The seams, from the only split that ever compiled

The 2022 attempt is one tree from one base, catalogued under `branch_accounting` in
`src/docs/development/upstream-map.yaml` (`bin/inflight.mjs branch <ref>` answers for any of them):
`origin/refactor/function-runner` at the root, `origin/refactor/extract-controller` and
`origin/refactor/state-machine` as siblings on it, and `origin/refactor/control-loop` as the
integration branch that merged both and then did the real work - it compiles, with tests migrated and
a review pass. **It was proposed upstream as confluentinc#488**, whose body is the whole design in four
lines: *function runner, worker pool, worker; controller, rebalance handler, configuration validator,
subscription interface; state machine; control loop.* **No ref since 2023 has added any of its classes
to main code**, so it has no successor. Reading it as a branch to merge is wrong: it predates the
package rename, every engine change since, and the test suite as it now exists. Read it as a map.

| Seam | The 2022 class | Where it is on master today | What moved since, and the verdict |
|---|---|---|---|
| **Lifecycle** | `StateMachine` on `origin/refactor/state-machine` - the `State` field, the failure reason, close, drain, the draining and closing transitions, `maybeCloseConsumer` | Same method names, still on the God class: `transitionToDraining`, `transitionToClosing`, `doClose`, `innerDoClose`, `areMyThreadsDone`. The `State` enum is unchanged | Close now interleaves with the revoke-commit protocol (`completePendingRevokeCommitFromClose`, `failPendingRevokeCommitOnControlThreadExit`) and the pool-shutdown guards. **Still the cleanest single cut, and the one whose method set still maps one to one.** First rung |
| **Rebalance and subscription** | `RebalanceHandler` implementing `ConsumerRebalanceListener` and a `SubscriptionHandler` interface, on `origin/refactor/extract-controller`; commit-on-revoke inline. Its javadoc states the second reason for the cut: *so the user doesn't have access to the public rebalance interface methods* | `subscribe` and the three callbacks on the God class, now the fastest-growing region: revoke hands a `RevokeCommitRequest` to the control thread and waits (`commitOnRevokeViaTheControlThread`, `waitForTheTakenRequest`), with the decline and failure paths astubbs#451 added | The 2022 content is obsolete; **the seam is still right**, and it is where the request-and-wait protocol belongs. Second rung, and the one that removes the most |
| **Worker pool and runner** | `PCWorkerPool` and `FunctionRunner` on `origin/refactor/function-runner`; `ExternalEngineRunner` with `VertxRunner` and `ReactorRunner` as runner subclasses on `origin/refactor/control-loop` | `setupWorkerPool`, `requireRejectionIsVisible`, `submitWorkToPool`, `makeBatches`, `runUserFunction` and `runUserFunctionInternal` on the God class, plus the static `UserFunctions` entry point; `ExternalEngine` still subclasses the God class | astubbs#360 changes the pool's type and moves queue-depth reading into `UserFunctionTaskAccounting`; astubbs#361 adds `DirectPullWorkerPool` beside it. **Cut this after both land**, or the cut and the two PRs fight over the same methods |
| **Control loop** | `ControlLoop` on `origin/refactor/control-loop` - the pass, commit timing, `maybeAcquireCommitLock`, `maybeWakeupPoller` | `controlLoop`, `controlLoopPass`, `isTimeToCommitNow`, `maybeAcquireCommitLock`, `checkPipelinePressure` and the load-factor family on the God class | astubbs#361 removes work distribution from the pass entirely under direct pull. Cut after it |
| **Mailbox** | `WorkMailbox` wrapping the blocking queue and `ControllerEventMessage` as a nested type | `workMailBox` is an inline field; `ControllerEventMessage` is already its own class; `processWorkCompleteMailBox` and `addToMailbox` on the God class | The actor section of `docs/refactoring.md` names a first slice - stop waking the control thread by interrupting it - and says it belongs *with* this decomposition, not before it. The mailbox cut is where that slice goes |
| **Configuration validation** | `ConfigurationValidator` on `origin/refactor/control-loop` | `validateConfiguration`, `checkGroupIdConfigured`, `checkAutoCommitIsDisabled` and the reflective `getAutoCommitEnabled` on the God class | Untouched by any open PR. A free cut at any time, and the brittle consumer-by-classname check `docs/refactoring.md` lists moves with it |

The 2022 queue reworks on top of this family (`origin/refactor/worker-queues`, the two
`origin/refactor/gpt3-*` branches) are not decomposition and are superseded by measurement; their
record is branch-only, on `origin/perf/engine-concurrency` (astubbs#363):
`git show aebf9d02b:docs/inflight/parked-2022-central-queue-rework.md` for the retraction of the "1/3
as fast" verdict, and `git show aebf9d02b:docs/inflight/perf-direct-pull-measured.md` for the
rebuilt-and-measured direct-pull engine that astubbs#361 carries.

## The older upstream drafts, and the ideas worth keeping from each

Every one of these was written by the same author as upstream maintainer, so "upstream draft" and
"fork branch" name the same commits: the fork remote holds the working branches, and
`origin/upstream/*` holds the copies upstream had when it closed them unmerged in the 2023 sweep.
`src/docs/development/upstream-pr-analysis.adoc` Group C ranks them and rules *keep ideas, not code*;
the manifest's `sweep-2023-actor-ipc` and `refactor-thread-model-god-class` entries register them.
What follows is the idea in each, read from its own body, javadoc and commits - which is the part the
adoc's one-line verdicts do not carry.

- **confluentinc#488, "Refactor God class to components"** (`origin/refactor/control-loop`, 2022-12).
  The seam table above *is* confluentinc#488. Its checklist left one item nobody has since answered:
  *ordering of components in new classes* - which class owns construction order once the God class
  no longer does. Decide it at the first cut, because `PCModule` is where that order lives today and
  [`core-pcmodule-injection-seam.md`](core-pcmodule-injection-seam.md) records that its registration
  is already write-only.
- **confluentinc#325, "New IPC system using lightweight Lambda actor queue"**
  (`origin/improvements/lambda-actor-bus`, 2022-07). The bus itself is separable, the manifest
  verified: a handful of files under `io.confluent.csid.actors` whose only PC coupling is one marker
  interface. **The payload that matters here is not the bus, it is the six interfaces the branch
  puts on the God class** - `ControllerInternalAPI` ("Internal thread safe API for the Controller"),
  `BrokerPollerAPI`, `WorkManagerIPCAPI`, `Supervisable`, `MultithreadingAPI` ("all these methods
  should go through the Actor system") and `ThreadSafe`. They partition the class's surface **by who
  may call it**, which is the same partition the seam table makes by *what it does*, from the other
  side. The 2026-08-17 ideation calls this the strangler seam: interface segregation lands as a pure
  refactor, and the transport underneath becomes a swap rather than a bet. Take the interfaces with
  the cuts; leave the bus for the thread-model decision.
- **confluentinc#524, "Use Actor for commit commands"** (`origin/improvements/commit-command-actor`,
  2022-12). The commit seam as a command actor, on top of the two below. Not a decomposition step -
  it changes *which thread* commits - so it is gated on the thread-model decision. Its `CommitData`
  type, a value object for what a commit carries between threads, is the reusable idea and would be
  wanted by the rebalance cut regardless.
- **confluentinc#270, "Shared nothing architecture - Partition Events"**
  (`origin/improvements/rebalance-messages`, 2022-04, tracker confluentinc#200, mirror astubbs#142).
  Rebalance events reach the controller as messages instead of the callback thread mutating shared
  state; its body names the consequence for the poller - *BrokerPoller can't access state - when
  performing commit, must request or be sent commit offset data by state owner* (confluentinc#419) -
  and removes the separate `onPartitionsLost` path as indistinguishable from revoke. **This is the
  design the rebalance cut should leave room for**: a `RebalanceHandler` that today calls into the
  God class directly can later post a message instead, if the seam is drawn at the callback rather
  than inside it.
- **confluentinc#271, "Major package restructure"**
  (`origin/upstream/improvements/package-restructure`, 2022-04). Six commits that never got a body,
  but the tree says what it meant: `controller`, `kafkabridge` (`ConsumerRebalanceHandler`,
  `OffsetCommitter`), `sharedstate` (`PartitionEventMessage`, `CommitRequest`, `CommitResponse`),
  `offsets`, and `internal` for what is left, plus a `ControllerEventBus` and a
  `PartitionEpochTracker`. **It is the package layout for the seams above**, and the adoc's verdict -
  *blast radius too large on its own; fold into C1/C2* - still holds: move a class into its package
  as it is cut, never as a sweep.
- **confluentinc#45, "direct work loading, direct result processing"** (`origin/direct-ringbuffer`,
  2020-12) and its 2022 descendants, the `gpt3-*` branches. Superseded: astubbs#361 is the finished,
  measured form of the same idea. Nothing to take.
- **`origin/massive-refactor`** (2020-12, the first attempt, never proposed upstream). Introduced
  `WorkMailbox`, `PartitionState`, `KafkaQueueManager` and the self-tuning load factor in one
  21-commit branch; `PartitionState` and the load factor landed by other routes, the rest did not. Its
  lesson is the one `docs/refactoring.md` records for `origin/move-cons-to-pc`, three weeks earlier
  still: the earliest attempt moved the consumer back into the controller so commits were in line with
  control, and the record does not say why it stopped. **Both stopped because they were one branch.**
- **`origin/improvements/interrupt-reason`** (2022-07). An `InterruptibleThread` that carries a
  *reason* with the interrupt. It is the ancestor of the first slice in
  [`waking-a-thread-by-interrupting-it-2026-08-17.md`](../solutions/workflow-issues/waking-a-thread-by-interrupting-it-2026-08-17.md),
  which chose a message over a reason-bearing interrupt; the mailbox cut is where that lands.
- **`origin/improvements/poller-bus-actor`** (2022-07). The poller as an actor, on a *second*,
  unreconciled actor base; its own commit says the two bases must be unified first. Gated on the
  thread-model decision, and out of scope for the cuts - it belongs to `BrokerPollSystem`'s note.
- **`origin/refactor/infinite-retry`** (2022-11). Moves timeout-retry into the controller so the
  poller only forwards the error. A control-loop concern; revisit at that cut, not before.

**The ideation's other keeper, stated once so it is not re-derived:** encode the current census of
concurrency primitives in the engine as an ArchUnit rule that fails when it rises, so every cut is
graded by what it *deletes* rather than what it moves. That is the falsifiable, mechanism-neutral
target the line count was wrongly asked to be;
[`static-archunit-main-code-rules.md`](static-archunit-main-code-rules.md) is where such a rule would
live.

## Merge before you cut

Measured 2026-09-08 by diffing every open PR's tip against **its own merge base** with master, on
this class and the ten next largest, with the package rename normalised. **Measure from the merge
base, never two-dot against master's tip**: the two-dot form counts everything master added since the
branch forked as the branch's own deletions, and made every stale draft look like it rewrote the
engine. Most of those turned out to carry nothing but the package rename in their imports. The script
is at the end of this note; re-run it rather than trusting the ranking below to have stayed true.

Ordered by how much of the class each rewrites, weighted by readiness. The ones that touch this class
directly:

1. **astubbs#360, virtual threads for the user function.** Root of the engine stack. Changes what
   `setupWorkerPool` returns and pulls queue-depth accounting out into its own class - a piece of this
   decomposition already done. Its base is three weeks stale but every parent has merged.
2. **astubbs#361, direct pull with an O(1) shard scan.** The largest single removal from the control
   loop on any open branch: work selection leaves the pass. Depends only on astubbs#360 now; its other
   declared parents (astubbs#335, astubbs#336, astubbs#358, astubbs#201) are merged.
3. **astubbs#352, the commit-failure seam.** Second-largest standalone rewrite of the class, and it
   also reaches `ProducerManager`, `BrokerPollSystem` and the options. No declared parents.
4. **The astubbs#225 producer-recovery stack** - astubbs#472, astubbs#474, astubbs#410, astubbs#420,
   then the conflicting leaves astubbs#434 and astubbs#408. Fresh, linear, mergeable at the root. It
   rewrites the revoke and close paths on this class and a third of `ProducerManager`, so the
   lifecycle and rebalance cuts should wait for at least the root four.
5. **astubbs#226, the health-check surface.** A modest hook into this class, on a base from early
   August. Rename it and rebase it, or close it; do not cut around it.

**Do not wait for these.** astubbs#333, astubbs#392 and astubbs#456 (adaptive concurrency and the two
navigator rungs) are the biggest numbers in the scan, but they are one linear stack based on
astubbs#361 through astubbs#363 and inherit whatever the cut does once astubbs#361 lands; sequence them
after. astubbs#362 and astubbs#363 are records and harness. The astubbs#242 proxy stack and the
astubbs#255 Streams stack touch this class only through the package rename. The drafts that carry
only the rename in their imports need `bin/rename-packages.sh`, not a merge slot.

## Recommended cut order

1. **Configuration validation** - unblocked today, and a rehearsal of the mechanics.
2. **Lifecycle** (`StateMachine`) - smallest of the real cuts, maps cleanly, blocked only on the
   astubbs#225 root.
3. **Rebalance and subscription** (`RebalanceHandler`) - biggest removal, blocked on the same. Draw
   the seam at the callback so confluentinc#270's message form stays possible.
4. **Worker pool and runner** - after astubbs#360 and astubbs#361.
5. **Control loop** - after astubbs#361.
6. **Mailbox**, carrying the interrupt-to-message slice.

With each cut, put the matching confluentinc#325 interface on the new class's surface, and move the
class into its confluentinc#271 package. Each cut can promote its file to file-scoped analysis on its
own; do not wait for the whole set.

## Rules that bind the cut

- [`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md`](../../parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md)
  owns the field rules: `@GuardedBy` in the same change that moves a fixed race, `@ThreadConfined`
  asserted at the entry point, and the ledger of known shared state. Moving a field to a new class
  moves its ledger entry.
- `ControllerThreadOnly` and `assertOnControlThread` are the thread-ownership contract. A cut that
  moves a method across that boundary has changed the thread model, not just the file.
- [`two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`](../solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md)
  is why the rebalance cut must not change *which thread* commits on revoke while moving the code
  that does it, and
  [`the-revoke-path-commit-did-not-drain-the-mailbox-2026-09-07.md`](../solutions/logic-errors/the-revoke-path-commit-did-not-drain-the-mailbox-2026-09-07.md)
  is the most recent defect from a callback running on a thread that does not own the state it needs -
  the exact hazard a rebalance class on its own file hides better than the monolith does.
  [`core-control-thread-contract-debts.md`](core-control-thread-contract-debts.md) lists the debts
  that cut inherits.
- [`a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md`](../solutions/architecture-patterns/a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md):
  a new class that caches something the God class still owns has created exactly the contract
  confluentinc#270 was written to remove. Pass the owner, not a copy.
- The shared-nothing thread model (confluentinc#200, mirror astubbs#142) is the larger rework
  `docs/refactoring.md` says to do this alongside. Doing the cuts first is compatible with it: every
  seam above is a boundary that model would also need.

## Prior art checked

- `bin/inflight.mjs prior-art` across every ref for the class name, the five seam names and
  "decompos": no plan or note proposes the cut; the hits are the analysis registries gated on it
  and the plan for splitting *branches*, which is about PR stacks despite the name and lives only on
  `origin/docs/god-branch-decomposition-plan`:
  `git show 90188f0bb:docs/plans/2026-08-31-001-process-god-branch-decomposition-plan.md`.
- `gh pr list --state merged` by file: the merged engine work of the last month is inside the class,
  none of it extracts.
- `gh issue list --state all` and `gh pr view -R confluentinc/parallel-consumer` for each draft
  above: confluentinc#200 and confluentinc#186 are the open upstream trackers; none has a fork PR.
- `git log --all --diff-filter=A` for the five class names in main code since 2023: nothing.

## Reproduce the merge-base measurement

Ranks every open PR by its own changes to the listed classes, from its merge base, normalising the
package rename. Run from any checkout after `git fetch --all --prune`.

```bash
NEW=bz/stub/parallelconsumer; OLD=io/confluent/parallelconsumer
REL="internal/AbstractParallelEoSStreamProcessor.java state/PartitionState.java state/WorkContainer.java \
     ParallelConsumerOptions.java state/ShardManager.java internal/BrokerPollSystem.java"
pkgof() { git cat-file -e "$1:parallel-consumer-core/src/main/java/$NEW/ParallelConsumerOptions.java" 2>/dev/null && echo "$NEW" || echo "$OLD"; }
gh pr list -R astubbs/parallel-consumer --limit 100 --json number,headRefName --jq '.[] | "\(.number)\t\(.headRefName)"' |
while IFS=$'\t' read -r n ref; do
  git rev-parse -q --verify "origin/$ref" >/dev/null || continue
  mb=$(git merge-base origin/master "origin/$ref"); bp=$(pkgof "$mb"); rp=$(pkgof "origin/$ref"); tot=0; line=""
  for r in $REL; do
    a="parallel-consumer-core/src/main/java/$bp/$r"; b="parallel-consumer-core/src/main/java/$rp/$r"
    git cat-file -e "origin/$ref:$b" 2>/dev/null || continue
    st=$(git diff --numstat "$mb:$a" "origin/$ref:$b" 2>/dev/null | awk '{print $1, $2}'); [ -z "$st" ] && continue
    set -- $st; [ "$1$2" = 00 ] && continue
    line="$line ${r##*/}(+$1/-$2)"; tot=$((tot + $1 + $2))
  done
  [ "$tot" -gt 0 ] && printf '%5d\t%s\t%s\t%s\n' "$tot" "$n" "$ref" "$line"
done | sort -rn
```
