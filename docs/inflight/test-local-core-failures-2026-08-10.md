# Five core test failures in one local session, three clusters, one diagnosis

Recorded 2026-08-10 from astubbs/parallel-consumer#240's branch. **Not a rate** - these came from roughly
seven full reactor runs on one machine over one session, under varying background load (several
concurrent Maven builds and parallel agents), and none was designed as a sample. The point of writing it
down is the *pattern* and the *asymmetry*, not the numbers.

None of the five is caused by the work on that branch: its diff touches `parallel-consumer-connect/`,
docs and `.github/`, and core cannot see any of them.

## What failed

| Test | Cluster | Status |
|---|---|---|
| `ParallelEoSStreamProcessorTest.inFlightMessagesCommittedIfProcessedDuringShutdown[3]` | shutdown-commit | **Explained** - see astubbs#260 |
| `ParallelEoSStreamProcessorTest.executorThreadsInterruptedOnShutdownTimeout[1]` | shutdown-commit | Soaked, not reproduced |
| `JStreamParallelEoSStreamProcessorTest.testConsumeAndProduce` | JStream | Unexplained, **not soaked** |
| `JStreamParallelEoSStreamProcessorTest.testFlatMapProduce` | JStream | Unexplained, **not soaked** |
| `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` | producer-lock | Unexplained, **not soaked** |

## What is already settled, so nobody re-derives it

**The shutdown-commit cluster is answered by astubbs#260**, which is open and diagnoses the mechanism as
*correct product behaviour*: PC commits the highest sequentially succeeded offset plus one, so under KEY
ordering a completed record on an unblocked shard re-commits the same base offset with updated
incomplete-offset encoding. The defect was in `assertCommits`, which meant an exact sequence in
transactional mode and a duplicate-insensitive set in the others while its javadoc claimed it collapsed
repeats. That PR also rules out the tempting reading: *"No precondition could have fixed this… unbounded
by design."* Check any new sighting in this class against that fix first.

It also corrects a link made in `test-load-tightness-flakes.md` before astubbs#260 was read: this is **not**
astubbs#101 returning - that fixed the opposite symptom, a commit that never happened.

**`ProducerManagerTest` is not fresh territory either.** A sibling flake on the same
`producerTransactionLock` was fixed in astubbs#110, and
`docs/solutions/test-flakiness/unforceable-trigger-commit-lock-timeout-2026-08-07.md` records it as the
source of that write-up's control-arm method. Start there. This sighting failed on a `getElapsed()`
timing assertion, which is the shape that doc warns is *three different mechanisms wearing one costume* -
stall, load-tightness, unforceable trigger.

## The part worth someone's attention: the local/CI asymmetry

**CI's Unit lane was green throughout, on the same trees**, while the local rate was high enough to block
roughly half the verification attempts in this session. Both facts are solid; together they are not
comfortable.

Two readings, and this session cannot settle between them:

1. The local environment differs from CI in a way that matters - core count, fork count, or background
   load from concurrently running agents - and produces contention artifacts CI never sees.
2. CI's timing happens not to surface instability that is really there. Note CI runs
   `-Dsurefire.rerunFailingTestsCount=2` was **removed** in astubbs#224 precisely because retries were
   hiding flakes, so this reading is less likely than it was - but "less likely" is not "excluded".

Only one of the five was soaked (`executorThreadsInterruptedOnShutdownTimeout`: 0/6 unloaded, 0/8 at
`SOAK_FREE_CORES=2`), so the contention reading is *evidenced* for exactly one test and *assumed* for the
rest. Do not generalise that soak.

## What would actually settle it

- Soak the three unsoaked tests with `bin/soak-test.sh <Class#method> 20` at a low `SOAK_FREE_CORES`,
  which is the tool built for this question.
- Compare the local invocation against what CI runs: `bin/ci-unit-test.sh` uses `-Pci` and a fork count
  the hosted runner's two cores imply, whereas a bare `./mvnw test` on a 12-core laptop does not.
  A difference there would explain the asymmetry without implicating the product.
- If a soak *does* reproduce under load, classify before touching anything: this repository's history has
  twice found a real main-code bug hiding behind exactly this signature.

## Do not

Loosen a timeout, add a retry, or `@Disabled` any of these to get a green local run. Per `AGENTS.md`, a
test failing under load may be exposing a real bug, and this family in particular is where confluentinc#857 and
the drain zombie were both found. Quarantine requires a diagnosis, and four of these five do not have one.
