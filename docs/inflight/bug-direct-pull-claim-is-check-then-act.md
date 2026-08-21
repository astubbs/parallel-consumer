# A record can be delivered twice under direct pull: the claim is check-then-act, not compare-and-set

<!-- inflight-type: bug -->
<!-- inflight-impact: correctness -->
<!-- inflight-state: open - diagnosed and reproduced, deliberately NOT fixed yet -->
<!-- inflight-labels: release-note, needs-measurement -->

Diagnosed 2026-08-22 on `research/market-analysis-recut`, from the raw `AssertionError` recorded in
[`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md). **The mechanism is proven and
reproduced; no fix is applied.** The product tree is unchanged - what this note records is the
diagnosis, the numbers, and the shape a fix would have to take.

**Only the direct-pull engine is affected.** `WorkManager#getWorkIfAvailable` has exactly two callers
in product code: `AbstractParallelEoSStreamProcessor#retrieveAndDistributeNewWork`, which is the
control loop and is single-threaded, and `DirectPullWorkerPool#takeBlocking`, which every worker
runs. The defect needs two concurrent selectors, so the engine PC ships cannot reach it.

## The mechanism

`ProcessingShard#getWorkIfAvailable` guards a take with

```java
if (workContainer.isAvailableToTakeAsWork() && workContainer.onQueueingForExecution()) {
```

Those are two separate steps. `isAvailableToTakeAsWork()` reads three terms - not in flight, no
success verdict, retry delay passed - and `onQueueingForExecution()` then re-validates **only the
first**, via `inFlight.compareAndSet(false, true)`. The CAS makes the *claim* atomic; it does not
make the *decision* atomic, and the comment above it says the claim decides, which is true only of
the in-flight term.

`WorkManager#onSuccessResult` completes a record in this order:

```java
wc.endFlight();   // inFlight = false        <-- releases the only term the claim re-validates
pm.onSuccess(wc); // offset out of incompleteOffsets
sm.onSuccess(wc); // container out of the shard
```

`endFlight()` comes **first**. So between it and `sm.onSuccess` the container is out of flight, still
in the shard's `entries`, and carrying a success verdict. A scanner arriving *in* that window is
still refused, because `isAvailableToTakeAsWork()` tests the verdict. A scanner that read
availability *before* the record was ever taken, and reaches its CAS inside that window, is not.

### The interleaving, step by step

1. Puller **A** scans the shard, reaches offset X, evaluates `isAvailableToTakeAsWork()` → **true**
   (free, no verdict, no delay). A is descheduled here, between the check and the claim.
2. Puller **B** evaluates the same guard, wins `onQueueingForExecution()`, and takes X.
   `deliveryCount` = 1.
3. B hands X to the control thread, which runs the user function's verdict:
   `onUserFunctionSuccess()` → `handleFutureResult` → `onSuccessResult`.
4. `endFlight()` sets `inFlight` back to **false**. `pm.onSuccess` removes X from `incompleteOffsets`.
   `sm.onSuccess` removes the container from the shard.
5. A resumes and executes the second half of its guard: `onQueueingForExecution()`. The CAS
   `false → true` **succeeds**, because step 4 reset the flag. A takes an already-completed record.
   `deliveryCount` = 2.
6. **The claim clears the verdict** - `maybeUserFunctionSucceeded = Optional.empty()` - erasing the
   one term that would have refused it. There is now no trace on the container that it ever
   succeeded.
7. A already holds the container reference from its own iteration, so its removal from the shard in
   step 4 does not stop A returning it. **The user function runs a second time on the same record.**
8. The verdict comes back, `handleFutureResult` routes it to `onSuccessResult` again, and
   `PartitionState#onSuccess` does `incompleteOffsets.remove(offset)` on an offset that is already
   gone.

Step 8 is the sighting. In a test JVM `assert (removedFromIncompletes)` fires. **In production
assertions are off, so step 8 returns quietly, and the only surviving evidence is step 7 - the record
processed twice and the offset committed twice.** That is the `1001 records for 1000` sighting in the
flakes ledger, same defect, weaker instrument.

Two lesser symptoms, useful as corroboration: `availableWorkContainerCnt` is decremented once per
take but incremented once per admission, so a double take drives it negative (it is deliberately
unclamped, so this is visible); and `sm.onSuccess`'s second `entries.remove` returns null, so the
conservation population is *not* double-retired - conservation stays balanced and cannot detect this.

## Evidence

### Deterministic proof of the primitive

The interleaving above played out by hand, no threads: check availability, run a full
take/succeed/return cycle, then perform the claim. Confirmed, in order:

- **P1** - the claim returns `true` on an already-completed record. **Held.**
- **P1b** - and clears the success verdict. **Held.**
- **P2** - returning that second claim throws `AssertionError` from `PartitionState.onSuccess`,
  matching the recorded stack. **Held.**
- **P3** (control) - re-evaluating `isAvailableToTakeAsWork()` at claim time refuses it, and a second
  claim attempted while the record is *still in flight* also refuses. **Held.** So it is the
  **staleness of the check relative to the claim**, not the check itself.

### Reproduction from the real concurrent path

Many concurrent copies of `DirectPullConcurrentSelectionTest#theInFlightCounterNetsBackToZeroWithPullsAndReturnsOverlapping`
in one JVM - the pressure dimension a full suite supplies and running the class alone does not.
Twelve cores, one-minute load average 7-9, `UNORDERED`, 24 concurrent scenarios, 8 pullers and 1
returner each, 1500 records per scenario, batch 4. Counts occurrences instead of failing on the
first.

| Arm | Scenario runs | Record completions | Double completions |
|---|---|---|---|
| **Prediction P4, first attempt** - 12 scenarios, 600 records, batch 4 | 120 | 72,000 | **0 - REFUTED at this scale** |
| **Prediction P5** - 12 scenarios, 400 records, batch 1 (maximum claim contention) | 240 | 96,000 | **0 - REFUTED at this scale** |
| Harness validation: claim sabotaged to a non-atomic check-then-set | 60 | 24,000 | 682 (2.8%) |
| **Baseline run 1** - the table's conditions | 2,400 | 3,600,000 | **2** |
| **Baseline run 2** - the table's conditions | 7,200 | 10,800,000 | **2** |
| **Control arm** - one term changed (below) | 7,200 | 10,800,000 | **0** |

**Report the refutations first, because they are the trap.** P4 and P5 were stated in advance and
both came back zero, and either would have read as "cannot reproduce" if the campaign had stopped
there. The defect needs roughly forty times that volume: **4 occurrences in 14,400,000 record
completions across 9,600 scenario runs**, or about **1 in 2,400 runs of the failing test's shape**
- consistent with a defect seen once in a full suite and 0 out of 8 in isolation.

**The negatives are trustworthy only because the harness was validated against a sabotage.**
Replacing the CAS with a check-then-set produced 682 detections in 24,000 completions, so a
zero from this harness means zero, not blindness.

**Every single occurrence, in all four, was reported as `delivery2/returns2`** - the container had
`deliveryCount == 2` and had been handed to the returner exactly twice. That is direct evidence for
a double *take*, and it rules out an offset that was never added.

### The control arm

One term changed, everything else identical: after winning the CAS, the claim re-checks the success
verdict and rolls back if one is present.

```java
if (!inFlight.compareAndSet(false, true)) { return false; }
if (isUserFunctionSucceeded()) { inFlight.set(false); return false; }   // the changed term
```

Predicted before running: double completions go to 0, with no records lost. **Both held** - 0 in
10,800,000, and all 10,800,000 records still completed, so nothing stalled. The deterministic proof
above flips at exactly **P1** under this arm and its in-flight control still passes, which is the
same-magnitude-different-position check rather than a bigger hammer.

The re-check is sound under the JMM without further ordering work: the verdict is written before
`endFlight()`'s volatile write, and a CAS that observes `false` has read that write, so the
happens-before edge makes the verdict visible. **It is written here as an experiment, not proposed as
the fix** - a fix should decide whether the claim's whole decision belongs in one atomic step (a
state field, rather than a boolean plus three side conditions) instead of bolting a second term onto
the CAS.

## What was ruled out, and how

- **A harness bug in the failing test. Ruled out.** The returner receives each container once per
  `toReturn` insertion, and the reproduction instruments per-offset return counts independently: a
  double completion is always accompanied by `deliveryCount == 2`, so the container was *taken*
  twice, not enqueued twice. The deterministic proof needs no test threads at all. And the harness's
  shape is the engine's shape - N workers on `WorkManager#getWorkIfAvailable`, one control thread on
  `handleFutureResult` - which is exactly `DirectPullWorkerPool#takeBlocking` plus the control loop's
  mailbox drain.
- **Any other path reaching `PartitionState#onSuccess`. Ruled out.** One caller chain only:
  `handleFutureResult` → `onSuccessResult` → `PartitionStateManager#onSuccess` →
  `PartitionState#onSuccess`.
- **An offset that was never in `incompleteOffsets`, making the assert fire with no double delivery.
  Ruled out for these sightings.** Three other things remove from `incompleteOffsets` - the bootstrap
  truncation in `maybeTruncateBelowOrAbove`, the poll-batch prune in
  `maybeTruncateOrPruneTrackedOffsets`, and the re-init on assignment - and all three run only on
  registration or assignment, which in the failing test completes before any puller starts.
  Independently, every observed occurrence carried `deliveryCount == 2`.
- **Two `WorkContainer` instances for one offset. Not this.** The stale-replacement path in
  `ProcessingShard#addWorkContainer` needs an epoch change, and the failing test assigns once and
  registers once.
- **Machine load. Not the variable**, as the flakes ledger already established at 0/8. The
  reproduction here ran at a one-minute load of 7-9, well below the 12.8 at which isolation produced
  nothing. Concurrent interleaving inside one JVM is the variable, and volume is what buys it.
- **A memory-visibility bug in the availability check. Ruled out by reading.**
  `isAvailableToTakeAsWork()` reads `isNotInFlight()` (a volatile read) *before*
  `isUserFunctionSucceeded()` (a plain read), so the acquire semantics of the first make the verdict
  visible. Get that order backwards and there would be a second, independent hole.

## A second, latent instance of the same shape - not the cause here, but the same assert

`PartitionState#maybeRegisterNewPollBatchAsWork` does

```java
getShardManager().addWorkContainer(epochOfInboundRecords, aRecord);
addNewIncompleteRecord(aRecord);
```

in that order, so a record is visible to shard scanners **before** its offset is in
`incompleteOffsets`. Under direct pull the scanners are worker threads. This is closed today only
because registration and completion both run on the control thread, so a worker cannot get a verdict
back while registration is in progress. It is recorded because it is the one shape that would fire
the same assert with no double delivery at all, and because "the control thread does both" is an
invariant nothing states or tests.

## Rebuilding the reproduction

The scaffolds are deliberately not committed - the deterministic one currently *passes*, so
committing it would enshrine the defect, and the stress harness finds nothing at any setting cheap
enough for the suite. Rebuild as a test in
`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/state/`:

- **Deterministic**: register one record; read the container via `shard.getWorkContainerAt(0L)`; call
  `isAvailableToTakeAsWork()` and keep the result; then `wm.getWorkIfAvailable(1)`,
  `onUserFunctionSuccess()`, `wm.handleFutureResult(...)`; then call `onQueueingForExecution()` on the
  container you kept. It returns `true`. Feed it back through `handleFutureResult` for the
  `AssertionError`.
- **Stress**: the body of `theInFlightCounterNetsBackToZeroWithPullsAndReturnsOverlapping`, with its
  own `PCModuleTestEnv` per scenario, run from an outer pool of 24 scenarios; catch the
  `AssertionError` around `handleFutureResult` and count it rather than failing; count returns per
  offset to separate a double take from a double enqueue. Needs of the order of 10^7 record
  completions to land a hit - budget about 15 seconds per 10^7 on twelve cores.
- **Always validate the harness before believing a zero**: sabotage `onQueueingForExecution` to
  `if (inFlight.get()) return false; inFlight.set(true);` and confirm detections appear.

## Prior art, including what returned nothing

- [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) - both sightings, and the ruling-out
  that this note completes. Its "what has NOT been ruled out" names branch (a), selection handing the
  same container to two pullers, as supposedly excluded by the claim CAS. **That exclusion was
  wrong**, and this is branch (a) after all - the CAS excludes two *simultaneous* claims, not a claim
  whose decision was made before the record completed.
- [`bug-direct-pull-engine-review-findings.md`](bug-direct-pull-engine-review-findings.md) - the
  correctness read of the same engine. It examined this very expression and concluded "the claim
  decides, not the availability check", which is the reading this note corrects.
- Merged PRs touching `DirectPull|ProcessingShard|PartitionState`: astubbs#296, astubbs#31,
  astubbs#114, astubbs#24. astubbs#31 (a stale container at a reused offset after rebalance) is the
  only one in this area; different mechanism, epoch-driven, not related.
- Issues, all states: nothing on this. astubbs#173 (`confluentinc#777`, duplicate processing on
  partition revocation) is a different duplicate-delivery mechanism - revocation, not selection.
- `docs/solutions/`: **nothing** for this mechanism.
