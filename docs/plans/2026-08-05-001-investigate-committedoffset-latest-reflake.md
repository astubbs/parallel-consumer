# `PartitionStateCommittedOffsetIT.committedOffsetRemoved[1] latest` - re-flaked after #80 un-quarantined it

**Status:** RESOLVED. Hypothesis confirmed by deterministic reproduction, fixed, and verified against
a harsher trigger than the one that broke it. Kept as the record of the diagnosis - the fix commit
carries the summary.
**Opened:** 2026-08-05
**Test:** `io.confluent.parallelconsumer.integrationTests.state.PartitionStateCommittedOffsetIT#committedOffsetRemoved(OffsetResetStrategy)[1] latest`

## Why this branch exists

This test was the quarantine lane's first occupant (the `[latest]` nudge race). PR #80 owned the fix,
and on merging it removed the `@Quarantined` annotation and the registry entry. It has since failed
again, on an unrelated PR (#111, which changes no main code).

That matters procedurally as well as technically: with #80 merged there is **no open owning fix PR**,
so `bin/check-quarantine-owners.sh` will reject a re-quarantine. The options are a real fix, or a new
owning PR - this branch.

## Evidence

**Failing run:** https://github.com/astubbs/parallel-consumer/actions/runs/30880293439/job/91900105420
(job `91900105420`, `Integration Tests`, head SHA `4cffd300`, 2026-08-04T05:19:22Z → 05:24:07Z,
GitHub-hosted 2-core runner.)

> **Log-retrieval caveat, learned the hard way.** I re-ran that job to test flake-vs-deterministic,
> which replaced the visible logs with attempt 2's (and attempt 2 was then cancelled by an unrelated
> push, so it produced no verdict at all). `gh run view --job <id> --log` returns the *latest* attempt.
> The original is still reachable via the REST endpoint, which ignores attempts:
> `gh api repos/astubbs/parallel-consumer/actions/jobs/91900105420/logs`.
> Do not re-run a job whose logs you have not already saved.

### The autopsy block (verbatim)

```
=== AMBIENT PROBE AUTOPSY (test failed): [1] latest ===
failure: ComparisonFailureWithFacts: value of        : myCollection.size()
expected        : 2
but was         : 1
myCollection was: [ConsumerRecord(topic = LoadTest-834464130, partition = 0, leaderEpoch = 0,
                   offset = 50, CreateTime = 1785820927508, key = key-50, value = value-50)]
probe clean - no rebalance dwell, no lag stagnation, no frozen partitions observed:
the fault is likely in the test itself, not consumer-group progress
=== END AMBIENT PROBE AUTOPSY ===
```

Two things to take from it. The failing parameter is **`[1] latest`** - the same one #80 called out.
And **`probe clean`**: consumer-group progress was healthy, so per `AGENTS.md` this points at the test
rather than at the library.

### Timeline (same log, 05:22:0x)

```
22:07.467  Producing 200 messages to LoadTest-834464130      <- TO_PRODUCE = 200, offsets 0..199
22:08.068  PC1 assigned LoadTest-834464130-0
22:08.545  runPcUntilOffset first-poll await: pcId=PC1 ...   <- awaitWithTopicNudge begins
22:09.547  Producing 1 messages to LoadTest-834464130        <- NUDGE #1
22:10.573  Producing 1 messages to LoadTest-834464130        <- NUDGE #2
22:10.595  PC1 partitions revoked
22:11.098  new consumer, offset reset  (checkHowManyRecordsWithKeyPresent)
22:11.317  FAILURE: expected 2, but was 1
```

## What I think happened

**The test's search window is computed from an assumption that the nudge mechanism violates.**

`causeCommittedOffsetToBeRemoved` sends two compaction keys and then asks:

```java
checkHowManyRecordsWithKeyPresent("key-" + offset, 2, TO_PRODUCE + 2);
```

`searchUpToOffset = TO_PRODUCE + 2` assumes the partition contains exactly the 200 seeded records plus
the 2 compaction records, so the compaction records must be at offsets 200 and 201. The reader loop
stops as soon as it sees `searchUpToOffset - 1` (201):

```java
while (highest < searchUpToOffset - 1) { poll(1s); ... }
```

But `runPcUntilOffset` awaits via `awaitWithTopicNudge`, which **produces extra records into the same
topic** to nudge the consumer along (`BrokerIntegrationTest#awaitWithTopicNudge`). The log shows two
such nudges firing before the check. Every nudge shifts the compaction records one offset further out:

| nudges fired | compaction keys land at | window is 0..201 | outcome |
|---|---|---|---|
| 0 | 200, 201 | contains both | passes |
| 1 | 201, 202 | contains one | fails |
| **2 (observed)** | **202, 203** | **contains neither** | **fails, finds only the original at offset 50** |

That predicts precisely what was observed: exactly one `key-50` record, and it is the **original**
(`value = value-50`), not the compactor (`value = "compactor"`). The reader stopped before reaching
either compaction record.

It also explains the load-sensitivity, which is the part that makes it look like a race. Nudges fire
only when the await has not yet progressed, so a fast, uncontended box fires zero and the test passes -
locally it passed **3/3** on clean master. A contended 2-core CI runner fires one or more and the test
fails. The suite runs forked, and the same log shows `CloseAndOpenOffsetTest` and
`MultiInstanceRebalanceTest` executing concurrently.

**Confidence:** high on the mechanism, because it is arithmetic rather than timing - but it is a
hypothesis until the experiment below runs. Note it is *not* a product bug on this evidence, which is
consistent with `probe clean`. That must be re-checked rather than assumed: #80's own history is a
reminder that a "known flake" here was previously a real drain-zombie defect.

## Outcome

**The hypothesis was right, and it was arithmetic rather than timing.** Confirmed by prediction, not by
absence of failure: injecting ONE extra record - simulating `awaitWithTopicNudge` looping twice -
reproduced the exact signature (`expected: 2, but was: 1`, surviving record being the *original*
`value-50`) deterministically, on an idle machine, in BOTH parameters.

The second stale assumption turned up during the trace and explains the `[1] latest` bias:
`producedCount = producedCount + 1; // run sends one` feeds `EXPECTED_RESET_OFFSET`, which is only used
under LATEST. `git log -L` dates that line to upstream `28ccc1da6`, when `runPcUntilOffset` genuinely
sent one record up front; #80 later moved nudging inside the await and left the arithmetic behind.

Fixed by removing the assumption rather than correcting it - both call sites now read the partition end
offset from the broker. After the fix, TWO extra nudges give 3 tests / 0 failures, and a soak of 8 runs
with 2 cores free passes 8/8. The soak is corroboration only: it can fail to disprove, never prove.

**The probe is now permanent, not a one-time check.** Verifying by hand and deleting the probe would
have left nothing to catch a re-introduced count assumption - the next `TO_PRODUCE + 2` would pass on an
idle machine exactly as this one did. The test therefore produces one extra record on every run, forcing
`N >= 2` always, and `awaitWithTopicNudge`'s javadoc now states that it produces 1..N records and that
anything downstream must ask the broker rather than count.

That guard was itself verified by re-introducing the bug: with the guard in place, the old
`TO_PRODUCE + 2` bound fails **3/3 parameters in 14 seconds** with the original signature, where without
it the same bug passed locally and failed roughly 1 CI run in 6. A regression test that does not fail
when the bug returns is worse than none, because it looks like protection.

The plan below is kept as written, because the route mattered - step 1 is what turned a plausible story
into a confirmed one, and step 2's "fix the arithmetic, not the timing" is what the fix did.

## Plan (as written before the work)

1. **Falsify or confirm, deterministically.** Force `n` nudges before the check (or send `n` filler
   records directly) and assert the prediction table above: 0 → pass, ≥1 → fail. If forcing a nudge
   does not fail the test, the hypothesis is wrong and the timing questions in step 4 come first.
2. **Fix the arithmetic, not the timing.** `sendCompactionKeyForOffset` already blocks on
   `send(...).get(1, SECONDS)`, so it holds the `RecordMetadata` - the compaction records' real offsets
   are available for free. Derive the search window from those (or from `endOffsets` after sending)
   instead of from `TO_PRODUCE + 2`. **Do not** simply widen the constant: that restores greenness while
   leaving the same latent assumption, and this test has already been re-quarantined once.
3. **Sweep for the same assumption.** Any other test that computes an expected offset from a produced
   *count* while `awaitWithTopicNudge` can inject records into that topic has this bug. Grep for
   `awaitWithTopicNudge` alongside `TO_PRODUCE`-derived offsets.
4. **Only if step 1 refutes the hypothesis:** re-examine as a genuine race - consumer visibility of
   acked records, and the `latest`-vs-`earliest` asymmetry (only `[1] latest` has ever failed).
5. **Re-quarantine only with this diagnosis attached**, and only if the fix is not immediate - the
   registry requires an open owning fix PR, which this branch would become.

## Prior art

**`debug/committedoffset-firstpoll-stall` - a different symptom of this same test, and it has
instrumentation worth reusing before you write your own.** From the #80 era, it investigated the
*first-poll stall*: the test hanging in `runPcUntilOffset`'s await. That is not what happened here -
this run's await completed (it fired its nudges and moved on) and the failure came later, in the
assertion. So the two are separate faults in one test, and the earlier branch is **not** a duplicate of
this work.

Where it earns a read anyway: its whole content is `logback-test.xml` DEBUG appenders for the
kafka-client packages plus a hook in `PartitionStateCommittedOffsetIT` (2 files, 17 lines). Step 1 below
wants exactly that kind of visibility - which records actually land at which offsets - so start by
cherry-picking it rather than re-deriving the logging config. Check whether it still applies before
trusting it; it predates several merges to master.

**`docs/solutions/test-flakiness/latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md` - the
fix that CAUSED this bug, and the first thing to read.** It is the SOLVED write-up for the previous
`[1] latest` failure on this same test: with `auto.offset.reset=latest`, a single pre-await nudge record
could be leapfrogged by the offset reset resolving after it, leaving the consumer positioned past every
record that would ever exist and the await unwinnable at any timeout. The fix was to nudge from *inside*
the await instead of once up front.

That is precisely where these nudge records come from. **This bug is the second-order cost of that
fix:** moving the nudge inside the await made its count unbounded (one per poll iteration) while two
call sites went on assuming exactly one. Neither fix is wrong - the earlier one solved a real
unwinnable-await race - but the pair is a clean example of a fix changing an invariant that
arithmetic elsewhere silently depended on. `AGENTS.md` says to check `docs/solutions/` before treating
a problem as novel; here it would have named the mechanism immediately.

- `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md` - the drain-zombie
  write-up that landed with #80, and the reason "it's just a flake" gets no benefit of the doubt here.
