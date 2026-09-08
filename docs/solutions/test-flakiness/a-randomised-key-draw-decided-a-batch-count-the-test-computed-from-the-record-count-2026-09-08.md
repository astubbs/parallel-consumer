---
title: "A randomised key draw decided the batch count, and the test computed its expectation from the record count"
date: 2026-09-08
category: test-flakiness
module: parallel-consumer-core
problem_type: test_design_bug
component: testing
severity: medium
root_cause: exact_expectation_derived_from_record_count_while_the_randomised_key_distribution_decides_it_under_order_restricted_shards
resolution_type: test_fix_controls_the_input_distribution_and_keeps_the_exact_assertion
status: "SOLVED - test-side fix. The mechanism is confirmed by a three-arm controlled experiment with a control arm, and the exact CI signature reproduces deterministically on demand. The product behaviour is correct and unchanged."
applies_when:
  - "A test asserts an EXACT count of something the engine derives from how records are DISTRIBUTED, not from how many there are"
  - "The input distribution is randomised by a shared test helper the assertion never inspects"
  - "The ordering mode is KEY or PARTITION, so a shard hands out at most one record per work-retrieval round"
  - "A flake reproduces at a rate near a combinatorial probability rather than near a timing one"
symptoms:
  - "ConditionTimeoutException, alias 'expected number of batches', 30 seconds"
  - "Expected size: 3 but was: 4"
  - "Only the KEY parameter fails - simpleBatchTest(ProcessingOrder)[3] - never [1] or [2]"
  - "The dumped batch contents carry one key drawn three times out of five records"
  - "Fires across parallel-consumer-reactor, -mutiny and -vertx alike, on branches whose diff contains no main Java"
  - "Passes on re-run of the identical head, and is invisible to any load or contention arm"
related_components:
  - BatchTestMethods
  - KafkaTestUtils
  - ProcessingShard
  - AbstractParallelEoSStreamProcessor
tags: [batching, key-ordering, shards, randomised-input, expectation-versus-input, awaitility, flake]
---

# A randomised key draw decided the batch count, and the test computed its expectation from the record count

`BatchTestMethods.simpleBatchTest` sends five records at `batchSize=2` and asserts, exactly, that they
arrive in `ceil(5 / 2) = 3` batches. Six sightings across `ReactorBatchTest`, `MutinyBatchTest` and
`VertxBatchTest` between 2026-08-18 and 2026-09-05 timed that assertion out at 30 seconds with
`Expected size: 3 but was: 4`, always on the `KEY` parameter, always on a branch that could not have
caused it. It was carried as undiagnosed in
[`docs/inflight/test-untracked-ci-flakes.md`](../../inflight/test-untracked-ci-flakes.md) with three
readings live: contention, a product defect in the batcher, and expectation-versus-input.

**It is the third, and none of the other two is involved.** The rule worth keeping is the general one:

> **An exact count is only assertable over an input the test controls.** If the engine derives the
> number from how the records are *distributed* and the helper randomises that distribution, the
> assertion is a lottery whose odds are set by a draw nobody reads - and no amount of re-running,
> load-shedding or timeout-loosening touches it.

## The mechanism, in two lines of product code

Under `KEY` ordering a shard is per key, and `ProcessingShard.getWorkIfAvailable` breaks out of its
scan after taking one record when `isOrderRestricted()` - so a shard yields **at most one record per
work-retrieval round**. `AbstractParallelEoSStreamProcessor.makeBatches` then partitions whatever that
round returned into chunks of `batchSize`. Batches are therefore
`sum over rounds of ceil(recordsTakenInThatRound / batchSize)`, and the number of rounds is decided by
how many records are on distinct keys.

`KafkaTestUtils.sendRecords` draws keys **with replacement** from a hundred-integer pool, so five
records can land two, three or more to a key. Five distinct keys give one round of five and three
batches. One key drawn three times gives rounds of 3, 1, 1 and **four** batches. The expectation
`ceil(records / batchSize)` never moves, because it is computed from the record count alone.

The library is right and the test is wrong. Three records on one key cannot share a batch without
breaking the ordering guarantee `KEY` exists to provide.

## The experiment, with its control arm

Three arms, KEY ordering, `batchSize=2`, five records, everything but the key distribution held
identical; predictions stated before each run. Batches were counted directly rather than through the
30-second await, so a failing arm costs no more than a passing one.

| Arm | Keys | Prediction | Result |
|---|---|---|---|
| A - control, as the test ships | random, with replacement | 3 batches in ~all reps; a 3-way collision is about 1 draw in 1000 | **20/20 gave 3.** Two reps happened to draw a 2-way collision and still gave 3 |
| B - forced 3-way collision | `7,7,7,8,9` | 4 batches every time | **20/20 gave 4**, sizes `2+1+1+1` |
| C - forced distinct keys | `1,2,3,4,5` | 3 batches every time | **20/20 gave 3**, sizes `2+2+1` |

`2+1+1+1` is the shape recorded in all four sightings whose batch contents were captured - two
singleton keys paired into one batch, then the three colliding records one to a batch each.

Supplementary arms pin the predicate more precisely than "3-way collision":

| Arm | Keys | Result |
|---|---|---|
| D | `7,7,8,8,9` - two 2-way collisions | **3 of 5 reps gave 3, 2 of 5 gave 4** |
| E | `7,7,7,7,8` | 5/5 gave 4 |
| F | `7,7,7,7,7` | 5/5 gave 5 |

So **any** key collision can cost an extra batch, and a 3-way collision did so every time. Arm D is the
arm that matters for the rule: the same input produced two different counts, which is proof that no
expectation computed over a colliding input can be exact.

**The literal CI signature reproduces on demand.** Pointing `simpleBatchTest` itself at
`7,7,7,8,9` and running `CoreBatchTest#simpleBatchTest` fails 1 of its 3 parameters in 31.7s with
`ConditionTimeoutException`, alias `expected number of batches`, `Expected size: 3 but was: 4` -
character for character what CI reported six times.

## Why the rate looks the way it does

Five draws from a hundred keys give a 3-way-or-worse collision about once in a thousand. Only the
`KEY` parameter is exposed, in four classes per unit run (core plus three wrappers), so roughly one
unit-lane run in 250 should carry one. Six sightings over three weeks of pull-request runs is the
order of magnitude that predicts - which is the corroboration a contention reading never had, since
contention would have to explain why the failure spared `UNORDERED` and `PARTITION` entirely.

## The fix, and why it is not a loosened assertion

Keeping the exact count means removing the randomness from the input, not weakening what is asserted.
`KafkaTestUtils.sendRecordsWithDistinctKeys` draws **without** replacement - the distribution stays
arbitrary, the collision cannot happen - and `simpleBatchTest` uses it. The exact-count assertion is
unchanged and is now actually justified.

That leaves `KEY` behaving like `UNORDERED` for that test, so the KEY-specific guarantee moves into a
new test of its own rather than being lost: `BatchTestMethods.keyOrderNeverBatchesTwoRecordsOfOneKey`
sends a deliberate 3-way collision and asserts what is deterministic about it - every record delivered
exactly once, no batch over `batchSize`, and **no batch holding two records of one key**. Net coverage
goes up: the behaviour that broke the old expectation was never tested before.

**Residual, stated rather than glossed.** Distinct keys remove the collision but not the theoretical
possibility that a retrieval round splits unevenly (rounds of 1, 1, 3 would also give four batches).
That was not observed in 41 reps of arms A and C, it has never been sighted on the `UNORDERED`
parameter which has always been exposed to exactly the same split, and it is a different mechanism from
the one diagnosed here. If `[1]` and `[3]` ever fail together with no key collision in the payload,
that is the thing to look at - and the fix would be to make the test wait for all records to be
queued before the first retrieval, not to loosen the count.

## Other instances of the class - searched, and none found

The class is *an exact count asserted over an input whose distribution the test randomises*. Every
caller of `KafkaTestUtils.sendRecords` and every `Math.ceil` in test code was read:

- `BatchTestMethods.batchFailureTest` - computes the same `ceil` but asserts
  `hasSizeGreaterThanOrEqualTo`. A collision can only add batches, so it cannot fail this way. Dismissed.
- `BatchTestMethods.averageBatchSizeTest` - `UNORDERED`, where each record is its own shard. Not exposed.
- `TransactionalBatchProduceTest`, `MdcContextPropagationTest`, `OffsetEncodingBackPressureUnitTest` -
  all `UNORDERED` (the last by the base class default) and all asserting record counts, not batch counts.
- `TransactionMarkersTest` - `PARTITION`, and its own comment says `// just so we dont need to use keys`,
  which is this hazard already understood at one call site.
- `BitSetEncoder` - main code, unrelated ceiling arithmetic.

So it is a single site, not a pattern. The guard against it recurring is
`sendRecordsWithDistinctKeys` existing and saying in its javadoc when to reach for it.
