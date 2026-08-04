# `committedOffsetRemoved[3] none` can error on a rebalance during its own setup

Seen **once**, during the #115 investigation, in a deliberately CPU-loaded probe run. Not reproduced
since, and not the same fault as the nudge-arithmetic bug #115 fixed: that one failed an assertion in
`checkHowManyRecordsWithKeyPresent`, this one throws before reaching it.

```
[3] -- ERROR!
org.apache.kafka.common.errors.RebalanceInProgressException: Offset commit cannot be completed
since the consumer is undergoing a rebalance for auto partition assignment. You can try
completing the rebalance by calling poll() and then retry the operation.
```

**Where.** Only the `NONE` parameter runs this setup block (`PartitionStateCommittedOffsetIT`, in the
`offsetResetPolicy.equals(NONE)` branch): a raw `KafkaConsumer` is `subscribe`d - so group-managed -
given a single `poll(Duration.ofSeconds(1))` to complete the join, then immediately `commitSync`. One
second is not a guarantee that the group has finished rebalancing, and under contention it isn't enough.

**Do not assume it is only a test-timing problem.** The error names its own remedy, so the tempting fix
is to await assignment instead of relying on one poll. But #100 fixed a *main-code* bug in this exact
area - a rebalance-time commit escaping the control loop and killing the broker-poll thread - and the
#857 family has a history of looking like test flakiness first. Establish which it is before changing
the test: give the consumer an uncontended broker and see whether it still happens, per the
contention-vs-genuine-bug rule in `AGENTS.md`.

**To reproduce:** `bin/soak-test.sh 'PartitionStateCommittedOffsetIT#committedOffsetRemoved' 20` with a
low `SOAK_FREE_CORES`. One observation is not a rate; get one before spending time on a fix.
