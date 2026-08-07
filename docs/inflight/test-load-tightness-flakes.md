# Load-tightness flake family (undiagnosed)

Shared signature: a **fast-failing** assertion or timeout under heavy contention, passing in isolation
or on rerun. Roster and rates from the 20-run fork16 acceptance hunt on astubbs#80's branch (2026-07-30);
baseline for comparison is 15/20 runs fully clean, zero stall-class failures.

| Test | Rate | Symptom |
|------|------|---------|
| `MultiInstanceMetricsTest.sameRegistryCanBeReusedAfterPcInstanceClosed` | 0/20 hunt, ~1/104 on CI | 1-2s produce/commit lock timeouts |
| `TransactionTimeoutsTest.produceTimeout` | 1/20 + 1 highcpu | tight produce-timeout assertion |
| `LoadTest` | 1/20 | 60s throughput awaits |
| `DbTest` | 2/20 | postgres container start under contention |
| `KafkaSanityTests`, `TransactionMarkersTest` | singles | residual, uncategorised |
| `PartitionStateCommittedOffsetIT.committedOffsetRemoved[3] none` | 1 sighting (2026-08-05) | `RebalanceInProgressException` out of the test's own setup |

**Classify before touching any of them** (the astubbs#68 lesson): this family is exactly where the upstream
confluentinc#857 deadlock and the drain zombie were hiding, and both looked like tightness first. Two members have
since been *solved* and left the family, which gives you their signatures to rule out - the nudge race
is an unwinnable await plus a `SubscriptionState` reset positioned past the data
(`latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md`), and the drain zombie is a poll spin
in `DRAINING` state (`pc-silent-stall-under-contention-2026-07-29.md`).

The `committedOffsetRemoved[3] none` sighting, for whoever picks it up: only the `NONE` parameter runs
that setup block, where a `subscribe`d raw `KafkaConsumer` gets a single `poll(1s)` to complete its join
before `commitSync` - not a guarantee under contention. Seen once, in a deliberately CPU-loaded probe
run during astubbs#115, and one sighting is not a rate. The exception names its own remedy, which makes
"just await assignment" tempting; resist that until it is classified, because astubbs#100 fixed a *main-code*
bug in exactly this area (a rebalance-time commit killing the broker-poll thread). Reproduce with
`bin/soak-test.sh 'PartitionStateCommittedOffsetIT#committedOffsetRemoved' 20` at a low
`SOAK_FREE_CORES`.

**Explicitly NOT a member: `RebalanceEoSDeadlockTest.noDeadlockOnRevoke`** (1/20). Per the astubbs#68 record
its contended failure maps to the real confluentinc#857 deadlock - that sighting is live confirmation the
deadlock is still on master, with its fix waiting in astubbs#29.
