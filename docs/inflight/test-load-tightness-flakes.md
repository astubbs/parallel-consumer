# Load-tightness flake family (undiagnosed)

Shared signature: a **fast-failing** assertion or timeout under heavy contention, passing in isolation
or on rerun. Roster and rates from the 20-run fork16 acceptance hunt on #80's branch (2026-07-30);
baseline for comparison is 15/20 runs fully clean, zero stall-class failures.

| Test | Rate | Symptom |
|------|------|---------|
| `MultiInstanceMetricsTest.sameRegistryCanBeReusedAfterPcInstanceClosed` | 0/20 hunt, ~1/104 on CI | 1-2s produce/commit lock timeouts |
| `TransactionTimeoutsTest.produceTimeout` | 1/20 + 1 highcpu | tight produce-timeout assertion |
| `LoadTest` | 1/20 | 60s throughput awaits |
| `DbTest` | 2/20 | postgres container start under contention |
| `KafkaSanityTests`, `TransactionMarkersTest` | singles | residual, uncategorised |

**Classify before touching any of them** (the #68 lesson): this family is exactly where the upstream
#857 deadlock and the drain zombie were hiding, and both looked like tightness first. Two members have
since been *solved* and left the family, which gives you their signatures to rule out - the nudge race
is an unwinnable await plus a `SubscriptionState` reset positioned past the data
(`latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md`), and the drain zombie is a poll spin
in `DRAINING` state (`pc-silent-stall-under-contention-2026-07-29.md`).

**Explicitly NOT a member: `RebalanceEoSDeadlockTest.noDeadlockOnRevoke`** (1/20). Per the #68 record
its contended failure maps to the real upstream #857 deadlock - that sighting is live confirmation the
deadlock is still on master, with its fix waiting in #29.
