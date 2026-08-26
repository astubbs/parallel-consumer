# The chaos suite cannot model a CRASH, only a close - and the fleet's shared memory is why

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->
<!-- inflight-state: deferred - the Class 2 stall it would chase is losing on the evidence; revisit when a crash-shaped stall is suspected on its own evidence, or when the fork wants a fidelity claim it cannot currently make -->
## What is missing

Every stop the chaos conductor performs is a `close()` of some kind. `ManagedPCInstance.stop()` and
`stopAsync()` both call `pc.close()`, which is `closeDontDrainFirst()`; `STOP_DRAIN` reaches
`closeDrainFirst()`. Both are ORDERLY: the consumer leaves the group, the coordinator is told, and a
rebalance starts immediately.

**A crashed process does none of that.** It stops heartbeating with its assignment still held, and
nothing happens until the coordinator's session timeout expires. That is the shape most of the
confluentinc#857 reports describe - a member that is gone but not seen to be gone - and no scenario
in the suite currently produces it. So the family's most-reported condition is the one the suite
cannot generate.

## Why it is not a small change

All fleet members run in ONE JVM, and the correctness checks depend on that: `totalConsumed`,
`totalStarted`, `allConsumed` and `KeyOrderLedger`'s history are in-memory structures every instance
writes to directly. Killing a process therefore means the ledger loses exactly the records that
process handled - which is the evidence a crash scenario exists to examine.

An in-JVM approximation - stop an instance heartbeating without closing it, and let the coordinator
evict it - models the group's view but not the process's: threads stay alive, sockets stay open, and
in-flight work keeps running against a consumer nobody owns. That last part is not merely lower
fidelity, it is a DIFFERENT scenario, and one that can produce failures no real crash could.

## The approach to take when this is picked up

Antony's suggestion, and it is the one that makes the rest tractable: **separate JVMs that report
through the filesystem**. Each instance appends its deliveries and counters to its own file; a
central chaos admin tails them and assembles the same ledger the in-memory version builds today.
`kill -9` then models a crash exactly, and the killed instance's evidence survives it, because what
it wrote is already on disk.

Two things to get right, both learned the hard way this week:

- **The files must not live on `/tmp` on the dev box.** It is a 32GB tmpfs shared by every agent
  session, and chaos logs filled it to 99% on 2026-08-19 - truncating a run's log mid-quiet-phase,
  which read as a test ending early and cost real diagnosis time.
- **Flush per record, and write the ledger line before the work is acknowledged.** A crash that
  loses the last buffered writes makes the ledger under-report exactly the records a crash scenario
  is asking about.

## Why it is parked rather than queued

The suite's open question is a Class 2 stall that no scenario has reproduced in 9 seeds, and the
evidence so far points at detector calibration rather than a defect
(`test-857-revoke-under-work-sightings.md`). Building a multi-JVM harness is a large piece of work
to aim at a hypothesis that is currently losing. It becomes worth doing when a crash-shaped stall is
suspected on its own evidence, or when the fork wants a fidelity claim it cannot currently make.

## Related

- `docs/inflight/test-857-revoke-under-work-sightings.md` - the replays and the recovery experiment
- `docs/testing.md` - the chaos suite's shape and lanes
<!-- file-refs: N/A - the sightings ledger arrives with astubbs/parallel-consumer#29, which this branch was split out of; it resolves once that merges -->
