# Stream time on the PC path: what landed, and what U13 still owes

Branch `feats/ks-streams-stream-time-lowwater`, on top of
`feats/ks-streams-task-lifecycle-and-rebalance`. Plan:
`docs/plans/2026-08-11-002-feat-ks-streams-stream-time-lowwater-plan.md` (astubbs#255, master-plan U13).

## What landed

Stream time advances on the PC path. `PcTaskDispatcher` publishes a **low-water mark** - the lowest
stream timestamp still in flight, or the highest ever dispatched when nothing is, clamped monotone -
and the patched `StreamTask` reads it from `maybePunctuateStreamTime`, `canPunctuateStreamTime` and
`streamTime()`. That last one is what `ProcessorContext.currentStreamTimeMs()` delegates to, so a
**public API that returned a constant -1** on this path now returns a real value.

The extracted timestamp crosses the seam in `WorkPreparer`'s return value (`PreparedRecord`), not off
the `ConsumerRecord` - those are different numbers whenever a topology configures a `TimestampExtractor`.

Verified: Kafka's own 419 with the seam off, zero failures; module unit suite green, including nine new
stream-time tests in `PcTaskDispatcherTest`.

## What U13 still owes, in priority order

1. **U13.3's integration test.** There is no end-to-end proof: no real topology, no real broker, no
   `STREAM_TIME` punctuator observed firing, and no seam-OFF control arm. This matters more than it
   looks, because of item 2.
2. **U13.4's divergence measurements.** How far punctuation lags stock, whether the lag is bounded by
   the slowest in-flight record, whether two runs over identical input punctuate at the same points,
   and the **lateness meter split by cause** (dispatch-order term versus not-yet-fetched term).
   `getDispatchesBehindStreamTime()` exists and is unread outside tests - it is the instrument waiting
   for its experiment. The split is what U14's task-idling gate decision needs.
3. **U13.5's reinstatement ledger.** Eight of thirteen `PcUnsupportedConstruct` reasons still argue
   that stream time "never advances on the PC path", which is now false, and there are **42 occurrences
   in `pc-streams.patch`** of the same claim in the `@DoNotCall`/`@deprecated` strings a user sees as a
   compile error. Note the detector the plan specifies must match the *claim*, not one literal string:
   a grep for "never advances on the PC path" returns **zero** hits in the patch.
4. **U13.6's divergence records**, including the README's known-gaps text and `CONCEPTS.md`.

## The thing to read before trusting a number here

**Kafka's upstream suites cannot measure this unit, and that is a finding rather than an excuse.**

- `StreamTaskTest` seam-ON is **30 failing before and after**, identical case-for-case (N=3 each side).
  Both pile F cases stay red for reasons that are not stream time:
  `shouldPunctuateOnceStreamTimeAfterGap` fails at `:1209` on `numBuffered()` (pile C, U14's), and
  `shouldRespectPunctuateCancellationStreamTime`'s failure **moved** from the stream-time assertion at
  `:1303` to the asynchronous-dispatch one at `:1307` - and it was observed green in one run. Racy;
  recorded UNRESOLVED.
- `StreamThreadTest` seam-ON read **41, 37, 37 and 1** across four runs of byte-identical code. It is
  not a measurement. An interim draft of the plan reported "41 to 37" as a win; retracted.

So the evidence that stream time advances is currently indirect plus the module's own unit tests. Item
1 above is what closes that.

## Traps this work hit, worth not re-learning

- **`-Dincluded.groups=<nonexistent>` empties the module's DEFAULT surefire execution.** It is the right
  way to isolate the *upstream* execution, and it silently skips the module's own tests at the same
  time. A run that looked green with "419, zero failures" had not run a single new test.
- **`-Dtest=...` leaves stale XML in `surefire-reports-kafka-upstream/`.** It overrides the upstream
  execution's `<includes>`, so the module's own classes get written into that directory and are still
  there on the next reading. Check file timestamps before trusting a parse of that directory.
- **The seam-ON gate does work from the CLI**, despite the pom pinning
  `pc.streams.dispatch.enabled=false` in that execution's `<systemPropertyVariables>` - Surefire copies
  command-line user properties last. Prove it per run: `PC dispatch active for task` appears 122 times
  in a seam-ON run and zero in a seam-OFF one.
- **Publishing the mark per record is wrong**, and it looks right. The monotone clamp would fix the mark
  on a partial batch: hold 100, publish 100, then hold 50, and the clamp refuses to come back down. The
  publish has to be at the tail of the dispatch batch - which means a worker can read a mark that does
  not yet include its own record. That reads low, which is the safe direction.

## Delete when

Items 1-4 are done, or re-homed onto their own tracking with the evidence they need.
