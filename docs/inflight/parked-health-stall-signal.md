# Parked: the stall / progress signal half of the health-check API

Covers astubbs#126 (upstream `confluentinc#71`) and the interface half of astubbs#157
(upstream `confluentinc#484`).

## What shipped

`ParallelConsumer#getHealth()` returns a `PCHealth` snapshot — control-loop run state,
broker-poller run state, `Optional<Exception>` failure cause, and one derived
liveness verdict. `State` moved to the public package so the snapshot has a single
source of truth rather than a mirrored enum. `state` and `failureReason` became
`volatile`, and the Lombok-accidental public `setState` was narrowed.

**Two breaking changes were taken**, both before 0.6.0.0 ships, and both named in their
commit bodies (which is what the release-notes generator reads):

- `State` moved from `io.confluent.parallelconsumer.internal` to
  `io.confluent.parallelconsumer`. An enum has no source-compatible shim, so the move is
  irreversible once published. Taken because the type was effectively unreachable — no
  accessor anywhere returned one.
- `AbstractParallelEoSStreamProcessor#setState(State)` is no longer public. It was a
  Lombok `@Setter` default rather than designed API, and let any caller holding the
  concrete type force the instance to `CLOSED`.

Two further changes are release-gated and queued in
[`docs/refactoring.md`](../refactoring.md#breaking-changes-queued-for-next-major-version):
promoting `getHealth()` to abstract, and giving the concrete `getFailureCause()` an
`Optional` return.

## What did NOT ship: a stall / progress signal

astubbs#157 asks the sharper question — can I tell a *stuck* consumer from a working
one? That half is **not** answered, and astubbs#157 stays open as its home. Three
findings decided that, all from this repo's own evidence:

1. **`lastCommitTime` is not a progress marker.** `AbstractParallelEoSStreamProcessor`
   only attempts a commit when `wm.isDirty()`, and only `PartitionState#onSuccess` sets
   dirty — `onFailure` is a no-op. So a workload where every record is failing, or an
   idle topic, freezes `lastCommitTime` indefinitely on a perfectly healthy consumer.
   See `docs/solutions/test-flakiness/unforceable-trigger-commit-lock-timeout-2026-08-07.md`.
2. **The run state cannot carry it either.** Every stall documented in this repo happens
   while `state == RUNNING`. See
   `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md` — a PC
   that polled zero records for 120s with a live consumer and no exception.
3. **Our own calibrated detector still has open RED calibration.** `ProgressProbe` (in
   the chaos integration tests) ships GREEN-calibrated, having never reproduced a true
   unbounded stall on master. Publishing an `isStalled()` on that basis would
   over-promise.

The verdict's javadoc and the README say this out loud rather than implying coverage:
healthy means not shut down and not failed, never "making progress".

**Residual exposure, stated plainly:** `isHealthy()` returns true throughout the
120-second zero-poll incident above. That is the incident shape astubbs#157 reports.
Documentation is the only thing standing between the verdict and a reader who assumes
otherwise.

### Candidate mechanism when it is picked up

astubbs#222 proposes exposing
`pc.partition.highest.completed.offset - pc.partition.highest.sequential.succeeded.offset`.
That gap is head-of-line blocking, is computable from data PC already holds, and is not
defeated by the dirty asymmetry that rules out `lastCommitTime`. It is a **mechanism**
candidate, not the home — astubbs#157 is the home.

`PCHealth` is the extension point: it is `@InterfaceStability.Evolving`, so a progress
field can be added to it without touching the `ParallelConsumer` interface.

## Also deferred

- **Per-subsystem failure attribution.** The snapshot carries two run states but a single
  unattributed failure cause, sourced from the control loop. A poller-side failure already
  reaches the caller via the control loop, which rethrows it — but it is not labelled as
  the poller's.
- **A readiness-shaped verdict.** `isHealthy()` is liveness-scoped. `PAUSED` and `UNUSED`
  are both live-but-not-consuming, so wiring the verdict into a Kubernetes *readiness*
  probe would keep a permanently-paused instance in rotation. A readiness verdict would be
  a new accessor on `PCHealth`, not a change to `isHealthy()`.
- **Whether the snapshot's state accessors should be `Optional<State>`** so the `default`
  method can decline to derive them. Worth settling before 0.6.0.0 ships — see the plan's
  Outstanding Questions.

## Collisions to watch

- astubbs#57 owns `PCMetrics.java` and `PCMetricsDef.java`. This work adds no meter, but
  the `State` move forces a one-line import change in `PCMetricsDef.java`. Sequence behind
  astubbs#57 or expect a trivial import-level conflict.
- astubbs#29 owns the poll/lifecycle internals. This work touched only field modifiers on
  `state` / `failureReason` and added a read accessor to `BrokerPollSystem`; it did not
  reshape any transition.

## Review residuals not actioned

From the pre-merge review. Each was judged real but not worth blocking on; recorded so they
are not rediscovered from scratch.

- **`State` is a very generic name in the root public package.** Every sibling is qualified
  (`PCHealth`, `PCMetrics`, `ParallelConsumerOptions`). A user doing a wildcard import who
  also has their own `State` gets an ambiguous reference. `PCState` was proposed. Not taken:
  the fork's value is being drop-in for upstream 0.5.x users, and the name is what upstream
  called it. Frozen once 0.6.0.0 publishes — an enum has no shim.
- **An interrupt that is not a requested close reads as a clean shutdown.** The control
  loop's `catch (InterruptedException)` runs `doClose` without writing a failure cause, so
  the snapshot is `CLOSED` with nothing to blame. `notifySomethingToDo()` interrupts the
  control thread routinely, so this is a real path. Whether an unsolicited interrupt should
  count as a failure is a design call.
- **The read order in `getHealth()` has no test.** Swapping the two lines breaks nothing —
  every health test reads a quiesced instance. A repeated concurrent-reader test asserting
  that a `CLOSED` state never coexists with an absent cause after a crash would pin it.
- **`PCHealth` is not `final`.** Lombok's generated all-args constructor is package-private
  so external subclassing is blocked today, but that is an accident of Lombok's defaults
  rather than stated intent.
- **Serializing `PCHealth` straight out of an Actuator endpoint is likely to disappoint** —
  an `Optional` needs `jackson-datatype-jdk8`, and an `Exception` field serializes to a
  large, cycle-prone graph. Callers should map it into their own payload.
- **If a future field on `PCHealth` is added as `@NonNull`**, every existing
  `builder()...build()` call site keeps compiling and starts throwing at runtime. Add fields
  as nullable or with `@Builder.Default`.
