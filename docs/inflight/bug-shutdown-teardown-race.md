# Teardown can race a live broker-poll thread on a failing shutdown

<!-- inflight-type: bug -->
<!-- inflight-impact: reliability -->
<!-- inflight-vetted: 2026-09-07 - the ordering exposure is unchanged: `AbstractParallelEoSStreamProcessor.doClose()` still runs `deregisterMeters()` and `pcMetrics.close()` in a `finally` that executes whether or not the poll thread was joined, and the `brokerPollSubsystem.closeAndWait()` call site still catches `Exception` and only warns ("the consumer may not be closed..."), so a timeout still falls through to that `finally`. astubbs#57 is MERGED and its defensive fix is present but metrics-only - `PCMetrics.track()` undoes a late registration under `metersLock` and its javadoc names this exact unjoined-poll-thread path. Nothing has addressed the sequencing decision -->

`AbstractParallelEoSStreamProcessor.doClose()` runs subsystem and metrics teardown in a `finally`
that executes **even when the broker-poll thread was never joined**:

- `brokerPollSubsystem.closeAndWait()` declares `TimeoutException`, but its call site wraps it in a
  `catch (Exception)` that only warns (`AbstractParallelEoSStreamProcessor`, the
  `failed to close brokerPollSubsystem during close sequence` log line), so a timeout falls
  through to the `finally`.
- Earlier unguarded steps (`processWorkCompleteMailBox`, `drain()`) can throw straight past the join.

So on any shutdown that times out or throws - not the happy path - teardown runs while the poll
thread is still alive.

**The decision this needs:** whether `doClose` must guarantee the poll (and worker) threads are
joined before the `finally` teardown, or whether the teardown should instead be guarded on join
success. That belongs with whoever is hardening shutdown-under-load; it is a sequencing change, not
a local patch.

## Why this is filed separately from the metrics fix

Surfaced by the PCMetrics late-registration orphan: on that same failing-shutdown path, the poll
thread could register a meter while the control thread ran `pcMetrics.close()`, leaving the meter
orphaned in a (often user-supplied) registry.

<!-- post-merge: checked -->
astubbs#57 fixed that **defensively at the metrics end only** - `PCMetrics.track()` now removes a late
registration under `metersLock` instead of merely skipping it. That is a real fix for a real leak,
and it is deliberately not a fix for the ordering problem above.

The ordering exposure is broader than metrics: any teardown in that `finally` is exposed, and
metrics simply happened to be where it was noticed. Nothing about it has been addressed.
