# The close sequence warns identically for expected and unexpected failures

`AbstractParallelEoSStreamProcessor`'s inner close sequence wraps three steps in the same shape -
grep `during close sequence` - each catching `Exception` and logging a WARN before continuing to the
next step:

- `commitOffsetsThatAreReady()` → `failed to commit during close sequence`
- `brokerPollSubsystem.closeAndWait()` → `failed to close brokerPollSubsystem during close sequence`
- `maybeCloseConsumer()` → `failed to maybeCloseConsumer during close sequence`

Continuing rather than aborting is right - a failure in one step should not strand the others. The
problem is what the user is told.

## The defect

`catch (Exception e)` cannot distinguish *the broker went away mid-close, which is routine* from *we
have a bug*, so both are reported at WARN with the same wording. A user who sees one cannot act on
it, and a real defect is indistinguishable from noise sitting next to it. WARN is a claim that
someone should look; if nobody can act, the level is wrong, and if they should, the message needs to
say what to do.

This is the **same question the processing path already answers** - "is this failure expected?" -
simply never asked on the close path. `PCRetriableException.isPresentIn` exists precisely to split
those two cases, and nothing on this path uses it or any equivalent.

## What is NOT established

**Which of the three are expected.** Nobody has enumerated what actually throws from each step, and
guessing is what produced the undifferentiated `catch (Exception e)` in the first place. That
enumeration is the work: for each step, what can throw, and for each of those, is it a normal
consequence of shutting down or a fault.

Only once that is answered does the fix follow - expected causes drop to DEBUG or disappear,
unexpected ones keep WARN and say what to do about it, and the classification is written down so the
next `catch (Exception e)` here has a rule to follow.

## A second, smaller issue at the same sites

A throwing render aborts the remaining close steps. The log call is the last statement in each catch
block, so nothing after it *inside* the block is skipped - but an exception escaping the catch skips
the steps *below* it, so a throwing render in the first one leaves the broker poll system and the
consumer unclosed.

Reachable with a user-authored throwable: `maybeCloseConsumer` calls `consumer.close()`, which fires
`onPartitionsRevoked`, which runs the user's rebalance listener and wraps anything it throws in
`ExceptionInUserFunctionException`.

Lower stakes than the classification question above - it is shutdown rather than steady state, the
path is already degraded, and the cost is skipped close steps rather than permanently stalled
records. astubbs#267 guarded the equivalent shape at five hotter sites with
`ThrowableUtils.logWithoutEscaping` and deliberately left these; see
[`next-throw-safe-log-adapter.md`](next-throw-safe-log-adapter.md), which would remove the hazard
here without touching these lines at all.

**Do the classification first.** If some of these warns should not exist, guarding them is work spent
on a line that is about to be deleted.

## Gated on astubbs#29, which reworks what these steps call

astubbs#29 (`bugs/857-paused-consumption-multi-consumers-bug`, **draft**) does not change any of the
three lines - zero changed lines at `during close sequence`, `innerDoClose`, `maybeCloseConsumer` and
`closeAndWait`, checked against its **local** branch, which is 7 commits ahead of the pushed head.

But it rewrites `ConsumerManager` and `ThreadConfinedConsumer` by ~243 lines, and those are what
`maybeCloseConsumer()` and `closeAndWait()` call. So it changes the *inputs* to the question above -
what can actually throw from each step - without changing the warns themselves. **Do the
classification after astubbs#29 lands**, or it answers a question about code that is being replaced.

## astubbs#29 also adds a fourth variant, worth raising there

Its new `tryCommitOffsetsOnRevoke` (grep `Failed to commit offsets during revoke`) does:

```java
} catch (Exception e) {
    log.warn("Failed to commit offsets during revoke: {}", e.getMessage());
}
```

Two problems, both of which astubbs#267 removed elsewhere. `e.getMessage()` alone drops the type, the
cause chain and the stack, and prints `"... during revoke: null"` for anything thrown without a
message - on a path whose whole purpose is diagnosing a deadlock. And it is a third shape of the
classification problem in this note: catch `Exception`, warn, swallow entirely, with no rethrow.

Cheap to fix while that PR is still a draft. Raise it on astubbs#29 rather than editing that branch.

## Origin

Found by an adversarial review round on astubbs#267, which classed them as fine to leave for that
PR's scope. Recorded rather than fixed there because the deciding question is a product judgement
about what users should be told, not a code change.
