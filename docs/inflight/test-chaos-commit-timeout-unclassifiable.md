# A commit-response timeout cannot be classified, so the chaos lane fails on every branch

`ChaosRevokeUnderWorkCooperativeIT.revokeUnderWorkStaysProtocolHonestWithCooperativeAssignor` is
failing the `Chaos Pain Suite` job of the `highcpu` workflow on unrelated branches simultaneously -
observed on `fix/concurrent-listener-registration` and, with an **identical** signature, on the
docs-only branch `docs/inflight-note-currency`. A docs-only branch reproducing it is what rules out
any product change as the cause.

The assertion is `assertScenarioSlos`' end-of-run canary, "no instance may end the run with an
unclassified failure cause". The cause it cannot classify:

```
InternalRuntimeException: Timeout waiting for commit response PT30S to request
  ConsumerOffsetCommitter.CommitRequest(id=...)
```

## Why the whitelist cannot see it

`ManagedPCInstance.isExpectedCloseException` walks the cause chain testing each link with
`instanceof`, and its whitelist is `InterruptedException`, `WakeupException`, `DisconnectException`,
`ClosedChannelException`, `TimeoutException`.

The throw site is `ConsumerOffsetCommitter`, grep `Timeout waiting for commit response`, and it uses
the `InternalRuntimeException.msg(...)` factory - which takes **no cause**. So the chain is one link
long and the timeout exists only as English in the message. There is no `TimeoutException` instance
anywhere for the whitelist to match, and a genuine close-related timeout is therefore indistinguishable
from an unexpected bug.

This is a gap in the harness's classifier, not in the product: a commit that cannot complete because
its partitions were revoked mid-storm is the expected outcome of the scenario, which is what the
scenario induces on purpose.

## What is NOT yet established

- **Why it started.** The whitelist gap is longstanding, so something changed how often the commit
  path times out under this scenario, or the runner got slower. The suite draws a **random seed**
  when none is given (`chaos-pain.yml`, `CHAOS_SEED`), so a run is not reproducible without reading
  the seed out of its log first. Not yet done, and it is the first step for whoever picks this up.
- **Whether `PT30S` is the right bound** for a cooperative revoke storm, or whether the scenario has
  drifted into inducing more commit contention than it used to.

## Fixing it

Two candidate shapes, and the choice is a judgement about what the canary is for:

1. **Give the throw a real cause.** `ConsumerOffsetCommitter` knows it is a timeout; wrapping a
   `TimeoutException` makes it classifiable by the existing whitelist and improves the exception for
   users too. Widest blast radius, and the most honest.
2. **Widen the whitelist** to name this specific internal failure. Cheapest, and it keeps the change
   inside the test harness - but a message-matched whitelist entry is exactly the brittle thing this
   whitelist was written to avoid.

Do not simply delete the assertion: it is the only place a stopped-and-never-restarted instance's
failure is ever inspected, per its own comment.

## Related

- [`test-chaos-teardown-double-close.md`](test-chaos-teardown-double-close.md) predicts this failure
  *class* - "the resulting unclassified failure cause can then trip `assertScenarioSlos`" - from a
  different cause (the teardown double-close race). Same assertion, different input; fixing either
  one leaves the other.
- [`test-chaos-phase2.md`](test-chaos-phase2.md) holds the suite's open roster.

The job is not a required check - a PR showing `UNSTABLE` for this alone is not blocked on it.
