# confluentinc#857 family: two mirrors closed on reasoning, never confirmed

Both of these were attributed to a landed fix by reading the reporter's evidence, and neither was
verified against the environment that produced it. They are recorded here because "very likely fixed"
is a hypothesis wearing a conclusion's clothes, and because the 2026-08-18 work showed exactly how
that goes wrong - astubbs/parallel-consumer#44 sat attributed to astubbs/parallel-consumer#29 for
months, in a commit mode where that fix cannot run.

## astubbs/parallel-consumer#175 (confluentinc/parallel-consumer#809)

*Sporadic `InternalRuntimeException: Timeout waiting for commit response PT30S`* in production on GKE,
22 comments upstream, reported still present on the newest version.

Attributed to **astubbs/parallel-consumer#100** - an unhandled `RebalanceInProgressException` killed
the broker-poll thread, which is the only producer of commit responses, so every later commit blocked
until `offsetCommitTimeout`.

**Unconfirmed against the reporter's environment**, and the issue itself says so.

## astubbs/parallel-consumer#177 (confluentinc/parallel-consumer#833)

*PC runs for a while and then exits with `Timeout waiting for commit response PT30S`*, with ~50% of
records failing across 1000 keys.

Attributed to the same astubbs/parallel-consumer#100, on the evidence that commit responses stop
being produced at a point in time and every waiter then times out.

**That evidence does not discriminate.** A poll thread *wedged and still alive* produces the identical
trace: `maybeDoCommit()` is called only from the poll loop, so ANY reason that loop stops servicing the
queue yields "responses stop, waiters time out". Dead and wedged look the same from outside. The
issue now records both candidates rather than asserting one.

## What would settle either

astubbs/parallel-consumer#204 makes the distinction observable going forward: on a poller death it
releases the waiter immediately with the poller's own exception as the cause, so a *remaining* hang on
that path is a wedged-but-alive poller. That separates the two for future reports; it cannot
retroactively diagnose these.

For these two specifically, the honest options are to reproduce and diagnose, or to close them on
their own merits as unreproducible - **naming both candidates**. Neither should carry a closing
keyword from any PR on the present evidence.

## Do not

- Do not attach `Fixes` to either from a PR. That asserts a causation nobody established, and is the
  error corrected on astubbs/parallel-consumer#44.
- Do not treat 0.6.0.0 shipping as confirmation. A release does not verify a third party's
  environment.

Durable background:
`docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`.
