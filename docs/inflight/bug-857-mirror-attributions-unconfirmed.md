# confluentinc#857 family: a mirror closed on reasoning, never confirmed

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-vetted: 2026-09-08 - applied: shrunk to the astubbs#175 half, with the astubbs#177 closure recorded once here as this note owns the attribution question; checked: astubbs#175 is OPEN and still carries no confirmed attribution, astubbs#177 is CLOSED as completed at 2026-09-01T00:32:48Z - one second after astubbs#399 merged - carrying a single unrelated comment from 2026-08-27 and none naming either candidate, and astubbs#204 is MERGED -->

This was attributed to a landed fix by reading the reporter's evidence, and was never verified
against the environment that produced it. It is recorded here because "very likely fixed" is a
hypothesis wearing a conclusion's clothes, and because the 2026-08-18 work showed exactly how
<!-- post-merge: checked -->
that goes wrong - astubbs/parallel-consumer#44 sat attributed to astubbs/parallel-consumer#29 for
months, in a commit mode where that fix cannot run.

## astubbs/parallel-consumer#175 (confluentinc/parallel-consumer#809)

*Sporadic `InternalRuntimeException: Timeout waiting for commit response PT30S`* in production on GKE,
22 comments upstream, reported still present on the newest version.

Attributed to **astubbs/parallel-consumer#100** - an unhandled `RebalanceInProgressException` killed
the broker-poll thread, which is the only producer of commit responses, so every later commit blocked
until `offsetCommitTimeout`.

**Unconfirmed against the reporter's environment**, and the issue itself says so.

**That evidence does not discriminate**, which is the general point and not a property of one report.
A poll thread *wedged and still alive* produces the identical trace: `maybeDoCommit()` is called only
from the poll loop, so ANY reason that loop stops servicing the queue yields "responses stop, waiters
time out". Dead and wedged look the same from outside.

## What would settle it

astubbs/parallel-consumer#204 makes the distinction observable going forward: on a poller death it
releases the waiter immediately with the poller's own exception as the cause, so a *remaining* hang on
that path is a wedged-but-alive poller. That separates the two for future reports; it cannot
retroactively diagnose this one.

For astubbs#175 specifically, the honest options are to reproduce and diagnose, or to close it on its
own merits as unreproducible - **naming both candidates**. It should not carry a closing keyword from
any PR on the present evidence. The investigation half is
[`bug-177-commit-response-timeout-unreproduced.md`](bug-177-commit-response-timeout-unreproduced.md),
which owns the reproduction shape and the discriminator.

## What happened to astubbs/parallel-consumer#177 (confluentinc/parallel-consumer#833)

The same report, the same attribution, and the outcome this note existed to prevent. It was **closed
as completed on 2026-09-01, one second after astubbs#399 merged** - a closing keyword in a docs PR -
and it carries no comment naming either candidate mechanism. Nobody reproduced it, nobody diagnosed
it, and the closed issue now reads as a defect that was fixed.

Recorded rather than reopened: whether to reopen it is the owner's call, and the durable lesson is
the one already stated above - a closing keyword is a causal claim, and a docs PR is not in a
position to make one.

## Do not

- Do not attach `Fixes` to astubbs#175 from a PR. That asserts a causation nobody established, and is
  the error corrected on astubbs/parallel-consumer#44 and repeated on astubbs#177.
- Do not treat 0.6.0.0 shipping as confirmation. A release does not verify a third party's
  environment.

Durable background:
`docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`.
