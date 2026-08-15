# Parked: findings from the spike+freeze review, deliberately not applied

A code review of the spike and freeze cluster (`2b53104e3..6a2309c28`) returned eight actionable
findings. **They were parked by an explicit call to stop review-and-fix cycles and spend the
remaining budget on the language fan-out instead** (astubbs#242, 2026-08-14). Apply the rest when
the fan-out's first wave is in.

Six are done and deleted from this file: the produce-ack wait that serialized the report lane, its
missing failure-branch test, both halves of the freeze guards' green-without-having-run
(`SpecificationCoverageTest`'s unanchored match, and the breaking-change gate's absent self-test and
silent disarm), the P0 mid-session stream error that parked every executor silently, and `close()`
inventing Failure verdicts for an interrupted user function. What remains below is unfixed and
unrefuted.

Ordered by what bites soonest, not by severity.

## Interacts with work in flight — read this first if you are touching capabilities

**The Java gRPC client declares no capabilities, which the frozen spec reads as claiming the whole
v1 baseline.** `WireMapping.toConfigure` never calls `addCapabilities`, and
`protocol-specification.md` states that an empty list means the full baseline, not silence. It is
harmless only while the proxy's own set is `["dispatch"]` and the negotiated intersection stays
there. **The moment the lease unit adds `heartbeat` to `PROXY_CAPABILITIES`, the intersection grants
it to a client that never heartbeats**: every in-flight record returns at lease expiry, the
workers' eventual reports are fenced as superseded, and nothing commits — a redelivery loop from a
one-line omission. The fix is one line (`addCapabilities("dispatch")`, grown as duties land), and
it should go in with or before the capability grant. Ten client authors mirror this reference, so
the false claim propagates as the pattern if it stays.

## Worth settling at merge time, not now

Every commit on this branch ends its subject with `(confluentinc#154)`, and `AGENTS.md` reserves
that trailing parenthetical for the squash-added PR number, "never an issue". The ambiguity the rule
guards against is absent here (the reference is repo-qualified, not a bare number), but the merge
strategy should decide deliberately rather than inherit it, and the PR title needs the same check.

## Not covered by that review

It reviewed `6a2309c28`. The Python probe preservation (`8f4c8b86a`) landed after it was dispatched,
and everything from the fan-out's first wave is later still.
