# Parked: findings from the spike+freeze review, deliberately not applied

A code review of the spike and freeze cluster (`2b53104e3..6a2309c28`) returned eight actionable
findings. **They are parked by an explicit call to stop review-and-fix cycles and spend the
remaining budget on the language fan-out instead** (astubbs#242, 2026-08-14). Nothing here is
fixed; nothing here is refuted. Apply them when the fan-out's first wave is in.

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

## The rest

- **A mid-session stream error parks every client executor forever** (P0). `SessionObserver.onError`
  sets `streamClosed` and fails the handshake future, but never flips `running`, so executors block
  in `dispatchQueue.take()` with no timeout. `poll()` has already returned and the client surface
  exposes no listener, so the application cannot learn consumption stopped. The stall half is
  mechanical; how a caller *learns* is a wrapper-surface design decision every language mirrors.
- **`close()` invents Failure verdicts and drops queued work** (P1). `shutdownNow()` runs before
  `streamClosed` is set, so an interrupted user function's "processing was interrupted" failure is
  transmitted and applied engine-side — attempt incremented, retry scheduled into a stream that is
  closing. Queued records are dropped with no report at all, logged at debug. Both contradict the
  frozen shutdown contract, and the direct transport does the opposite (core's close lets in-flight
  work finish) — a transport divergence, which the one-API decision defines as a bug.
- **The produce-ack wait serializes the session's entire report lane** (P1). `producePayload` blocks
  the single serialized inbound stream callback for up to `sendTimeout` *per produce record*, so one
  slow broker interaction collapses the client's whole configured concurrency to serial. It becomes
  sharper when heartbeats land on that same lane: a produce stall could starve them into lease
  expiry for every in-flight record. Either hand the ack wait off a thread, or record the
  constraint where the lease unit will see it.
- **Three freeze guards can stay green while what they pin is violated.** (1)
  `SpecificationCoverageTest` matches field names as unanchored substrings of the spec prose, so an
  undocumented future field called `deadline`, `reason` or `window` passes — all three words are
  already in the document. (2) `bin/check-proto-breaking.sh` has no committed self-test, which the
  freeze unit's own text required ("verify the failure case so the gate is proven able to say no");
  the demonstration was manual. (3) That gate arms on the proto existing at one hardcoded path on
  master, so a post-freeze PR that renames the module, updates the constants *and* deletes a frozen
  field re-enters the grace branch and passes green. This is the repo's documented
  green-without-having-run class, seven prior instances — see
  `docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`.
- **The produce-failure branch has no test.** The produce-before-success-hook ordering is what backs
  the at-least-once claim, and its failure half — produce throws, record applied as failure and
  redelivered — is driven by nothing: every `MockProducer` in the tree is `autoComplete=true` and
  `errorNext` is never called.

## Worth settling at merge time, not now

Every commit on this branch ends its subject with `(confluentinc#154)`, and `AGENTS.md` reserves
that trailing parenthetical for the squash-added PR number, "never an issue". The ambiguity the rule
guards against is absent here (the reference is repo-qualified, not a bare number), but the merge
strategy should decide deliberately rather than inherit it, and the PR title needs the same check.

## Not covered by that review

It reviewed `6a2309c28`. The Python probe preservation (`8f4c8b86a`) landed after it was dispatched,
and everything from the fan-out's first wave is later still.
