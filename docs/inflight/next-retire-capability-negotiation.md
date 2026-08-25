# Retire capability negotiation — lockstep versions make it redundant

**Owner's call, 2026-08-15:** "We don't need to worry about negotiating capabilities. The version of
the sidecar and the client library must move in lockstep, and they can do a version check at the
beginning, and it's a hard failure if the versions don't match. No need for that extra plumbing.
Keep it simple."

He is right about the destination. Capability negotiation answers "what does the other side
support?", and lockstep versioning already answers it: version N of a client and version N of the
sidecar are two halves of one release. Negotiating what both sides already know is cost without
benefit — a second mechanism to keep true, in ten languages.

See [`next-client-sidecar-version-lockstep.md`](next-client-sidecar-version-lockstep.md) for the
lockstep rule itself and how the version check is added additively.

## The one ordering constraint — do not do this yet

**Capabilities are load-bearing today precisely because the clients are half-built.** The engine
currently grants `dispatch, heartbeat, manifest, worker-death`; every client declares only
`["dispatch"]`, and the negotiated intersection is what stops the engine expecting heartbeats no
client sends. Without it, the lease machinery would return every in-flight record at expiry and fence
the late reports as superseded — nothing would commit. That failure is not hypothetical: it is
exactly what the Java client's empty capability list would have caused, caught hours before it would
have fired.

So the sequence is:

1. **Now** — add the version check, and keep negotiation while the clients are partial.
2. **When each client implements what its sidecar version expects** — which is what lockstep
   guarantees at release — negotiation has nothing left to decide, because the answer is always
   "everything this version defines".
3. **Then** — stop negotiating. Clients stop declaring, the engine stops intersecting, and the
   version check carries the whole burden.

## What "retire" means, given the schema is frozen

The fields stay. Removing `Configure.capabilities` or `Configured.capabilities` is a breaking change
and `buf breaking` would refuse it, correctly. Retiring the *mechanism* costs nothing on the wire:
the fields simply stop being read, documented as vestigial with a pointer to this decision.

That is worth stating plainly for whoever reads the schema later and wonders why a negotiated-looking
field is ignored — an unexplained unused field invites someone to start using it again.

## What is genuinely lost, and why it does not matter here

Negotiation would let a *newer* client talk to an *older* sidecar with degraded features. That is a
real property in systems where the two are versioned independently — and it is exactly the property
lockstep has already decided not to want, because supporting it means a compatibility matrix growing
with every language and every release. Recorded so the trade is visible rather than accidental.
