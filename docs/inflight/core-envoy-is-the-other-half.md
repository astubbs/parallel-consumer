# Envoy: the sweep missed it, and the answer is probably "run both"

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-state: deferred - needs the Envoy-shaped research the sweep failed to do; nothing is scheduled by recording it -->

Two things, and the first one is an admission.

## The 2026-09-05 sweep did not cover Envoy at all

The brief named Envoy's global rate limiting **twice**, in the claim text and again in search angle
(b). The sweep returned **zero findings and zero sources mentioning it**, while covering Doorman, the
2007 Distributed Rate Limiting paper and Gubernator in that same angle. Nobody noticed until the
owner asked why Envoy appeared nowhere.

**That is this repository's own named failure mode**, verbatim: *a check that reports success without
having run*, and *silence from an instrument that could not have spoken is not evidence* - see
[`../solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`](../solutions/workflow-issues/a-check-that-reports-success-without-having-run.md)
and
[`../solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md`](../solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md).
A sweep that names a system in its brief and returns nothing about it has not surveyed that system;
it has produced a gap shaped exactly like a clean result. **The register's Envoy position is
therefore "not searched", not "not found"**, and it is the highest-value item in
[`process-prior-art-research-targets.md`](process-prior-art-research-targets.md) because the corpus
already treats Envoy as the reference shape.

It matters more than an ordinary miss because **three existing notes already describe the design in
Envoy's terms** - [`core-non-kafka-participants.md`](core-non-kafka-participants.md) says delegated
credits *are* "the Envoy shape (local bucket, slow-cadence global sync)",
[`core-standalone-deployment.md`](core-standalone-deployment.md) says the caller spends credit
locally "the way the Envoy shape requires", and
[`core-runtime-services-and-compat.md`](core-runtime-services-and-compat.md) proposes Envoy/xDS
projections outright. So the nearest comparator for the capacity-lease question was named inside our
own corpus and still went unexamined.

## "So is Hasten just an embedded Envoy?" - the question to answer, not dodge

It is the sharpest challenge available and it will be asked in public, so the answer must be
measured rather than asserted. Same for the sibling forms: *is it just an embedded Netflix
concurrency-limiter?*, and *does Ray already do this?*

**Envoy is the network dimension. Hasten is the work dimension.** The owner's formulation, and
**dimension is the load-bearing word - not layer.** Layers stack, which smuggles in an ordering and
invites the question of which sits on top; these are **orthogonal axes of the same problem space**.
A system has coordinates on both independently, neither contains the other, and moving along one
does not move you along the other. That is also why the composition is genuinely complementary
rather than diplomatic: orthogonal axes carry information the other cannot derive.

Worth noting that this is the second time the same correction has been made in this corpus, by the
same person, on a different project: `docs/inflight-vision.md`'s law 2 says a Git ref is **a
dimension the graph is observed through, not a node inside it**, for exactly the same reason - a node
is an entry in a structure, a dimension is an axis the structure is read along. Two projects, one
instinct, and both times the weaker word had already been written down.

Unpacked along each axis: a mesh sees a **call that already exists** and decides whether to let it leave. By then the
work has been created, a thread is committed to it, and the only levers left are shed, delay or fail.
The work dimension sits earlier - a record durably in a log that nobody has spent anything on yet, which
can be left un-dispatched at **zero cost**, because ownership and execution were decoupled upstream.
Not admitting a record does not stall its partition; not admitting a request means somebody is
already blocked holding it. That asymmetry is why the layers cannot substitute for each other in
either direction, and why neither is a special case of the other.

**What must NOT be asserted until measured**, and each is queued as a research target:

- **The performance ceiling comparison.** Nobody here has measured either side. A claim about a
  ceiling relative to a proxy's model is a benchmark result, not an argument, and this repository's
  own rules on measurement discipline apply.
- **Whether Ray does global rate limiting in this sense**, and what happens to its coordination model
  at very large adaptive worker counts. Unknown here; the sweep returned effectively nothing on Ray.
- **Whether Netflix's concurrency-limits is a fair characterisation of the local half.** It is an
  adaptive limiter, so the honest form of the question is what the *global* half adds over it, which
  is the same question Doorman already answers for its own shape.

## The synergy thesis - the owner's reading, recorded as a direction

**Run both. The two dimensions feed each other, and the feeding is mutual rather than one-way.**

The network dimension knows what is happening to calls in flight - latency, saturation, error rates at
the callee - and can shed or limit at the egress. It cannot know what work *exists but has not
started*, because at that point the work is not on the network yet. The work dimension sees exactly that
and nothing about the wire. The composition is therefore not competitive:
the mesh's measured downstream pressure is an input to admission decisions about undispatched work,
and the runtime's knowledge of pending demand is an input the mesh has no way to obtain. Each one's
scheduling and analysis output improves the other's.

That framing also matches the market position the corpus already takes: the primary audience is
teams who already run Kafka and did not set out to adopt a scheduler, and many of them already run a
mesh. **"Add this beside what you have" is a far cheaper ask than "replace your mesh"**, and it is
consistent with the rule that pulling people off an existing system is explicitly not the strategy.

## What Envoy already has that bears directly on this design

Named by the owner 2026-09-05, `claimed` and unverified here, and both are more useful as
**validation and as reading** than as competition:

- **The adaptive concurrency filter's gradient controller** - periodically measures request latency
  and minRTT and recalculates a concurrency limit. That is the same perturb-observe-infer loop as
  this engine's adaptive concurrency, which already ports Gradient2 from Netflix's
  concurrency-limits ([`core-auto-scaling.md`](core-auto-scaling.md)). **Study it closely rather than
  reinvent blindly** - it is a second independent production instantiation at a different operating
  point.
- **RLQS, the rate limit quota service** - independent evidence that a **delegated-credit resource
  plane is a sensible architecture for high-performance distributed quotas**. That is a more useful
  kind of prior art than a priority claim: it says the shape is validated rather than that somebody
  got there first. [`core-distributed-throttling.md`](core-distributed-throttling.md) now cites it in
  the resource-plane design, as instructed.

**Both strengthen the run-both reading rather than weakening it.** A design whose two hardest
mechanisms have each been independently arrived at by a widely deployed proxy is a design whose
mechanisms work; what remains distinctive is the dimension it operates on and the position it
occupies, which is exactly what the register says survives.

## Why deferred

The interesting half needs the Envoy research that has not happened, and asserting a synergy before
understanding the other side's model is how a plausible-sounding wrong claim enters the corpus.
Nothing is scheduled by recording this.
