# Envoy: the sweep missed it, and the answer is probably "run both"

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-state: deferred - needs the Envoy-shaped research the sweep failed to do; nothing is scheduled by recording it -->

Two things, and the first one is an admission.

## Searched 2026-09-05, and the discriminating question answers NO

The earlier sweep named Envoy twice in its brief and returned **zero findings and zero sources** on
it - this repository's own named failure mode, *a check that reports success without having run*. A
dedicated pass has now closed it, against Envoy's own documentation, protos and source.

**The result is better than expected.** Both mechanisms are real and relevant, and they validate
**different halves** of this design - but **Envoy never joins them**:

- **Adaptive concurrency is adaptive but strictly local.** One controller per process, shared across
  worker threads by mutex and atomics, with **no cross-instance channel at all**. The fleet-level
  coordination is a `jitter` field whose job is *do not all probe at once* - not *share what you
  learned*. N proxies produce an emergent uncoordinated global load with no global optimum computed
  anywhere.
- **RLQS is global but its upward signal is demand, never performance.** The usage report carries
  requests allowed, requests denied and time elapsed. No latency, no service time, no saturation - so
  a server *cannot* do performance-driven allocation over that protocol without an out-of-band
  signal.
- **The minRTT the gradient controller discovers is published nowhere, and RLQS never reads it.**

So: **nothing in Envoy closes the loop from measured service time to global allocation**, which is
precisely the composition this design claims. Envoy defines an interface where one could live and
implements none of it. **Envoy is not prior art against the composition.**

One caveat that must travel with that: **Envoy ships no RLQS server** - the docs say the open-source
reference implementation is unavailable, and the extension is documented as usable with Google
Cloud's. The allocation policy, the only place global optimisation could live, is out of tree and
unreadable. So *"RLQS does global optimisation"* is **unproven either way**, and whatever it does is
driven by demand rather than performance.

## "So is Hasten just an embedded Envoy?" - the question to answer, not dodge

It is the sharpest challenge available and it will be asked in public, so the answer must be
measured rather than asserted. Same for the sibling forms: *is it just an embedded Netflix
concurrency-limiter?*, and *does Ray already do this?*

**Envoy is the network dimension. Hasten is the work dimension.** The owner's formulation, and
**dimension is the load-bearing word - not layer.**

**Sharpened by the 2026-09-05 research, and this is the better discriminator.** The distinction is
not really the OSI layer - it is **the cost of not-yet-admitting**. Envoy's caller is present and
waiting, so declining costs a 503 and the decision must be immediate and probabilistic. A record
here is durable in a log with nobody waiting, so declining costs **nothing** and the decision can be
a *schedule* rather than a rejection. **That is why Envoy has shedders and this can have a
scheduler.** Every Envoy mechanism found - adaptive concurrency, admission control, RLS, RLQS,
circuit breaking, the overload manager - terminates an *existing* request; the nearest thing to
acting before work exists is `max_pending_requests`, which still holds a live caller. "Dimension,
not layer" survives intact; *network versus work* is the weaker phrasing of it. Layers stack, which smuggles in an ordering and
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

## What to take from Envoy rather than reinvent - and one place we are ahead

Researched 2026-09-05 from Envoy's documentation, protos and source; `surveyed` rather than run.

**Where we are AHEAD, which inverts the earlier direction on this note.** Envoy's filter implements
Netflix's **Gradient** - the minRTT-probing variant - while this engine already ports **Gradient2**.
Netflix's own README argues Gradient2 exists *precisely because* minimum-latency measurement suffers
bias and drift, replacing the probe with the divergence of a long and a short exponential average. So
the instruction is not *adopt Envoy's controller*; it is **read Netflix's argument for abandoning
minRTT probing before ever committing to a probe** - and that matters more here than for a proxy,
because probing means deliberately underloading **real customer work**.

**What to steal anyway:**

- **The probing hazard's full mitigation set**, which is the expensive part to rediscover: jitter
  across instances, the pin to minimum concurrency, discarding samples on entering the window, the
  forced re-probe after consecutive minimum windows, and both historical bugs - in-flight requests
  from before the pin skewing the sample, and two threads updating minRTT back to back.
- **The stability constants of a controller that actually shipped**: the gradient clamp, a
  square-root headroom term so the limit probes upward instead of sitting at a fixed point, a buffer
  on minRTT, and **percentile aggregation rather than a mean**.
- **RLQS's degraded-mode vocabulary, which is ready-made lease semantics** - assignment TTL,
  behaviour when no assignment was ever received, behaviour when an assignment expired, and an
  abandon action that purges an idle participant and re-initialises it on next use. All four
  concepts are needed here and the state machine is already worked out.
- **Deriving the governed identity from live attributes** rather than declaring it up front, which
  is directly applicable if a named shared resource should be *discovered* rather than configured.
- **A success-rate controller beside a latency controller** - Envoy's admission control filter uses
  the Google SRE client-side throttling formula, for the case where a resource is *failing* rather
  than merely *slow*.
- **RLS as the thing not to build.** Envoy's own trajectory from a per-request permit call to
  delegated quota is the argument for skipping that generation entirely.

**What RLQS validates, and the three ways it differs.** It is fair independent confirmation that a
serious project reached the same conclusion - advance assignment, local spend, no per-request permit
call, periodic usage report, TTL, explicit degraded-mode behaviour. The differences matter though:
what RLQS delegates is a **rate**, not a stock of credit to draw down; its bucket is *requests
matching these matchers*, not a named external resource with real capacity shared by unrelated
participants; and the number vended is one **a human configured**, never one discovered. Its
partition behaviour is also configuration-selected rather than solved - both over- and
under-admission are structurally reachable, and there is no lease return, no fencing token and no
reconciliation of what was actually spent.

**Both strengthen the run-both reading rather than weakening it.** A design whose two hardest
mechanisms have each been independently arrived at by a widely deployed proxy is a design whose
mechanisms work; what remains distinctive is the dimension it operates on and the position it
occupies, which is exactly what the register says survives.

## The risk the synergy story hides - co-deployment interference

**Surfaced by the 2026-09-05 research and addressed by neither project's documentation.** A consumer
calling a downstream API *through* an Envoy sidecar puts **two independent controllers on one
resource**, and they are coupled the wrong way round:

> Envoy probes by underloading and sheds with 503s -> this engine reads those 503s as downstream
> degradation and reduces its own concurrency -> offered load falls -> Envoy's `sampleRTT` falls ->
> Envoy raises its limit.

Two loops, different periods, no shared state, each reacting to the other's actuation as if it were
the environment. **That is a real hazard for the run-both reading**, and it is the first concrete
thing the composition would have to solve rather than assert. It also suggests what the integration
actually is: not two schedulers politely coexisting, but one of them **telling the other what it
did** - which is the mutual feeding the section above claims, stated as a requirement rather than a
hope.

## Why deferred

The interesting half needs the Envoy research that has not happened, and asserting a synergy before
understanding the other side's model is how a plausible-sounding wrong claim enters the corpus.
Nothing is scheduled by recording this.
