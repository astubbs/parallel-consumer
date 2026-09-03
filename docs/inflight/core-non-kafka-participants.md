# Non-Kafka participants: sell the tokens, and buy the eyes

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - ideas only; the grant-semantics question and the navigator's coordination rung both sit in front of any build -->

From a ce-brainstorm session, 2026-09-01, on the owner's idea of exposing the navigator's credits
over HTTP. It **takes the product decision** that
[`core-fleet-capacity-coordination.md`](core-fleet-capacity-coordination.md)'s claim 2 recorded as
explicitly not taken, and records the mechanisms that claim never named.

The same session's continuation asks the inverse question - whether the runtime itself has to sit
inside anyone's process at all - and
[`core-standalone-deployment.md`](core-standalone-deployment.md) owns that. It reads the four
mechanisms below as an ordered adoption ramp, and it is where the credit-vending surface stops being
a side door and becomes the product's only way to act.

## Why it is a correctness argument, not a market-widener

The system's claim is that it moves capacity toward the current global constraint. **If a third of
the calls to a shared service arrive from things the scheduler cannot see, every envelope it learns
is wrong, every bottleneck attribution is wrong, and the FinOps projection is fiction.** Coverage of
the non-Kafka half is a precondition for the claim already made, not an expansion of it. Rate
limiters, resource optimisers and Kafka runtimes are three disjoint product sets today; the
intersection is empty, and it is where this sits.

**Owner's rulings from the session, each overriding something on the record:**

- **The "so it's a service mesh? an APM? FinOps?" framing is not treated as a risk here.** Claim 2
  records it as one; the ruling is that there is nothing wrong with being a service mesh.
- **We are entering the rate-limiter market, deliberately, with a little foot.** Not as a business,
  as a reason to be reached for: it is already in the tool belt, it comes free with the Kafka client,
  and *you do not even need to wait*.
- **Partial coverage beats none, and this is not perfection-gated.** Some measurement of external
  systems through the function runtimes is a lot better than none. The absence of total capture is
  not an argument against capture.

## The competitive line, and why it is defensible

Every distributed limiter pays a network round trip per permit unless it batches - the
[distributed-throttling ideation](../ideation/2026-08-17-distributed-throttling-ideation.html)
established this against bucket4j's whole backend family, and concluded any integration "must take
the Envoy shape (local bucket, slow-cadence global sync)". Delegated credits *are* that shape
already, so the acquire is a local memory read. The same doc records resilience4j's maintainers
treating distributed rate limiting as out of scope - "differentiation, not duplication".

**The pitch is one sentence: it is right there in your tool belt, and the acquire never waits.**

## Four ways in, ordered by where the value lands

The variable that decides coverage is who has to do something. Ordered as the owner chose: the
demo leads, the coverage engine follows.

1. **Vend credits to external callers** over the sidecar's connection - the observable moment and
   the sellable sentence. Exact accounting and real control, for whoever integrates. Two client
   shapes share it: one that *holds* credit and spends locally, and one that holds nothing and is
   **paced** - the section below on the other customer.
2. **Ingest what monitoring already knows** - the firehose. Rather than convincing fifty teams to
   adopt something, convince one to give us a socket onto the telemetry they already run. Broad
   coverage at near-zero integration cost, at whatever fidelity their metrics happen to carry;
   observation only, no restraint. **This is the same move as the CDC-of-an-existing-corporate-Postgres
   scenario in [`core-runtime-services-and-compat.md`](core-runtime-services-and-compat.md) -
   "you do not move your source of truth into the runtime; the runtime turns the source of truth you
   already have into execution policy" - pointed at observability instead of policy.** It also pays a
   debt for free: granted credits versus scraped call counts *is* the declared-vs-observed recall
   measurement the risks register demands.
3. **Intercept transparently** - something on the classpath, a hook into the HTTP client, or reading
   the proxy logs. Per-call truth without application code, adopted by a platform team rather than by
   each application. This is the mechanism claim 2 originally imagined ("they all used the company's
   standard HTTP and database libraries").
4. **Answer a protocol existing gateways already speak**, so their infrastructure becomes a client by
   configuration. [`core-runtime-services-and-compat.md`](core-runtime-services-and-compat.md)'s
   "compatibility is a distribution strategy" pointed at rate limiting. Highest coverage per unit of
   effort and a one-way door on semantics, so it waits on the grant question below.

## The machinery already exists, and it is not the navigator's

The load-bearing finding of the session. The language-proxy plan
(`docs/plans/2026-08-12-001-feat-language-proxy-plan.md`, astubbs#383's stack) already specifies a
credit ledger for handing scarce supply to connected foreign clients:
<!-- file-refs: N/A - the language-proxy plan lives on astubbs#383's branch stack, not on this one; cite it there -->

- **KD9** - flow control is demand-driven and "credit rides on the acknowledgement"; a client asks as
  it frees capacity and the sidecar "sends at most what has been asked for and never more". Settled,
  and chosen over push-with-advertised-capacity.
- **R23** - a request may travel on the same message as a result report, so a client holding one
  outstanding request receives its next unit with no extra exchange.
- **KTD6** - "a credit arriving on a worker connection wakes the control loop rather than waiting for
  the next tick".
- **KTD9** - scarce supply is allocated "round-robin across connections holding credit, resuming from
  the last served connection", explicitly so one client cannot absorb what its peers asked for.

**A token broker is that ledger pointed at a different scarce supply**: divide scarce rate credits
among waiting callers instead of scarce records among waiting workers. Fair allocation across
everyone waiting is already solved there. This is what makes the exposure marginal cost rather than
a build, which is the test
[`core-internal-machinery-as-features.md`](core-internal-machinery-as-features.md) requires - and it
keeps that note's litmus test from firing, since no new distributed coordination mechanism is
introduced.

## A caller that is waiting is the demand signal

**The asymmetry this dissolves:** what makes the allocator qualitatively better than a generic
limiter is that it reports *useful* demand rather than queue depth - the payments-wants-5000-Stripe-
permits-but-only-800-are-executable example in
[`core-shared-execution-resources.md`](core-shared-execution-resources.md). An external caller cannot
supply that. It knows nothing about its own executable opportunity; it only knows it wants a token.
So the obvious fear is that non-Kafka participants are second-class citizens in the allocator,
competing on raw appetite while Kafka work competes on convertible demand.

**A caller blocked with outstanding credit answers this by construction.** It is not a forecast, an
advertised figure or a queue length - all of which can lie. It is a client that has real work, right
now, that will proceed the instant capacity arrives. That is *demonstrated convertible demand*,
which is exactly the quantity the allocator wants and the strongest form of it anything can supply.
The external participant turns out to report better demand than a generic limiter's client ever
could, without being asked to model anything.

And the transport for it already exists: a bidirectional stream with outstanding credit is
push-when-ready with no polling and no round trip. **"Long polling" is the wrong word for what this
is** - it describes the semantics from the caller's side, not the mechanism.

## The other customer: paced, not limited

Added 2026-09-02, from the owner. The persona above reaches for us because they know they need a
rate limiter. **A second persona never thinks that sentence.** They think *"I need more throughput"*
- and, in the owner's framing, they are not handed credits at all. They register work, and they
receive a **trigger**: *go now*. They never read a body, because to them it is not a business record;
it is a technical signal. Speed-limit sign versus pacemaker. In house terms: **the scheduler's dispatch
loop extended to a foreign caller.**

Why it is the same mechanism, and not a second build: **it is the language-proxy's worker protocol
with the payload removed.** The sidecar sends *records* to foreign workers under credit-on-ack flow
control (KD9, above); this sends *permission* to foreign callers under the same. The per-record
delivery token the sidecar's protocol already carries is exactly what a trigger carries - identity,
nothing else. The body is not unread because it is uninteresting; it is unread because **the delivery
is the message.**

What differs, and it is mechanical:

- **This client has no local spend, so the grant dial does not apply to it.** It is batch-of-one by
  construction, delivered by push, and overshoot is impossible because there is nothing to overshoot
  with. Throughput comes from **pipelining** outstanding registrations, not from delegation: the go
  for item *k+1* arrives while item *k* runs, which is how "the acquire never waits" survives for a
  client that owns no credit. **Batch-of-one now has a customer** - it did not when the dial was first
  written down, and the correction is worth recording: an earlier draft of this session put the
  throughput persona at the *delegation* end, which was backwards.
- **They still owe one verb back.** A client that never says *done* leaves the flow control with no
  feedback and the scheduler pacing blind, learning no cost. So the minimum protocol is two verbs -
  `next` and `done` - and R23 already says those ride the same message. That is the whole client.
- **Being thin has a price, and it is liveness.** A credit-holding client keeps going on what it has
  when the shard owner is unreachable; a paced client *stops*. That is the dependency it accepted in
  exchange for owning no semantics, and the endpoint's availability is therefore part of the product
  for this persona in a way it is not for the other.

The pacer's schedule has a shape of its own - staggered division, in
[`core-distributed-throttling.md`](core-distributed-throttling.md) - and this persona gets it for
free, because being told *when* is the only thing it ever asked for.

**"Participate in global scheduling" is what they get, not what they ask for.** Nobody asks to
participate in anything; they ask why they are slow, or how hard they can push. Coverage of the
non-Kafka half arrives as a byproduct - the same shape as the firehose in mechanism 2.

## Consult a gate, or be driven

The competitive line above was written against rate limiters. The paced persona compares us against
something else - **adaptive-concurrency libraries**, and the reference is Netflix's concurrency-limits,
whose Gradient2 astubbs#333 already ported as the local controller.

How that library is used, so the comparison is concrete: wrap a call site. `acquire` returns an
optional listener; empty means *shed now* (return 429 / UNAVAILABLE), present means do the work and
report `onSuccess` / `onDropped` / `onIgnore`. The limit is learned from round-trip times of
successful calls - TCP congestion control applied to concurrency. Adapters for gRPC interceptors, a
servlet filter and an executor wrapper; a blocking variant waits for a slot instead of rejecting; a
partitioned variant gives named groups guaranteed shares. It is **entirely local**: every process
discovers its own ceiling by probing and backing off, and never learns what anyone else is doing.

**So the shape distinction is consult-a-gate versus be-driven.** bucket4j and concurrency-limits are
both things you *ask* - "may I?" - and the answer is now, yes or no. Nothing in that set drives the
caller. The paced client does not ask; it is told.

The honest positioning, settled with the owner on 2026-09-02, is **"the next layer up", not "more
powerful"**:

- Hasten adds what a local library structurally cannot: a *named* resource shared across processes;
  *telling* a caller its allocation rather than making it probe; distinguishing "downstream
  saturated" from "I am slow"; holding and pacing rather than shedding; accounting across
  applications; lineage on the decision. [`core-auto-scaling.md`](core-auto-scaling.md) already
  states the relationship exactly - *the controller consumes ceilings as inputs*. Concurrency-limits
  is what one instance discovers alone; this is what happens when the ceilings stop being discovered
  and start being agreed.
- **The cost is smaller than the word "coordination" implies, and the owner's correction here is the
  point.** The coordination plane is embedded and sharded, failover follows Kafka ownership, and there
  is nothing separate to run. Latency-wise: a credit-holding client makes *zero* hops on the hot path,
  identical to concurrency-limits; a paced client makes one hop, pipelined - identical to any remote
  call it already makes. What is genuinely left is that a non-Kafka caller gains one endpoint to
  reach, which every non-local limiter also costs; concurrency-limits is the *only* option in the set
  with no dependency at all, and a team that only needs to stop hammering one downstream is right to
  pick it.
- **The failure mode of the global half is the local library.** By dimension 1's design - it ships
  with no fleet layer and treats ceilings as inputs when they arrive - an embedded participant that
  loses the shard owner degrades to exactly what concurrency-limits does. The floor is the
  conventional limiter; the ceiling is the global view. That holds for embedded participants only;
  the paced client's floor is *stop*, as above.
- **And the claim is untested, by our own rule.** The falsification staircase's first rung in
  [`core-lighthouse-mvp.md`](core-lighthouse-mvp.md) is literally *a local admission A/B against a
  conventional limiter*. "Beats concurrency-limits" is the precise sentence that rung exists to check,
  and the risks register's frictionless-derivation warning is about sentences that arrive this
  easily. Recorded here as the hypothesis rung 1 tests, not as a positioning line.

## What is deliberately not decided

- **What a grant promises** - a hard ceiling versus bounded overshoot. The owner's position is that
  the foundation comes first and all sorts of things can be built on top of it. Note that the vending
  surface offers a dial the in-process navigator does not have: a batch of one is an authoritative
  per-request grant, and a batch of N is delegation with overshoot proportional to N. The
  hard-ceiling half stays inside the ring-fence the risks register put around it. That dial turns
  out to decide something larger too - see the embedded-not-cluster section of
  [`core-standalone-deployment.md`](core-standalone-deployment.md). And the paced persona above
  sits at the batch-of-one end by construction, so the dial is now a choice between two customers,
  not a semantics question with no one on one side of it. A third axis arrived 2026-09-03: a grant
  may carry a **phase** - staggered division, owned by
  [`core-distributed-throttling.md`](core-distributed-throttling.md) - which is a property of the
  resource contract rather than of the caller.
- **Anything about which telemetry systems, which protocol, or what the endpoint looks like.**

## One collision to avoid

`STRATEGY.md` already mentions "candidates like an HTTP endpoint server" under its Flexibility
section. **That is inbound - a way to feed records in for processing - and is the opposite direction
from anything here.** A reader who greps for an HTTP endpoint will find it first.
