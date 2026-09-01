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
   the sellable sentence. Exact accounting and real control, for whoever integrates.
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

## What is deliberately not decided

- **What a grant promises** - a hard ceiling versus bounded overshoot. The owner's position is that
  the foundation comes first and all sorts of things can be built on top of it. Note that the vending
  surface offers a dial the in-process navigator does not have: a batch of one is an authoritative
  per-request grant, and a batch of N is delegation with overshoot proportional to N. The
  hard-ceiling half stays inside the ring-fence the risks register put around it. That dial turns
  out to decide something larger too - see the embedded-not-cluster section of
  [`core-standalone-deployment.md`](core-standalone-deployment.md).
- **Anything about which telemetry systems, which protocol, or what the endpoint looks like.**

## One collision to avoid

`STRATEGY.md` already mentions "candidates like an HTTP endpoint server" under its Flexibility
section. **That is inbound - a way to feed records in for processing - and is the opposite direction
from anything here.** A reader who greps for an HTTP endpoint will find it first.
