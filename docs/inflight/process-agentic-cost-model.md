# The agentic cost model: price conceptual complexity, not code

<!-- inflight-type: task -->
<!-- inflight-impact: process -->
<!-- inflight-state: deferred - a standing planning heuristic from the handoff supplement, recorded so estimates stop using the wrong unit -->

From the handoff supplement
([`docs/ideation/2026-08-30-hasten-handoff-supplement.md`](../ideation/2026-08-30-hasten-handoff-supplement.md),
section 27). The expensive unit is no longer lines of code or binding count - it is **unresolved
architectural and semantic decisions**. The pipeline that follows: architecture -> specification
-> invariants -> executable conformance tests/oracles -> agent implementation -> review ->
empirical validation. Once semantics are precise, agents absorb the mechanical breadth (SDKs,
generated RPC surfaces, compatibility facades, wrapper operators, lighthouse services, docs,
conformance matrices) - and throwaway implementations become cheap enough that competing designs
should often be *built and benchmarked* rather than debated.

What stays expensive regardless: distributed correctness and failure semantics, API/protocol
compatibility commitments, production validation and elapsed operational experience, security
boundaries, performance claims needing representative workloads, and complexity imposed on users.

The heuristic: **do not ask first how much code a feature requires; ask how much new conceptual
complexity it introduces.** A large mechanical feature reusing existing primitives may be cheap;
a small feature with a new consistency model or user concept stays expensive. This favours the
architecture exactly to the degree the breadth genuinely collapses onto the small hard core
(ownership, ordering domains, eligibility, Prescience, scheduling, resources, durable state,
causal identity, control authority) - which is also why the
[`core-lighthouse-mvp.md`](core-lighthouse-mvp.md) falsifiers matter: if the collapse is
illusory, this cost model stops applying. It matches the owner's standing rule that agent effort
is not a scarce resource - the scarce resource is maintainer attention on semantics, and this
names where that attention goes.
