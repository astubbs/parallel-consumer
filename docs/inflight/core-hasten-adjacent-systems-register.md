# Hasten adjacent systems: which claims are occupied, and by whom

<!-- inflight-type: register -->
<!-- inflight-impact: misdirection -->

The living view over what already exists in the engine's space. Sibling of
[`ci-inflight-adjacent-systems-register.md`](ci-inflight-adjacent-systems-register.md), same
contract and the same three evidence marks, different question set - because the questions that
discriminate here are about scheduling, not about knowledge.

**It is a register because it is consulted, never completed.** The first sweep ran 2026-09-05 and
[`docs/plans/2026-09-05-001-investigate-hasten-prior-art-sweep.md`](../plans/2026-09-05-001-investigate-hasten-prior-art-sweep.md)
**owns those findings** verbatim, including the ten claims that failed verification and the two
citation defects that must be fixed before anything is published. This note owns what changes from
here, and never restates a finding that document holds.

## Why it exists, and what it cost to not have it

[`core-engine-thesis.md`](core-engine-thesis.md) already carried the instruction - a single external
model's novelty search, *"recorded as a lead and not a survey; do the prior-art sweep before any
public 'first' claim"* - and it went unexecuted while the corpus grew. `docs/w2-vision.md`'s risks
register names the same hazard from the other side as **frictionless-derivation bias**: a medium with
no adversarial pressure, where the derived half accumulates detail and typographic confidence without
a falsifier ever running. The sweep is that falsifier, and it landed on the derived half.

## The evidence rule

Identical to the InFlight register's, and it is the reason a row can be trusted:

- **verified** - run, or read from primary source, by someone here, with the document cited.
- **surveyed** - read from the system's own published sources by a sweep, but not run.
- **claimed** - asserted and not checked anywhere. A lead.

The 2026-09-05 sweep additionally used **adversarial 3-vote verification**: each extracted claim went
to three independent verifiers and needed two refutals to be killed. Ten were killed. Rows below
carry the vote where one exists, because a 2-1 is not a 3-0.

## The question set - ask every system the same eight

1. **What does it schedule** - requests, queries, jobs, records, packets, invocations?
2. **What does it own**, and what does it merely observe?
3. **Where does it sit relative to the dispatch boundary** - above it, at it, or under it?
4. **Does the application have to be rewritten** into its model?
5. **Is there a cluster, a server, or a database in the per-call hot path?**
6. **Is capacity delegated, borrowed, or arbitrated centrally** - and on what substrate?
7. **What happens when the global half is unreachable** - stall, degrade, or over-admit?
8. **Does it reason about work it has not dispatched yet**, and from declared requirements or
   reactive signals?

Question 5 is the one that has so far separated this design from every durable-execution runtime;
question 8 is the one nothing found answers the way this design proposes to.

## The verdict per claim, after one sweep

| Claim | Status | Occupied by |
|---|---|---|
| **1. Admission is a scheduling state, not an execution state** | **REFUTED as novel** (3-0 x4) | Kueue, CockroachDB, Impala, Restate. Only the scoping clause survives: none admit records already durable in an external log somebody else owns |
| **2. Delegated capacity leases, no cluster, no per-call permit server** | **PARTIALLY REFUTED** | Doorman occupies the mechanism outright - renewable time-bounded leases to an embedded client that decides in-process. Residual: **the durable log as coordination substrate**, which Doorman (server tree + etcd), DRL (UDP gossip), Kueue (API server) and DBOS (Postgres) each miss on a different axis |
| **3. Global intelligence, local execution** | **REFUTED as novel**, most thoroughly of the set | Impala, Doorman, and SIGCOMM 2007 Distributed Rate Limiting - three decades, three layers |
| **Conservation-law safety bias** | **WEAKENED, not refuted** | Doorman makes it a per-client configuration option and ships an explicitly contract-violating *optimistic* mode; DRL's shipped designs bias the other way |
| **4. Prescience - backlog indexed by declared requirements** | **NO DIRECT HIT** | Nothing found. But requirement *declaration* is heavily occupied (Slurm GRES, AWS Batch consumable resources, Kueue requests, US 7,813,276) - the unoccupied part is only the **index over the committed backlog** plus horizon and feasibility reasoning |
| **5. The composite** | **NO DIRECT HIT**, medium confidence | Pieces occupied at different layers with incompatible topologies; synthesized rather than directly verified |
| **No rewrite, nothing in the per-call hot path** | **DIFFERENTIATOR CONFIRMED** (3-0 x3) | Restate and DBOS both fail it - Restate pushes rules cluster-side, DBOS hits Postgres per dequeue |

## What may never be claimed as novel

The register's most-used section, by the same logic as its InFlight sibling. Each is disprovable on
sight by anyone who knows the field, and the credibility cost of being corrected on your own novelty
claim exceeds anything the claim buys:

*Pre-execution admission control. Queueing work before it starts. Globally coordinated capacity with
locally decided dispatch. Graceful degradation when the coordinator is unreachable. Renewable
capacity leases delegated to an embedded client library. Divisible or borrowable quota across named
pools. Predeclared resource requirements. Embedded-library-not-cluster packaging.*

## What survives, stated narrowly enough to be attacked

1. **The substrate combination** - divisible capacity leases carried on a *durable log* to embedded
   instances, with no coordination cluster and nothing per-call in the hot path. A combination
   claim, not a mechanism claim, and the mechanism half is Doorman's.
2. **Prescience** - an inverted index over the committed backlog keyed by producer-declared execution
   requirements, with demand and capacity horizons, admission debt and feasibility.
3. **The composite**, resting on integration and on the two residuals above, never on any individual
   mechanism - every mechanism examined turned out to be occupied.
4. **Under the existing dispatch boundary, no application rewrite, nothing in the hot path** - the
   only differentiator the sweep positively confirmed rather than failed to refute.

## The caveat that governs how much of this to believe

**The sweep's coverage is uneven, and the hole is in the nearest neighbourhood.** Angles (a)
admission control and (b) distributed rate limiting are well covered by primary sources. Angle (c)
durable execution covered only Restate and DBOS - Temporal, Cadence, Conductor, Inngest and Azure
Durable Functions produced no surviving verified claims. **Angle (d), stream-processing elasticity,
produced nothing at all**: no verified claim touches Flink's reactive or adaptive scheduler or its
autoscaler, Kafka Streams, Beam/Dataflow, Pulsar Functions, KEDA, or Kafka Share Groups (KIP-932).
Angle (e) produced no research literature and no commercial capacity-governance hits.

Since (d) is the closest adjacent domain to a Kafka-resident runtime, **treating claims 4 and 5 as
"gaps" is premature - they are "not found", which is a much weaker statement.** That distinction is
the same one
[`../solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md`](../solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md)
owns, and it applies to this register's own output.

## The next sweep

In priority order, and the first is not optional before any public claim:

1. **Angle (d), properly** - Flink adaptive scheduler and autoscaler, Kafka Streams, Beam/Dataflow,
   KEDA, Pulsar Functions, KIP-932 Share Groups. Does anything there decouple ordering from
   concurrency, or schedule against a committed backlog? This is where claims 4 and 5 live or die.
2. **The remaining durable-execution runtimes** - Temporal, Cadence, Conductor, Inngest, Azure
   Durable Functions - against question 5, since that is the confirmed differentiator and it is
   currently confirmed against two systems out of seven.
3. **Log-transported quota** - Kafka's own quota and throttling internals, service-mesh rate-limit
   services with log-backed state, Raft-backed token brokers. The residual of claim 2 is a substrate
   choice and only four alternatives were checked.
4. **Scheduling-theory literature** on backlog-aware and requirement-aware lookahead, which was never
   searched.
5. **Gubernator** - all three claims about it failed verification, so the register holds no reliable
   position on the closest *distributed rate limiter without a central service* comparator.
6. **Escrow and reservation protocols, distributed-counter literature** - for whether the
   conservation-law contract has a formal precedent. Both DRL and Doorman leave the strict-safety
   corner explicitly open.

## What this changes about the thesis

- **2026-09-05, first sweep.** Two claims dropped as novelty arguments and one narrowed to its
  substrate. **The engineering is unharmed and arguably validated** - four independent teams
  converging on the same shape is evidence the shape is right - but the *pitch* has to change, and
  Doorman's design document is now available as a free design review of claim 2, including the lease
  expiry semantics, refresh intervals and unreachable-server fallbacks it already settled. Two notes
  carry claims this contradicts and are corrected in the same change as this register lands:
  [`core-admission-scheduling-model.md`](core-admission-scheduling-model.md) and
  [`core-distributed-throttling.md`](core-distributed-throttling.md). `docs/w2-vision.md`'s law 3
  gains a pointer rather than a rewrite, because the law is still the right law - it is only no
  longer *ours*.
