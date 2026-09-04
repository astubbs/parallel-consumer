# Prior-art research targets - what is queued, and what each would settle

<!-- inflight-type: register -->
<!-- inflight-impact: blind-spot -->

**One place to see every outstanding prior-art question across both projects**, because they were
otherwise scattered across two registers' tail sections where nobody reads them until they are
already mid-investigation. The two registers own the *findings*; this owns the **queue**, and each
row says what the answer would change - a target that settles nothing does not belong here.

- [`core-hasten-adjacent-systems-register.md`](core-hasten-adjacent-systems-register.md) - the engine.
- [`ci-inflight-adjacent-systems-register.md`](ci-inflight-adjacent-systems-register.md) - the harness tooling.

**Why this is a register and not a task:** it is consulted before starting an investigation, never
completed. New questions arrive faster than old ones close.

## Hasten - highest value first

- **Stream-processing elasticity, the whole angle.** Flink's reactive and adaptive schedulers and its
  autoscaler, Kafka Streams, Beam/Dataflow autoscaling, Pulsar Functions, KEDA, Kafka Share Groups
  (KIP-932). **The 2026-09-05 sweep returned nothing at all here**, and it is the nearest neighbour
  to a Kafka-resident runtime. *Settles:* whether Prescience and the composite are genuinely
  unanswered or merely unsearched - currently they are only *not found*, which is much weaker than it
  reads. Nothing public should assert either until this runs.
- **The rest of the durable-execution field** - Temporal, Cadence, Conductor, Inngest, Azure Durable
  Functions - against one question: is there a server or database in the per-call hot path, and must
  the application be rewritten? *Settles:* how strong the one positively-confirmed difference really
  is; it currently rests on two systems out of seven.
- **Log-transported quota.** Kafka's own quota and throttling internals, service-mesh rate-limit
  services with log-backed state, Raft-backed token brokers. *Settles:* whether the substrate
  residual of the capacity-lease question is real; only four alternatives have been checked.
- **Scheduling-theory literature** on backlog-aware and requirement-aware lookahead admission.
  Never searched. *Settles:* whether Prescience has an academic precedent that a product search
  cannot see.
- **Gubernator.** All three claims about it failed verification, so there is no reliable position on
  the closest *distributed rate limiter without a central service* comparator. *Settles:* a hole in
  the capacity-lease row.
- **Escrow and reservation protocols, distributed-counter literature.** *Settles:* whether the
  conservation-law contract has a formal precedent - both Doorman and DRL explicitly leave that
  corner open.

## InFlight - and two of these are conversations, not searches

- **agent-memory** - read the federation release properly, then **ask its maintainer** whether they
  would treat all active refs as simultaneously queryable knowledge including conflicting versions,
  rather than requiring promotion into a curated store. *Settles:* whether building separately is the
  wrong move entirely.
- **ctxpipe** - source archaeology on what repo ingestion actually means: HEAD, history, branches, PR
  heads, forks? Divergence preserved or collapsed? A README cannot answer it. *Settles:* whether the
  federated-context half is already occupied.
- **Engram** - whether it would move from *memories scoped to branches* to *branches as dimensions*.
  Possibly a short conceptual distance. *Settles:* the same question from the branch-insight side.
- **Backlog.md winner-selection archaeology** - how deeply is canonical-state semantics baked into
  loading, indexing and querying? *Settles:* a build decision - fork it as the shell, or take only
  the ideas.
- **Atlassian Teamwork Graph** - the watch item and the fastest-staling row; Code Context is in open
  beta. *Settles:* whether anything there retains simultaneously divergent repository knowledge.
- **Atomic's design documents and Radicle's COB internals** - read before inventing view identity,
  provenance or replicated-object semantics. *Settles:* nothing about competition; avoids
  re-deriving solved machinery.
- **Graphiti/Zep, A-MEM, MAGMA** - untouched memory-research leads. *Settles:* whether any treats
  concurrent versions as simultaneously true rather than as history to collapse.

## The rule these all run under

A sweep's verdict language is the instrument's, not ours. **Adversarial framing - "try to disprove
this" - is how a search is made to work hard; it is not a record of a belief that was held and
lost.** Both registers state this; it is repeated here because this note is where somebody
commissions the next search, and the brief they write is where the framing error would enter.

Second rule, from the InFlight sweeps: **the gap will be a family nobody thought to name, not a
product inside a family already listed.** The first InFlight sweep scoped itself to agent knowledge
graphs and returned tidy and wrong; the correction came from the owner asking about distributed issue
trackers. Budget a question for *which neighbourhood is missing* before spending the search.
