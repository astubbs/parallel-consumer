# Prior-art research targets - what is queued, and what each would settle

<!-- inflight-type: register -->
<!-- inflight-impact: blind-spot -->

**One place to see every outstanding prior-art question across both projects**, because they were
otherwise scattered across two registers' tail sections where nobody reads them until they are
already mid-investigation. The two registers own the *findings*; this owns the **queue**, and each
row says what the answer would change - a target that settles nothing does not belong here.

- [`core-hasten-adjacent-systems-register.md`](core-hasten-adjacent-systems-register.md) - the engine.
- [`ci-inflight-adjacent-systems-register.md`](ci-inflight-adjacent-systems-register.md) - the harness tooling.

**The sibling that asks the other question:**
[`process-open-research-questions.md`](process-open-research-questions.md) holds what *we* believe and
have not checked - unfalsified claims, constants that were reasoned rather than measured, and
decisions nothing is scheduled to resolve. This file is answered by reading other people's work; that
one by running something here.

**Why this is a register and not a task:** it is consulted before starting an investigation, never
completed. New questions arrive faster than old ones close.

## Hasten - highest value first

- **DONE 2026-09-05: Uber uForwarder.** Searched against source, IDL, issues and blog posts;
  [`core-hasten-adjacent-systems-register.md`](core-hasten-adjacent-systems-register.md) owns the
  result. It disproved four claims and confirmed the position axis. **Its own citations are the next
  targets:** Confluent's Kafka REST Proxy and Kafka Connect, both explicitly considered and rejected
  in Uber's 2021 write-up - Connect over rebalance latency against a hard end-to-end latency
  requirement, which is a measured argument somebody else already made about a system we also compare
  against.
- **The systems uForwarder never mentions**, which is its own signal: SQS, RabbitMQ, Pulsar shared
  subscriptions, and Parallel Consumer itself. *Settles:* whether the queue-semantics-over-a-log
  family has an answer nobody in this space cites - Pulsar shared subscriptions especially, since
  that is a broker offering the thing Kafka lacks.
- **DONE 2026-09-05: Envoy**, both mechanisms, against its documentation, protos and source.
  [`core-envoy-is-the-other-half.md`](core-envoy-is-the-other-half.md) owns the result: the
  discriminating clause answers **no**, we are ahead on the controller variant, RLQS validates the
  delegated shape, and a co-deployment interference risk surfaced that neither project's docs
  address. **Left open there:** what the RLQS server's allocation policy actually does, which is
  unknowable from public sources since no open-source server exists.
- **The performance-ceiling comparison against a mesh's model.** A benchmark, not an argument -
  nobody here has measured either side. *Settles:* whether the placement advantage is also a
  throughput advantage, or only an adoption-cost one. Do not assert either way before it runs.
- **Ray** - does it do global rate limiting in this sense, and what happens to its coordination model
  at very large adaptive worker counts? The sweep returned effectively nothing. *Settles:* a
  frequently-asked comparison that currently has no evidenced answer.
- **Netflix concurrency-limits** as a characterisation of the local half. *Settles:* the sibling
  challenge - *is this just an embedded adaptive limiter* - by making explicit what the global half
  adds, which is the same question Doorman answers for its own shape.
- **"Systems that embed scheduling or admission beneath an existing API"** - the claimed family
  includes workflow runtimes, actor systems, mesh-style control planes, database and language
  runtimes and proxies. **One name in the source of that claim, "ARCORIS", could not be identified
  here at all and may not exist** - treat the whole list as unverified until each is checked
  individually. *Settles:* whether the placement argument is as distinctive as it currently reads,
  and specifically whether any of them do **adaptive global optimisation from measured performance**,
  which is the discriminating clause.

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
- **Arktos Global Scheduler** - reported to be built around a global view across clusters and data
  centres, with application-aware scaling and migration driven by observed input-flow behaviour and
  multidimensional optimisation. *Settles:* whether the global-view-plus-measured-behaviour
  combination is already occupied at cluster scale, on a non-Kafka substrate.
- **IBM's LLA work** - reported to continuously adapt distributed CPU and network allocation to
  workload and resource variation, maximising aggregate utility from end-to-end latency. *Settles:*
  whether the adaptive-global-optimisation clause has a mature academic answer, and what objective
  function it optimises - utility-from-latency is a stronger formulation than anything recorded here.
- **Hadar and the online-scheduling literature** - task placement across heterogeneous accelerators
  from measured or modelled workload performance. *Settles:* how far the measured-performance-driven
  placement idea has already been taken mathematically.
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

## Substrate neutrality - a standing rule for every brief from 2026-09-05

**The unit of comparison is the execution architecture, never the transport.** If somebody built
this shape under RPC, actors, work queues, a database, generic async work, or across clusters and
data centres, that is **exactly as relevant** as a Kafka project - and the first sweep's angles were
implicitly Kafka-shaped, which is one reason it missed things.

The searchable form of the question, stated without naming a substrate: *does anything place an
admission or scheduling decision beneath an existing API or programming model, and drive it from
adaptive global optimisation over measured performance?* The final clause is the discriminating one.
Most systems have some of it - a limiter, a quota, a scheduler - and far fewer close the loop from
observed behaviour back into a global allocation.

## Classify, do not score

For every candidate in either project, the output is a **classification** and not a verdict:
**competitor · substrate · integration · historical prior art · something to join or build upon.**
A row carrying only "occupied" or "not occupied" has thrown away the half that decides what to do
next - which is the whole reason the InFlight register's join candidates are useful and its
occupied rows are merely interesting.

## The rule these all run under

A sweep's verdict language is the instrument's, not ours. **Adversarial framing - "try to disprove
this" - is how a search is made to work hard; it is not a record of a belief that was held and
lost.** Both registers state this; it is repeated here because this note is where somebody
commissions the next search, and the brief they write is where the framing error would enter.

Second rule, from the InFlight sweeps: **the gap will be a family nobody thought to name, not a
product inside a family already listed.** The first InFlight sweep scoped itself to agent knowledge
graphs and returned tidy and wrong; the correction came from the owner asking about distributed issue
trackers. Budget a question for *which neighbourhood is missing* before spending the search.
