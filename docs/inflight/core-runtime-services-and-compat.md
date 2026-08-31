# Runtime services at the processing edge, and compatibility as a distribution strategy

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - caught from the handover document (sections 34, 38, 41), absent from the transcript excerpts -->

From the compound-engineering handoff
([`docs/ideation/2026-08-29-hasten-compound-engineering-handoff.md`](../ideation/2026-08-29-hasten-compound-engineering-handoff.md),
which owns the detail). The rule over the whole cluster: *you write a normal Kafka Streams
application; the runtime turns it into an operationally complete distributed application* - and
**if Kafka/Streams already knows the answer, the developer is never asked to configure it again.**

## Distributed Interactive Queries as a first-class runtime service

Streams materializes fault-tolerant local state but leaves remote IQ routing/transport to the
application. The runtime already needs discovery, ownership routing and polyglot RPC - so ship
distributed IQ out of the box: local-owner execution, direct routing to remote owners,
replica-aware standby reads under an explicit consistency policy, the same store/query surface in
every language binding, and the dashboard consuming the *same API* (no privileged UI backend).
Plus a live catalog of Streams applications, topologies, stores, owners and restore state - one
discoverable state fabric from separately deployed JVMs. Reuse ruling from the owner's checklist
(2026-08-31): KS standby replicas serve double duty - they are the read replicas that spread IQ
load, **and the hot-failover mechanism is Kafka Streams' own, used directly rather than
reimplemented** - the same do-not-rewrite-the-specialist instinct, applied to Streams itself.

## Semantic health, not process health

Readiness should mean *this instance can correctly serve the work and state it currently owns*,
not that the JVM answers HTTP. The runtime can inspect what no generic framework can - the sparse
completion frontier, deadlock probes, restore progress, scheduler progress - and distinguish
alive / assigned / ready / restoring / draining / fenced / making-progress /
unresolved-frontier-pressure, projected onto conventional Kubernetes probes. **This directly
extends astubbs#226's health surface** and is a candidate answer to the stall-signal question
that PR deliberately left open (astubbs#157): the admission model's states are exactly the
vocabulary "stuck vs working" was missing.

## Embedded compatibility facades - "compatibility is a distribution strategy"

Where the runtime already materializes the authoritative state behind an established
infrastructure API, expose the facade instead of requiring another server tier: a Schema
Registry-compatible local cache/API (Confluent SR, Karapace and Apicurio are named prior art -
no novelty claim, and licence review before reuse), a Consul-compatible discovery projection over
the catalog the runtime keeps anyway, and Envoy/xDS projections where existing gateways can
consume its routing/policy - with the line worth keeping: *Istio meshes network connections; this
meshes execution.* These are [`core-ecosystem-adapters.md`](core-ecosystem-adapters.md)'s move
pointed at infrastructure APIs instead of application frameworks, and
[`core-internal-machinery-as-features.md`](core-internal-machinery-as-features.md)'s filter
applies unchanged. Explicit cautions kept: no new proxy or security system for architectural
symmetry; wrapper insertion stays boring (wrap clients developers already use, discover safe
facts); no speculative edge-authority abstractions.

## CDC to automatically served distributed read models

Database -> CDC topics -> discovered schemas -> generated/augmented Streams materialization ->
active/standby stores -> distributed IQ -> polyglot typed clients: declare which tables to
mirror, and the runtime supplies the operational apparatus around the materialization. The
product-language precision the handoff insists on: these are *Streams-backed materialized read
models with explicit consistency semantics*, never "database replicas". The supplement's dogfood
scenario is the sharpest form: an existing corporate Postgres of service/resource policies,
mirrored via CDC into live ResourceContracts - *you do not move your source of truth into the
runtime; the runtime turns the source of truth you already have into execution policy*. Valuable precisely
because the resulting state lives inside the execution fabric - queryable, actor-associable,
lineage-participating, observed beside the work that updates it.

Also swept in from the handoff's cron section: **backfill and bounded replay through the same
runtime** - select a topic/range/key/time/version scope and let the normal scheduler execute the
resulting work under production QoS and resource constraints. Corrected by the GitHub Codex review, 2026-08-31:
records below the committed frontier cannot be re-delivered by an eligibility change - replay
needs its own *acquisition* (a separate position/group), stable replay identities, and an
explicit duplicate-effect boundary; seeking the live group backward would disturb commits and
mix replay with current work. Acquisition is distinct from eligibility
([`core-work-identity-model.md`](core-work-identity-model.md)'s replay-as-new-incarnation is the
identity model for it; [`core-scheduled-intent.md`](core-scheduled-intent.md) owns the
eligibility half).
