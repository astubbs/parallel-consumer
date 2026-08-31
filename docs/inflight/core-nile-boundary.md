# The Nile boundary: data placement is theirs, work placement is ours

<!-- inflight-type: task -->
<!-- inflight-impact: process -->
<!-- inflight-state: deferred - a relationship hypothesis and a validation criterion, resolving the "Nile discussion" the earlier captures flagged as uncaptured -->

From the handoff supplement
([`docs/ideation/2026-08-30-hasten-handoff-supplement.md`](../ideation/2026-08-30-hasten-handoff-supplement.md),
final section, which owns the detail). Nile (tenant-native Postgres) and this project attack the
same architectural smell from opposite sides of the application boundary: logical tenants
unnecessarily coupled to physical database placement, versus logical work unnecessarily coupled
to physical Kafka/execution placement.

**The clean boundary: Nile owns data placement; this project owns work placement** - and the
compact operational form, *Nile controls supply; Hasten controls demand*. The deep-integration
hypothesis is a joint loop: Nile exposes tenant/database capacity and placement as resource
signals ([`core-shared-execution-resources.md`](core-shared-execution-resources.md)); admission
and placement respect them before overload manifests as latency; and Prescience exposes committed
future demand so Nile can place and scale *before* the workload arrives. This is also a real test
of the resource model: dependencies must not all be dumb fixed-capacity numbers - some are
sophisticated elastic substrates making their own placement decisions, and the contract has to
accommodate a counterparty with agency.

**The validation criterion, which generalises well beyond Nile:** *don't replace the specialist
system - remove the generic distributed execution machinery it should never have needed to
build.* The deletable class around a system like Nile is durable jobs and ownership, retries and
unresolved work, per-tenant ordering, rate limits, failover, quiescence, admission and execution
observability - never the storage engine, consistency, isolation or query intelligence. If a
sophisticated specialist can keep its differentiated core and delete meaningful bespoke
orchestration in favour of this substrate, that is unusually strong evidence for the thesis -
the same test [`process-csid-repo-archaeology.md`](process-csid-repo-archaeology.md) applies to a
future JMS layer, aimed at a live counterparty instead of an old repo.
