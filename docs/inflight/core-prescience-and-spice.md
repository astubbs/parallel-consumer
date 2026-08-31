# Prescience and Spice: the admission index over the committed backlog

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - the lookahead half of the admission model; scoped by "if it cannot affect admission, selection or explanation, do not index it" -->

From the 2026-08-30 exchange (model root:
[`core-admission-scheduling-model.md`](core-admission-scheduling-model.md), codename glossary
there). Prescience is not a caching feature bolted on - it follows from the admission insight: if
you decide what to dispatch instead of blindly dispatching and blocking, knowing more of the
available work improves the decision, and the natural limit is knowing all of it.

## Spice: the record's scheduling silhouette

Every record carries two layers: **execution envelope + business payload**, and the payload stays
cold. The envelope (Spice) is only what affects eligibility, selection or explanation: ordering
identity, resource IDs, priority/QoS, not-before/deadline, estimated cost, causal metadata. Two
compressions keep it tiny: resource *names* become dictionary IDs (the catalog travels separately),
and better still a record can carry just `executionContractId + parameters` ("contract 17, tenant
42") with the contract defining the resource template. The boundary that keeps this safe and
general: **the engine understands what work needs, never what work means** - no business
awareness, so the Java and Python producers only agree on the envelope, and everything global
works identically across languages. (2026-08-31 addition: one semantic extractor should feed
both Spice and lineage/OTel export, with *knowing* and *exporting* as separate first-class
visibility policies - [`core-decision-lineage.md`](core-decision-lineage.md) carries the privacy
warning the CSID whitelists taught.)

## The index, and where this fork has unfair prior expertise

Partition-local postings lists - `resource-id -> outstanding offsets`,
`ordering-domain -> outstanding offsets` - compress extremely well because offsets are monotonic:
delta coding, varints, Roaring-style containers. **That is literally the offset-encoding
machinery this project already ships and measured** (astubbs#306's DeltaList and the encoder
benchmark) applied to a new index. The knowledge hierarchy: record metadata -> partition
Prescience (exact offsets) -> app demand -> cluster -> global (counts, weighted demand, deadlines
only). 100% Prescience never means one centralised index - exact locally, aggregate globally.

## Open implementation question (owner, 2026-08-31): fetch keys and headers without bodies?

The wish: build Prescience by reading only keys + headers off the broker. No such fetch exists -
Kafka's fetch protocol returns whole (often compressed) record batches with no server-side
projection - so the honest emulations are client-side: drop bodies after decode (saves memory,
not network), or drop selectively once memory pressure crosses a threshold. The trap the owner
already named: rehydration then needs random seeks back to dropped offsets, which in the worst
case is a disaster (re-fetching whole batches for one body, fetch-session churn). The
architectural fix for the *network* half is already on the table below: head/body externalization
puts the decision surface in the control record so full-body fetch is never needed for indexing.
Verdict to record: body-dropping is a memory-tier tactic inside the Store, not a bandwidth
strategy; the bandwidth strategy is Spice/externalization.

## The reframing that names it (2026-08-31): an inverted EXECUTION index, not a cache

A cache accelerates retrieving something whose identity you already know; **an index creates new
access paths** - and Prescience exists so the runtime can discover candidate work by semantic
property: which work is eligible, blocked, shares a domain, depends on this state, becomes
runnable if this resource grows. The search-engine analogy is load-bearing: Lucene does not
reorganise source documents per query - it builds inverted indexes. Likewise the engine never
reorganises topics into priority/retry/DLQ/per-tenant queues; it keeps the immutable log and
builds `priority/critical -> offsets`, `resource/salesforce -> offsets`,
`eligible-before/T -> offsets`, and selection is intersection. The operational vocabulary maps
wholesale (segments, immutable generations, incremental indexing, hot postings in RAM, cold on
NVMe, index generation = routing epoch = fencing), so 100% Prescience never implies one
monolithic structure. The one-line definition: **Kafka lets you read the log by physical
position; Prescience indexes the log by execution meaning.** And the mature decision surface
holds more than records - state versions, continuations, timers, leases, barriers - "how much of
the committed future execution state can the engine reason about", with record coverage as the
first measurable form. Lineage ([`core-decision-lineage.md`](core-decision-lineage.md)) is the
same construction pointed backward: derived semantic indexes over durable facts, one of possible
futures, one of realised pasts.

## The supplement's formalization - and the standalone-feature claim

The handoff supplement (sections 11-17) firms this up in five ways, detail there:

- **The Store is separate from the execution shards.** Flow: log -> Prescience Store (complete,
  compressed, derived, disposable - rebuild from Kafka after loss) -> scheduler selection ->
  execution shards (the hot hydrated subset) -> workers. Never grow the PC buffer enormous;
  tier the store instead (compressed RAM -> mmap/NVMe -> embedded LSM behind an abstraction).
- **Minimal Spice is just `ordering key -> outstanding offsets`** - which alone yields domain
  depth, heads, and independent-work counts, making **100% Prescience a plausible standalone
  Parallel Consumer feature today**, before any of the rest of the runtime exists. At that point
  PC is a semantic queue over Kafka on its own.
- **`targetPrescience(100%)` as an elastic runtime SLO**: extend the local store, then add nodes
  for *knowledge* capacity (not CPU), then spread ownership - and if partition count prevents
  distribution, partition count has become a knowledge-distribution constraint
  ([`core-partition-virtualization.md`](core-partition-virtualization.md) closes that loop).
- **Physical coverage is not semantic Prescience.** A **No-record** is indexed but semantically
  opaque work; a **No-ship** is an opaque execution boundary (a legacy ERP) behind which nothing
  can be seen. The model states where knowledge ends instead of implying omniscience - report
  both numbers. **Spice density** (useful scheduling information per byte) is the quality metric.
- **Two open designs for large payloads**, both kept until experiments pick one: a two-layer
  index (payload untouched, hydrate on selection) versus transparent head/body externalization
  (which gives large-message support a second purpose: Prescience density). Thresholds adaptive,
  never hard-coded.

## Producer declarations create a feedback loop, and topology extends the horizon

Declared demand can be checked against observed usage: "this class declares {db} but consumed
stripe in 94% of executions" -> flag the mismatch, suggest the contract edit (agent-shaped
maintenance; humans approve the semantics). Envelopes can also mature from binary needs-R to
weighted vectors ({R:3.2, CPU:0.7}) learned from execution - at which point selection becomes
bin-packing the buffet against the capacity profile, and the system stops merely *limiting*
demand and starts **shaping** it (safe here precisely because the work is durable - reordering
within ordering constraints, not reshaping live traffic). And with KS stages carrying contracts
(astubbs#271), demand propagates through the topology: future downstream demand derived before
the intermediate records exist - Prescience of the causal future, not just the committed one.
