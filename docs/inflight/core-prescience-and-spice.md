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
