# Decision lineage: one causal graph across work, state and effects - OTel is a projection

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - but the causal identity model should be established early, because everything else becomes a projection of it -->

From the 2026-08-31 morning conversation about the CSID-era projects
([`process-csid-repo-archaeology.md`](process-csid-repo-archaeology.md) catalogues the repos
themselves; model root: [`core-admission-scheduling-model.md`](core-admission-scheduling-model.md)).

## The claim

A trace tells you what executed. Decision lineage tells you **what could have executed, what did,
why, and what that execution caused** - "waited 4.2s because salesforce/acme had no lease, chosen
ahead of 12,000 eligible records because of deadline, executed on node N under epoch 17". Nearly
free, because the scheduler must know the answer to make the decision - the product is retaining
and projecting decisions that would otherwise vanish (the same rule already recorded in
[`core-bottleneck-attribution.md`](core-bottleneck-attribution.md): every adaptive decision keeps
its evidence). The delivery rule inherited from the CSID work: **never build a tracing backend -
emit the richer graph through OpenTelemetry**, let Jaeger/Grafana/Datadog consume ordinary
telemetry, and let the engine's own UI understand the richer attributes.

## The state-store discovery, which changes the model

The CSID lineage project was forced past message-chain tracing: for stateful Streams operations it
wrote provenance into the stored bytes and re-linked the *historical* trace when state was read -
because a join's output is caused by today's record AND the three-hour-old event that produced the
state it read. So the model is: **execution causality is a graph of work AND state**, not a chain
of messages. Nodes: work, execution attempt, state version, effect, decision, output. Edges:
consumed, read-state, wrote-state, caused, waited-for, retried-as, scheduled-because. "Why was
this shipment rejected?" traverses to the customer-update event that set `creditHold=true` months
ago. Two companion principles: *provenance is data* (a trace only in Jaeger is telemetry; a
provenance pointer stored with durable state survives process death, restoration and replay), and
the unification worth keeping verbatim: **Prescience is the graph of what may happen; lineage is
the graph of what did happen; the scheduler operates at the boundary** - Why Wait? queries it
forward, lineage queries it backward.

## Design decisions taken from the archaeology, with the rejections

- **Lineage lives on the execution object, not in ThreadLocals or agent tricks.** CSID needed
  ThreadLocal capture and ByteBuddy hooks into `PartitionGroup.nextRecord()` because Kafka has no
  execution object - and ThreadLocal breaks outright under this engine's concurrent dispatch onto
  virtual threads. The engine HAS an execution context; put identity, provenance, causal parents,
  trace context, scheduling decision and state dependencies there, and make OTel one projection.
- **Do not copy their storage.** Magic bytes prepended to serialized state values was ingenious
  for a POC and wrong as a permanent representation (mixes application state with observability
  metadata, poisons versioning). Instead: a **sidecar provenance structure keyed by
  `(store, key, state-version)`** - the same move as the sparse completion frontier, and it keeps
  changelogs un-inflated. Kafka changelog restores state; the provenance index restores causal
  meaning - payload/control separation appearing independently yet again, which is the tell that
  it is the architecture: *Kafka stays authoritative for data; the engine maintains compact
  semantic indexes over it* (Prescience, sparse completion, routing epochs, resource state,
  provenance - projections of one execution graph).
- **Causal summaries, Git-shaped.** An aggregate that consumed a million events does not carry a
  million parent IDs: a state version records its immediate causal executions, its previous
  version pointer, and its source frontier - walk backward lazily, like commits and parents. This
  is also what makes "replay everything caused by bad input X" graph traversal rather than
  topic-range replay.
- **One semantic extractor, separate visibility policies.** The same description of the work
  (business IDs, ordering identity, resources, deadline, privacy classification) feeds both Spice
  ([`core-prescience-and-spice.md`](core-prescience-and-spice.md)) and lineage/OTel export - but
  what the scheduler may *know* and what telemetry may *export* are first-class, separate
  policies. CSID's capture/propagation whitelists are the warning, not an implementation detail:
  payload values in span attributes explode privacy, PII, cardinality and cost. The engine may
  know `customerId=82374823` locally and export `tenantTier=enterprise` or a hash.

## Two consequences worth their own lines

- **Lineage is a top reason to have the KS wrapper at all, before any acceleration.** CSID's
  instrumentation had to hook PartitionGroup, StreamTask, store builders, window stores and
  deserializers just to preserve causal context - empirical evidence both for stopping the
  Streams work at its feature boundary AND for the wrapper exposing clean first-class hooks
  (record became execution; store read/wrote; output produced) instead of anyone ever
  ByteBuddy-ing package-private internals again (astubbs#255 / astubbs#271).
- **Terms from uncaptured exchanges.** This conversation leans on concepts ("the continuation is
  data", virtual records, engine RPC, selective causal rollback) from weekend exchanges that were
  not part of the captured transcript. No notes exist for them yet; if they carry weight, those
  exchanges deserve the same capture treatment before anything cites them as settled.
