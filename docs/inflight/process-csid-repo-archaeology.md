# CSID repo archaeology: what the owner's Confluent-era projects donate, and under what terms

<!-- inflight-type: task -->
<!-- inflight-impact: process -->
<!-- inflight-state: deferred - a donor catalogue for when the receiving tracks start; nothing here is scheduled -->

From the 2026-08-31 morning conversation. Four repos, three dispositions - and a continuity
worth stating: lineage found that generic tooling does not understand events' business identity;
the JMS bridge found that applications want execution semantics partitions cannot express;
secrets-providers found that specialist systems should be surfaced through common abstractions;
PC found execution independence below the partition. The current architecture is those four
lessons meeting at the point where durable work becomes execution.

## csid-event-lineage (+ demos) - raid, do not transplant

A POC built on OTel Java instrumentation 1.13.0 - too old to lift wholesale, exactly right to
mine: its module layout is a **map of the interception surface** (Kafka clients, Streams,
Connect, common), its state-store provenance trick is the discovery that reshaped the lineage
model ([`core-decision-lineage.md`](core-decision-lineage.md) owns the consequences), and its
capture/propagation whitelists are the privacy warning. The demos repo is a ready-made lineage
lighthouse (producers, stateful Streams, joins, multiple sinks); the cheap idea kept: run one old
scenario as a regression showing OTel-only lineage beside engine execution lineage.

## csid-jms-bridge - the architecture-archaeology pile, Apache-2.0

Customer-driven evidence that richer execution semantics recur whenever anyone puts a messaging
model over Kafka - and its synthesized features map almost one-to-one onto admission-model
primitives: JMS priority -> selection policy; expiry -> deadline predicate; selectors -> semantic
eligibility; request/reply + correlation -> RPC and durable continuations; large messages ->
payload externalization; temporary destinations -> ephemeral addressing. Its milestone about
keeping references to Kafka records instead of copying messages is the payload/control separation
independently discovered. Repo-level Apache-2.0. Far-future option recorded, not planned: a JMS
compatibility layer as projections of engine primitives - another rung for
[`core-alternate-api-facades.md`](core-alternate-api-facades.md)'s ladder, and the
"specialist system deletes generic distributed machinery" test applied to a product the owner
already built the hard way.

### The deep-dive upgrade (2026-08-31): a historical requirements document

Read closely, the bridge is a catalogue of the semantic gaps that recur whenever a richer
execution model is projected onto Kafka - identity, routing, eligibility, ordering, durability,
failover, retries, transactions, control state - i.e. exactly the territory the admission model
makes generic. Its three best exhibits, each now landed on its owner: the **journal pattern**
(Kafka WAL -> Streams fold -> materialized state; ack on WAL durability; the journal is a
*recovery substrate*, not the live store - so Kafka is the durable authority from which the
runtime reconstructs whatever live structure it wants, and durability and semantic completion are
separate facts); the **startup barrier** (epoch marker + wait for the projection -
[`core-frontier-handover.md`](core-frontier-handover.md) now owns the primitive); and **HA via
consumer-group membership** replacing a filesystem lock (production evidence for "prefer deriving
coordination from ownership that already exists" - distributed primitives as projections of
durable state + exclusive ownership + epochs). Its crash-window diagrams are the effect-frontier
problem drawn by hand, and its key-extraction config is the ordering/routing/ownership coupling PC
attacks, met a project earlier. One warning kept: the bridge *embedded Artemis* rather than
reimplementing JMS - the right instinct for any future protocol surface (find the seam where the
existing model delegates generic mechanics; don't replace the specialist, delete the generic
machinery underneath it).

**Next archaeology worth running**: the old PC issue tracker and design discussions - feature
requests that felt awkward or out-of-scope at the time are likely more "the scheduler trying to
emerge before the abstraction existed". The repo already has the greppable substrate:
[`issue-index.md`](issue-index.md) and the upstream-mirror sweep.

## csid-secrets-providers - pattern donor only, licensing caution

**Do not reuse code without checking**: its README declares CSID Accelerator status with
Confluent IP/licensing restrictions - treat as prior experience unless individual files' licenses
say otherwise. The pattern it donates is the provider SPI (one stable concept, ecosystem-specific
implementations: Vault/AWS/GCP/Azure/K8s/CyberArk), which is exactly the shape the
**ResourceProvider** adapters in
[`core-shared-execution-resources.md`](core-shared-execution-resources.md) want (Postgres,
Salesforce, quota APIs, GPU schedulers each translating external state into the common capacity
vocabulary). The boundary stays firm both ways: the engine consumes existing secret providers and
never owns secret storage.
