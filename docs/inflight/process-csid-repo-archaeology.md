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
