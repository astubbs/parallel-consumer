# Docs from an executable progression: the diffs between example stages ARE the curriculum

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - builds on the industry-grounded examples and the docs site, neither merged -->

From the follow-up Codex strategy conversation, 2026-08-29 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)).

Structure the flagship example as a numbered progression of runnable stages (the sketch used the
parcel-logistics domain astubbs#266's core example already established), where each stage adds one
concept. Then the learning path is not authored - **it is encoded in the transitions**: an agent
diffs stage 05 -> 06 and generates "previously our parcels shared customs capacity globally; now
we guarantee capacity for medical shipments", then explains the five lines that changed.

One executable progression, many generated presentations: the 5-minute quickstart, the 30-minute
tutorial, the manual chapter, the workshop, the blog series, the conference talk, the interactive
site - and the audience-relative entries, "coming from KafkaConsumer / Kafka Streams / Python",
which are just different starting stages on the same ladder. All grounded in one running
application, so a presentation can never drift from code that compiles.

**The supplement upgrades the example to a company: Hasten Logistics LLC** ("Why wait? We
deliver."). The binding rules: *if the product claims it, Hasten Logistics uses it*; **no feature
demo - one company**; a new idea that cannot compose into the same estate has its kernel
membership challenged. The estate is simultaneously tutorial, integration test, polyglot
conformance suite, chaos environment, performance playground, documentation source, onboarding
path and conference demo. And **no language island**: every supported language contributes a
*meaningful* component playing to its ecosystem (Python route optimization, Go customs, Rust
CPU-bound work, a Haskell effect-typed RPC surface, an Elixir/BEAM-native actor), with
cross-language call *paths* (Java -> Python -> Go) rather than hub-and-spoke Java-to-everything -
that is what proves neutrality is architectural. The CDC dogfood also lives here: a corporate
PostgreSQL the runtime emphatically does not own, mirrored into live resource policies
([`core-runtime-services-and-compat.md`](core-runtime-services-and-compat.md)).

**The handoff document carries the worked curriculum** - a 13-stage composable tutorial stack
(consumer -> Streams -> buffet -> named resources -> delegated semaphore -> QoS -> observability
-> scaling -> workflow -> actors -> infrastructure -> economics -> global system), the logistics
domain-mapping table, and two binding rules this note adopts: *each stage imports the previous
and adds only the new capability*, and the architectural law that **the canonical example must
compose** - a capability needing a new conceptual universe should have its scope challenged.
Detail lives there:
[`docs/ideation/2026-08-29-hasten-compound-engineering-handoff.md`](../ideation/2026-08-29-hasten-compound-engineering-handoff.md), section 4.

This generalises machinery the repo already trusts: the README is generated from tagged compiled
source precisely so prose cannot drift from code, and astubbs#266's examples exist to demonstrate
capability rather than API mechanics. The docs site ([`docs-site.md`](docs-site.md), astubbs#208)
is the rendering surface; agent generation is what makes maintaining seven presentations of one
progression affordable.

## Relationship to existing demo work - reconcile, do not add a fourth artefact

Three demo artefacts already exist, and the progression must compose with them rather than becoming
their sibling: the **uber demo** (astubbs#332, collapsed into astubbs#331's `feats/polyglot-demos` -
every language client on one workload: breadth), the **three-reveal demo**
([`web-three-reveal-demo.md`](web-three-reveal-demo.md) - one topology, one climbing line: depth),
and the **realistic-domain Streams benchmark** (`test/ks-streams-realistic-domain-benchmark`, one of
the sibling branches [`branch-ks-streams-workstream.md`](branch-ks-streams-workstream.md)
signposts - a business-shaped workload for the Streams work). The natural reconciliation: the
progression's domain IS the realistic domain, its later stages ARE the three reveals, and any stage
is a candidate workload for the uber demo's language matrix - one application, staged, presented
many ways, rather than four demo codebases drifting apart.
