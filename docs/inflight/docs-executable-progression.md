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

This generalises machinery the repo already trusts: the README is generated from tagged compiled
source precisely so prose cannot drift from code, and astubbs#266's examples exist to demonstrate
capability rather than API mechanics. The docs site ([`docs-site.md`](docs-site.md), astubbs#208)
is the rendering surface; agent generation is what makes maintaining seven presentations of one
progression affordable.

## Relationship to existing demo work - reconcile, do not add a fourth artefact

Three demo artefacts already exist, and the progression must compose with them rather than becoming
their sibling: the **uber demo** (astubbs#332, collapsed into astubbs#331's `feats/polyglot-demos` -
eleven language clients, one workload: breadth), the **three-reveal demo**
([`web-three-reveal-demo.md`](web-three-reveal-demo.md) - one topology, one climbing line: depth),
and the **realistic-domain Streams benchmark** (`test/ks-streams-realistic-domain-benchmark`, one of
the sibling branches [`branch-ks-streams-workstream.md`](branch-ks-streams-workstream.md)
signposts - a business-shaped workload for the Streams work). The natural reconciliation: the
progression's domain IS the realistic domain, its later stages ARE the three reveals, and any stage
is a candidate workload for the uber demo's language matrix - one application, staged, presented
many ways, rather than four demo codebases drifting apart.
