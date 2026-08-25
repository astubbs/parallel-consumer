# HTTP strategy: the priority decision the ideation left open (astubbs#242)

The HTTP-strategy ideation pass has run - the third continuation (ideas 14-20) of
[`../ideation/2026-08-14-language-proxy-interaction-model-ideation.html`](../ideation/2026-08-14-language-proxy-interaction-model-ideation.html)
- and [`next-http-strategy-ideas.md`](next-http-strategy-ideas.md) points at it. Read the doc for the
ideas; this note tracks only what stays open after it.

- **Priority is undecided**: the REST Proxy v2 compat surface (idea 14: cheapest reach, zero new
  clients, feasibility settled compatible-and-beneficial) vs the native dialect's prerequisite
  (idea 16: the session-scoped lease re-spec). Demand decides, and nobody has asked for either yet.
- **Escalate regardless of the HTTP decision** (idea 20): `KafkaClientFactory` applies no key
  allowlist to client-supplied `kafka_properties` - verified live 2026-08-17; class-valued
  properties instantiate reflectively in the sidecar JVM. KTD41 resolved KTD11's binary-location
  gap but not this one.
- **Owed before any browser-reachable surface**: the observation-plane security write-up
  (CORS, origin rules, DNS rebinding) that idea 18 defers.
