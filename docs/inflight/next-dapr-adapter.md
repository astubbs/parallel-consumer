# Next: Dapr adapter — ideation done, probe is the first move

<!-- inflight-type: feature -->


> Extracted from `origin/docs/ideate-dapr-adapter` @d5c8cf1b0, `docs/inflight/next-dapr-adapter.md`.

A full ideation pass on the Dapr adapter (the pluggable pub/sub component backed by this engine,
raised in the HTTP-strategy notes) ran on 2026-08-17. **The ranked result, its evidence, and the
rejection record live in
[`docs/ideation/2026-08-17-dapr-adapter-ideation.html`](../ideation/2026-08-17-dapr-adapter-ideation.html)**
— read that before re-deriving any of this. Related issue: astubbs#242 (mirror of
confluentinc#154). The earlier seed docs (`next-study-dapr-and-kafka-proxies.md`,
`next-http-strategy-ideas.md` §3) sit on branch `docs/proxy-http-ideation`, not yet on master.

What the ideation settled that the seed docs left open:

- **The contract unknown mostly dissolves on paper.** Dapr's pluggable pub/sub proto acks
  per-message, out of order — structurally the Dispatch/Report cycle. Ordering and the in-flight
  ceiling are admission-side and never need wire expression, same as our own v1.
- **The deployment unknown has a clean answer**: the component container *is* the engine's JVM
  host, talking to daprd over UDS — Dapr's own sanctioned topology, so no "two sidecars".
- **The one live unknown**: whether daprd serializes component→app delivery (dapr/dapr#5946
  suggests its built-in Kafka component does; a pluggable component replaces that code path, so
  possibly moot). Only a measurement probe answers this — that probe is the ideation's top-ranked
  first move, and its verdict is a three-way triage: build as-is / build + patch daprd upstream /
  no-go.
- **If daprd needs patching, the fork-patch model from the Kafka Streams work applies**: tracked
  patch applied to pinned upstream sources at build time, upstream-first PR in parallel, drop the
  patch when it releases. Precedent:
  `docs/solutions/architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md`
  (astubbs#271, issue astubbs#255) with mechanics in `parallel-consumer-streams/bin/apply-patch.sh`
  on `feats/ks-on-pc-spike`.

Decision pending: none blocking — the probe is unblocked, cheap, and gates everything else.
Per the expansion rule (demand decides), the full component build waits on a named ask; the probe
and the STRATEGY.md ordered-concurrency framing are the bank-now parts.
