# Next: a standing comparison of PC against the systems that solved the same problems

A document comparing Parallel Consumer with the projects that have already crossed the same
boundaries, so this project can check itself against them instead of rediscovering their answers.
Wanted as a permanent reference, not a one-off: the flow-control / in-flight-accounting seam has
produced four consecutive review rounds where each fix caused the next round's defect, and every one
of those rounds would have been shortened by knowing what Beam or Temporal already decided.

## Why it is worth writing down

Research during the language-proxy ideation (astubbs#242) surfaced that the hardest questions on that
branch are all solved problems elsewhere, with published rationale:

- **Apache Beam's portability framework / Fn API** — the closest analogue in existence. Runner owns
  scheduling and state; a per-language SDK harness runs user code; gRPC between them on split
  control/data/state/logging planes. The runner is the *server* and the harness *dials out*,
  deliberately, because runners often sit where they cannot accept inbound connections. One harness
  fans out internally — threads for Java and Go, one subprocess per core for Python because of the
  GIL. Work travels as bundles, and **any element failing discards and retries the whole bundle**;
  per-item outcomes were considered and deliberately rejected as not worth the complexity.
- **Temporal** — replaced N full-logic SDKs with a shared Rust core plus thin bindings, explicitly to
  stop reimplementing hard protocol logic per language. Note the limit, which is easy to overstate:
  the **Go and Java SDKs are independent implementations**, not core bindings; only Python,
  TypeScript, .NET and Ruby wrap the core.
- **Ray** — worker pool lifecycle owned entirely by a node-local daemon; the language driver never
  spawns workers, because spawn/reap/orphan semantics are where the per-language bugs live.
- **Envoy `ext_proc`** — one bidirectional stream per request, with the *external processor*
  controlling how much it receives. Inverts the usual assumption that the host dictates granularity.
- **Bytewax** (Rust core, Python over in-process FFI, no IPC at all) and **Quix Streams** (pure
  Python, no polyglot split) — the two baselines either side of a protocol boundary.
- **Dask** — batches assigned to the least-busy worker, batch size an explicit tunable trading
  submission overhead against idle-start latency.
- **Kafka Share Groups (KIP-932)** — the nearest competitor for the same use case, and the reason
  PC's "no processing clock" property matters: Share Groups' acquisition-lock timeout genuinely is a
  redelivery clock, and PC's absence of one is a stated differentiator.

## What the document should answer, per system

Who owns scheduling; what crosses the language boundary and what never does; the unit of work;
how worker death is detected and what it costs; how thin the per-language client is; how ordering is
preserved; and where PC deliberately differs rather than accidentally.

## The second half: competitive positioning

Two landscapes, and PC sits differently in each:

- **Kafka clients generally** — key-based ordering with concurrency beyond partition count, breadth
  of client languages, latency, retry and offset semantics.
- **Message-processing systems generally** — output modelling, batching, delivery guarantees, and
  what a user has to give up to get each.

## Not started

No file written yet. This note exists so the research is not lost — it was gathered during the
astubbs#242 ideation and is currently only in that session's artifact
(`docs/ideation/2026-08-14-language-proxy-interaction-model-ideation.html`), which is scoped to the
interaction model and will not carry the comparison as it grows.
