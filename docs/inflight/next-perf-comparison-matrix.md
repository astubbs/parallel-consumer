# Next: the cross-language performance comparison matrix

> Extracted from `origin/docs/ideate-perf-comparison-matrix` @3dd35926a, `docs/inflight/next-perf-comparison-matrix.md`.

Candidate work, ranked in full in `docs/ideation/2026-08-17-perf-comparison-matrix-ideation.html`
(branch `docs/ideate-perf-comparison-matrix`, stacked on `feats/proxy-requirements`, no PR). The ask
it serves: the classic README intro performance test running in each proxy language - the
double-click demo wanted before astubbs#293 merges - across big-data replay, a slow processing
stage, and a Kafka Streams augment-and-republish, comparing plain consumer, plain Streams, PC core,
PC-on-Streams, and every proxy client.

**Eight ranked directions.** The top pick builds the matrix as a fifth scenario family inside the
existing conformance harness - no second orchestrator, all timing engine-side so runners measure
nothing, cells fail closed through the registry's coverage gates. The rest: adopt
`parked-testing-as-a-feature-for-the-clients.md` as the fairness charter; canonicalize the README's
single-partition keyspace sweep as the spec workload; the java-direct -> java-grpc -> foreign
attribution ladder so the sidecar hop is a published column; docs-as-data result fragments
regenerating the README charts; a live polyglot performance loop over the plan's demo commitments;
config-fingerprint/provenance/saboteur attestation; and a two-tier split with ratio-gated PR checks
and pinned-hardware headline numbers.

**Already settled - do not rediscover:**

- The fairness constitution exists: no language-vs-language ranking, publish the case we expect to
  lose, report the sidecar hop rather than engineering it out
  (`parked-testing-as-a-feature-for-the-clients.md`).
- `StreamsApp` in `parallel-consumer-examples` already implements the PC-on-Streams arm; the matrix
  has no dependency on the Kafka Streams spike branch.
- Corrected during the ideation's verification pass: the performance suite is a **required PR gate**
  in `maven.yml` today, not "excluded from CI" as widely repeated;
  `pr-highcpu-fast-feedback.yml` itself records that accurate benchmarking belongs in a separate
  isolated/on-demand run.

**Cross-track boundary, agreed 2026-08-17 with the polyglot-demo ideation track**
(`docs/ideate-polyglot-demo`, its doc `docs/ideation/2026-08-14-language-proxy-interaction-model-ideation.html`):
that track owns the demo app, UI, live loop, and marketing narrative; this track owns workload
definitions, measurement semantics, and the blessed-numbers pipeline. The shared contract is the
plan's R77 stats stream plus the scenario definitions, to be designed as **one record shape** with
that track's proposed observation-receipts topic - a live stream sample and a blessed result
fragment should be the same record at different aggregation levels. Jointly open: the exact
stats-line field list; and the receipts transport must be switchable off (or costed in the
attribution ladder) so the demo's transport cannot distort the numbers it displays.

That track's "bring-your-own-topic what-if machine" (its idea 3, added 2026-08-18) consumes this
track's scenario definitions with **user-supplied knobs** - processing delay, ordering mode,
failure percentage, max concurrency. Constraint on this track's workload schema: parameterize the
processing delay (and those siblings) rather than hardcoding the 0-5 ms slow stage, so both tracks
run one definition. It also reuses this track's plain-KafkaConsumer arm as its live comparison arm,
under the recorded constraints (fingerprinted config, ratio framing, no absolute-number claims).

**Next step.** Brainstorm idea 1 (the harness architecture - everything else attaches to it) or
idea 6 (the live loop) into requirements, then implement as a stacked PR off
`feats/proxy-requirements`.
