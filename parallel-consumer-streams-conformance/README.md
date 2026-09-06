# Parallel Consumer - Streams Conformance

A **test-only** conformance net for the Kafka Streams foreign bindings (astubbs#242). It holds no
main sources and produces no artifact anybody may use: **nothing downstream may depend on this
module**, and its pom skips deploy, install, signing and publishing so nothing accidentally can.

What it is for: a corpus of language-neutral cases (YAML, so a foreign driver can read the same
file), an oracle that computes each case's final state with plain Kafka Streams under
`TopologyTestDriver`, and the proofs that run every case through the oracle - determinism, a
positive control on a perturbed twin, a coverage gate over the builder surface, and the guards that
retire themselves when an engine arrives. It needs no broker and no container, so it runs in the
no-Docker unit lane.

The plan that specifies it, unit by unit, is
`docs/plans/2026-09-05-001-test-streams-conformance-net-plan.md`.
