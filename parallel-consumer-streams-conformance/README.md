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

## How to run

```bash
./mvnw -pl parallel-consumer-streams-conformance -am test
```

`-am` is not optional - `docs/building.md` owns why - and no Docker is needed: every proof runs in the
no-Docker unit lane.

The corpus itself is documented beside it, in
[`src/test/resources/cases/README.md`](src/test/resources/cases/README.md).

## What is deferred

This rung measures **no binding**: it evidences the oracle and the net around it, at final state,
under `TopologyTestDriver`, at the reactor's one pinned Kafka version, with no broker row. Everything
the driver rung inherits - the wrapper dependency and its binding row, the call-log slot, the
update-stream observable, the refusal-class cases, the reconciliation against the wrapper's proto, and
the one question left open for the owner - is written down once, in
[`docs/inflight/test-streams-conformance-driver-rung-obligations.md`](../docs/inflight/test-streams-conformance-driver-rung-obligations.md).
It is not restated here, so there is nothing to drift.

**Nothing downstream may depend on this module**, and nothing can: it has no main sources, and its pom
skips deploy, install, signing and publishing. A dependency on it would be a dependency on a test
fixture that is free to change shape with every case added.
