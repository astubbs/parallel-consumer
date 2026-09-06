---
title: Streams Conformance Net - Plan
type: test
date: 2026-09-05
topic: streams-conformance-net
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-brainstorm
execution: code
---

# Streams Conformance Net - Plan

## Goal Capsule

- **Objective:** the engine-independent half of a conformance net for the Kafka Streams foreign bindings (astubbs#242): a case format, a corpus over the wrapper's builder surface, a live oracle that computes each case's expected outcome by running it through plain Apache Kafka Streams, and the proofs that make the result a CI gate a maintainer can act on.
- **Product authority:** this plan owns the oracle rung only. The driver for our engine, the per-language drivers, the differential fuzz generator and a real-broker Kafka row are later rungs, described under How This Work Fits Together and not active scope here.
- **What this rung does not deliver:** confidence about any binding. That arrives at the driver rung, which is therefore the gate on reopening exactly-once across the boundary and the sidecar-versus-embedded choice - not this rung's green.
- **Open blockers:** none. Every open item is deferred to planning.

---

## Product Contract

### Summary

Define correctness for every Kafka Streams binding once, as data, and let Apache Kafka's own engine say what each case should produce - live, at test time, never hand-written.
This rung ships the case format, the first corpus, the oracle runner and the proofs that the net can say no; the first binding to be measured against it arrives with the driver rung.

### Problem Frame

The owner's question is verbatim: *how can we be confident proceeding if we aren't sure what we've done works?*
Every claim the fast-path program has made so far is a measurement, and a measurement is not a guarantee.
Two open architectural questions - exactly-once across the boundary, and sidecar versus embedded engine - cannot be settled safely without first being able to prove that a binding preserves Kafka Streams' behaviour.
What the net establishes is behavioural equivalence on deterministic, failure-free execution; it is a precondition for those decisions, not a sufficient basis for either - exactly-once in particular turns on crashes, retries and duplicate suppression, none of which a no-broker comparison exercises.

Today that proof is a hand-written Python test per feature, which does not scale to a second binding, let alone ten.
The 2026-08-25 design note (`docs/inflight/test-cross-binding-streams-conformance.md`, on the astubbs#334 branch) already settled how: reflect the scenario, not the API.
The proxy clients already run that shape (astubbs#387 and astubbs#390), so the Streams net extends a mechanism the repository runs rather than inventing one.
What was still open was the agreement contract, the size and content of the first cut, how the lowest rung earns a PR with no binding to check, and how the oracle exists at all.
<!-- file-refs: N/A - named as a file on the astubbs#334 branch (research/kafka-streams-foreign-wrappers), which this rung is cut below on master; it is cited as prior art, not as a file this plan touches -->

### Key Decisions

- **The conformance net is the next piece of work, ahead of exactly-once-across-the-boundary and the embedded-versus-sidecar choice.** It is the only one of the three that makes the other two safe to attempt. (session-settled: user-directed - chosen over exactly-once and embedded-vs-sidecar: you cannot choose an engine placement or build exactly-once across a seam you cannot prove preserves behaviour.)
- **This rung is cut off master as the lowest rung of the PR ladder, split from the driver rung.** The oracle needs only `kafka-streams` as a library; our engine lives at astubbs#334, the top of the stack. (session-settled: user-directed - chosen over one rung carrying oracle and driver together: the corpus and oracle become reviewable before any binding exists, which is astubbs#387's define-correct-once principle.) Governs R5, R9.
- **The mechanism is the 2026-08-25 note's: reflect the scenario, not the API; Kafka Streams is the source of truth; one driver per language; differential fuzzing beside it, not instead.** (session-settled: user-approved - chosen over reflecting Kafka's own test suite through the binding, which tests internals, casts to implementation classes, needs call-for-call fidelity across 59 unsupported overloads, and requires a full object-graph proxy on the return path.) Governs R1, R2.
- **The maintainer's CI gate is the primary outcome; the user-facing trust claim follows once a binding row exists.** Done for this rung is a gate that goes red on divergence, not a documentation feature. Governs R6, R11, R13.
- **Final state is the floor; update streams are compared only when a case pins a close-driven emit rule.** TopologyTestDriver commits after every processed record and so over-counts cached emissions relative to a broker; only a close-driven rule makes an update stream deterministic. Governs R3, R12.
- **The oracle is live: expected outcomes are computed from plain Kafka Streams on every run and never committed.** Chosen over version-tagged committed recordings. Nothing goes stale and there is one artifact class fewer; the accepted price is that a `kafka-streams` dependency bump that changes Kafka's own behaviour is invisible, because both sides of the comparison move together. Governs R5, R7.
- **Cut one is the case format, the corpus, the live oracle runner and the proofs - no fuzz generator, no driver.** What it delivers is a reviewable corpus and a harness proven able to say no; the confidence the Problem Frame asks for arrives with the first binding row, not with this rung. Governs R12.
- **The foreign-call-log slot is reserved in the case format and left unset by this rung; the first driver fills it.** Plain Kafka Streams has no boundary and so cannot produce a call log; hand-writing one would be exactly the expectation the trust claim avoids. The slot's line format is astubbs#390's runner transcript, which already exists for this purpose. Governs R4.
- **The rung earns its PR with a control arm, a positive control and two guards, and no Docker.** Same case twice must agree; a case's author-chosen perturbed twin must not; a binding row must be registered exactly when the wrapper's own engine is on the classpath; and every builder operation must have a case. Mirrors astubbs#387's broker-free shape, including its coverage gate. Governs R7, R8, R9, R10, R16.
- **Driver rungs are runners under astubbs#390's existing contract and registry, not a second registry.** The `RunnerContract`, `LanguageRunners`, the `pc.conformance.language` selector, `ConformanceRunnerPrebuild` and the sidecar shim are reused; a language's Streams driver costs one runner. Constrains How This Work Fits Together; nothing in this rung's requirements.

### Requirements

**The case**

- R1. A case is data, not code: a topology description, input records, a perturbation of those inputs (R8), and the expectations, with nothing in it specific to any language. Every input record carries an explicit timestamp relative to a case-level base instant that sits past the window-clamp margin rather than at the epoch, and the loader rejects a case that omits one, so no case inherits wall-clock time.
- R2. The topology description can express every operation on the wrapper's builder surface: source, mapValues, groupByKey, count, reduce, join, windowedBy, aggregate, toStream and sink.
- R3. A case declares its agreement level: final state, which is the default and comprises every state store's contents plus the final record per key on every sink topic - the close-deterministic observables - or final state plus update stream, and the latter is valid only when the case names a close-driven emit rule. A case whose final state would be empty fails at load, so a stateless case cannot pass by observing nothing.
- R4. A case reserves a foreign-call-log slot whose line format is the runner transcript of astubbs#390; on this rung the slot is unset and not compared.

**The oracle**

- R5. The expected outcome of a case is produced at run time by executing it through plain Apache Kafka Streams via `TopologyTestDriver`; it is never hand-written and never committed.
- R6. Every case in the corpus runs through the oracle in the no-Docker unit lane, so the gate needs no broker and no container.
- R7. The oracle is shown deterministic on every run: the same case executed twice yields an identical outcome, and a difference fails the run naming the case (the control arm - nondeterminism is the forbidden anomaly).
- R8. The differ is shown able to say no on every run: each case's author-chosen perturbed twin - a perturbation the case's operations cannot absorb, such as a key change or an extra record for `count` and a change to the last value for a last-wins `reduce` - yields a different outcome, and agreement fails the run naming the case (the positive control). Perturbing a copy of the computed outcome instead would prove only that the comparison works, not that the pipeline from execution to comparison is sensitive to its input.

**The gate**

- R9. A guard asserts that a binding row is registered exactly when the wrapper's own streams engine - never Apache Kafka Streams, which this rung carries as the oracle's dependency - is reachable on the test classpath, in both directions. A class reaches that classpath only through a dependency the conformance module declares, so the guard can first fire when a later rung declares the wrapper module as its test dependency; from then it goes red if that dependency lands without a row or a row is written without it, and retires itself when both are present.
- R10. Selecting a binding by name that is not registered fails the run naming the typo and what is registered; it never runs the oracle alone and reports green.
- R11. A red names the case and which proof failed - determinism (R7), the positive control (R8), a guard (R9, R16) or a load-time rule (R1, R3) - so a maintainer can act on it without re-running; naming the row that diverged is the driver rung's extension of this rule, since this rung has one row.

**The corpus**

- R12. The first corpus has at least one case per builder operation as its floor, and at least one multi-operation chain per handle-kind transition in the grammar - stream to grouped stream to table, table to stream, stream to time-windowed stream - because divergence in a wire-crossing binding lives at the joins between operations, not inside them; plus one non-linear topology (a join), one windowed aggregate compared at final state, and one windowed aggregate with a pinned emit rule, the last oracle-only until the wrapper exposes an emit control.

**The records**

- R13. The rung produces its `docs/data/testing-evidence.yaml` row, stating what is and is not evidenced and the exact bounds of the claim - final state, under `TopologyTestDriver`, at one pinned Kafka version, with no broker row - in the words a later docs entry must inherit. It adds no `docs/data/module-maturity.yaml` row and no `docs/features/` entry: a test-only module with no published artifact has no maturity, API expectation or support posture to state, and nothing here is a user-facing feature yet.
- R14. The obligations this rung leaves for the driver rung are recorded in this rung's PR body and in a `docs/inflight/` note it adds, for whoever next touches the astubbs#334 branch to fold into the design note `docs/inflight/test-cross-binding-streams-conformance.md` there; that note exists only on that branch and is not a file this rung can edit.
<!-- file-refs: N/A - named as a file on the astubbs#334 branch (research/kafka-streams-foreign-wrappers), which this rung is cut below on master; it is cited as prior art, not as a file this plan touches -->

**Coverage and refusals**

- R15. A second case class expects a named refusal rather than an outcome: the case declares the fault the wire must raise for an invalid specification - an aggregate naming both a function and a combine, a retention below the minimum - and no oracle row is computed for it, because plain Kafka Streams never refuses what this wire invented; it is marked driver-only until a binding exists.
- R16. A coverage check holds the corpus against the wrapper's builder surface and fails when an operation has no case, with an explicit deliberately-uncovered list carrying a reason per entry - the third guard, mirroring astubbs#387's scenario-coverage test, so the eleventh operation cannot ship uncovered while the gate stays green.

### Actors

- A1. The maintainer proceeding on the fast-path program, who needs a red they can act on.
- A2. Plain Apache Kafka Streams, running under `TopologyTestDriver` - the oracle row, and the only row on this rung.
- A3. A binding driver, arriving with a later rung as a runner under astubbs#390's contract.
- A4. The CI unit lane, which runs the gate without Docker.

### Key Flows

- F1. The gate runs
  - **Trigger:** the unit lane runs the conformance module.
  - **Actors:** A2, A4
  - **Steps:** every case in the corpus is executed through the oracle; each case is executed a second time and compared to the first (R7); each case is executed against its perturbed twin and the outcomes are required to differ (R8); the coverage check holds the corpus against the builder surface (R16); the guard checks the classpath (R9); a binding selector, if given, is resolved (R10).
  - **Outcome:** green, or a red naming the case and the proof (R11).
  - **Covered by:** R5, R6, R7, R8, R9, R10, R11, R16
- F2. A maintainer adds a case
  - **Trigger:** a new operation or a new shape needs covering.
  - **Actors:** A1, A2
  - **Steps:** the maintainer writes the topology, the inputs, and the agreement level; if update streams are to be compared, names the emit rule (R3); runs the module locally; the oracle computes the outcome and both proofs run against the new case.
  - **Outcome:** a case whose expectations were never typed by hand, and which has already been shown to be deterministic and discriminating.
  - **Covered by:** R1, R2, R3, R7, R8
- F3. The first binding arrives
  - **Trigger:** the driver rung declares the wrapper module as a test dependency of the conformance module, which is what brings the wrapper's engine onto its classpath.
  - **Actors:** A1, A3
  - **Steps:** the guard goes red (R9) until a driver row is registered; the driver rung registers a runner under astubbs#390's contract; the runner's transcript fills the call-log slot (R4); the same corpus runs against both rows.
  - **Outcome:** the first real divergence signal, and the net's trust claim becomes checkable.
  - **Covered by:** R4, R9

### Acceptance Examples

- AE1. Determinism is proven, not assumed
  - **Covers R7.**
  - **Given** any case in the corpus, **when** it is executed through the oracle twice in one run, **then** the two outcomes are identical, and a case for which they differ fails the run naming that case.
- AE2. The differ can say no
  - **Covers R8.**
  - **Given** any case in the corpus, **when** its author-chosen perturbed twin is executed, **then** the outcome differs from the original's, and a case for which they agree fails the run naming that case.
- AE3. The guard reads both ways
  - **Covers R9.**
  - **Given** the wrapper's engine is not on the test classpath, **when** the guard runs, **then** it passes only if no binding row is registered; **given** a declared dependency has put it there, it passes only if a binding row is registered.
- AE4. A typo cannot read as a pass
  - **Covers R10.**
  - **Given** a selector naming a binding that is not registered, **when** the module runs, **then** it fails naming the unknown binding and the registered ones, and no case is executed.
- AE5. Update streams need a pinned rule
  - **Covers R3.**
  - **Given** a case declaring update-stream comparison without naming a close-driven emit rule, **when** the corpus is loaded, **then** loading fails naming the case; **given** the same case with a rule named, it loads.
- AE6. The default compares final state only
  - **Covers R3, R5.**
  - **Given** a case with no agreement level declared, **when** it runs, **then** only final state - store contents and each sink's final record per key - is compared, and the update stream is neither recorded nor compared.
- AE7. A red is actionable
  - **Covers R11.**
  - **Given** a case that fails one of this rung's proofs, **when** the run reports, **then** the report names the case and which proof failed; naming a diverging row is verified at the driver rung, where a second row exists.
- AE8. A stateless case cannot pass by observing nothing
  - **Covers R3.**
  - **Given** a case whose topology holds no state store and produces no sink record, **when** the corpus is loaded, **then** loading fails naming the case; **given** the same topology with a sink, its final record per key is the final state compared.

### Scope Boundaries

- The differential fuzz generator over the builder grammar - a later rung; nothing of ours consumes it until a driver exists.
- The driver for our engine, and the Python and other per-language drivers - the driver rungs, off astubbs#334.
- A real-broker Kafka row beside the `TopologyTestDriver` row - belongs to the driver rung, where a broker exists anyway.
- Committed, version-tagged recordings - rejected in favour of the live oracle.
- Reflecting Apache Kafka's own test suite through the binding - rejected in the 2026-08-25 note.
- A `docs/features/` entry and the user-facing trust claim - enabled by the first driver row, not by this rung.
- Exactly-once across the boundary, and the sidecar-versus-embedded choice - the questions this net exists to make safe, not part of it.
- Measuring refusal conformance - the fault a binding must raise for an invalid specification is specified here (R15) and first measured at the driver rung.

<!-- ce-section: work-relationships -->
### How This Work Fits Together

This plan owns the oracle rung.
The breakdown below is the current understanding of the surrounding work, not a committed roadmap; a later plan may revise, split, merge or discard any of it and cite this one.

- The driver rung: our engine measured against the corpus.
  - Depends on this plan for the case format, corpus and oracle.
  - Depends on astubbs#334 for the streams engine and on astubbs#390 for the runner contract and registry; it bases on astubbs#334, which carries both.
  - Fills the call-log slot (R4) and turns the guard (R9) green.
  - Reads final state through the binding's interactive-query surface, which becomes load-bearing for conformance there.
  - Declares the wrapper module as a test dependency of the conformance module in the same change that lands the engine there; the guard (R9) cannot fire before that declaration exists.
- The per-language driver rungs: one runner per binding under astubbs#390's contract.
  - Depend on the driver rung.
  - Can proceed independently of each other.
- The differential fuzz generator.
  - Depends on this plan's case format.
  - Can proceed independently of the driver rung; it exercises the oracle alone until a binding exists.
- The real-broker Kafka row.
  - Belongs to the driver rung; proves the final-state-floor contract empirically.
- Emit control on the wrapper.
  - Still to decide, and owned by astubbs#334's follow-ons; until it exists, pinned-emit cases (R12) are oracle-only.
- The user-facing trust claim (`docs/features/`, the parked testing-as-a-feature requirement).
  - Enabled by the first driver row.

### Dependencies / Assumptions

- `kafka-streams-test-utils` is the only new dependency this rung needs; it is already a test dependency of the wrapper module on astubbs#334, so the version is settled there.
- No Kafka version matrix runs on master today: the compatibility job is PR-only and disabled. The oracle therefore runs at the single pinned `kafka-streams` version, and a bump that changes Kafka's behaviour changes both sides of the comparison at once. This is the accepted consequence of the live oracle, not a gap to close here.
- The wrapper exposes no suppression or emit-strategy control; its assembler's own comment says the wire does not expose it. Pinned-emit cases are oracle-only until that changes.
- The wrapper's builder surface is ten proto operations and is `v1alpha1`, unfrozen. The case format assumes that grammar is stable enough that a future runner translates a case into builder calls one-to-one; if the grammar moves, the format moves with it in the driver rung.
- The proxy conformance module runs with no Docker and no broker; this rung holds itself to the same.

### Outstanding Questions

**Resolve Before Planning**

- None.

**Deferred to Planning**

- Whether this rung is a new module or a second scenario family inside the proxy conformance module; the latter would base the rung on astubbs#387 rather than master, so the placement decision above constrains the answer.
- The case file format and where the corpus lives.
- How final state is read from `TopologyTestDriver` for comparison, and how a perturbed twin is derived from a case's inputs.
- The vocabulary a case uses to name a close-driven emit rule, and how the oracle applies it.
- Whether the guard (R9) lives in this rung's module or is added by the driver rung's module.

### Sources / Research

- `docs/inflight/test-cross-binding-streams-conformance.md` on the astubbs#334 branch - the settled mechanism, the `TopologyTestDriver` over-count qualification, and the product-feature framing.
- `docs/inflight/parked-testing-as-a-feature-for-the-clients.md` on the same branch - the owner's requirement that conformance is a documented feature, which the Streams net inherits.
- astubbs#387 - the proxy conformance suite: scenario as data, the engine as a row, every scenario proven able to fail, nothing deferred stubbed, and the two guard tests this rung mirrors.
- astubbs#390 - the runner contract, the transcript line format, the language runner registry and prebuild, which the driver rungs reuse.
- `parallel-consumer-proxy-streams/src/test/java/bz/stub/parallelconsumer/streams/WindowedAggregatorCallCountTest.java` on the astubbs#334 branch - the measured statement that `TopologyTestDriver` commits per record.
- `CONCEPTS.md` - the house definitions of control arm, positive control and red-proof, which R7 and R8 use by name.
<!-- file-refs: N/A - named as a file on the astubbs#334 branch (research/kafka-streams-foreign-wrappers), which this rung is cut below on master; it is cited as prior art, not as a file this plan touches -->

## Deferred / Open Questions

### From 2026-09-06 review

- **Whether the streams engine may reach master without a conformance row** - How This Work Fits Together / R9 (the guard) (P1, feasibility, product-lens, confidence 75)

  The flagship engine work either sits behind two test rungs or someone quietly weakens the guard to unblock it, because the plan asserted a coupling - astubbs#334 cannot merge until a driver row exists - through a classpath guard that cannot enforce it: a class reaches a module's test classpath only by a dependency that module declares. Feasibility's fix drops the claim and places the obligation on the driver rung; product-lens would promote the coupling to an accepted decision with a mechanism that works. The applied edits took the first half; whether the coupling is wanted at all is still open.

- **Pinned-emit mode: build it now or reserve it** - R2 / R3 / R12 (P2, feasibility, adversarial, confidence 75)

  The vocabulary for naming a close-driven emit rule gets fixed before the semantics it names exist, so both it and the corpus case change when the wrapper's emit control arrives - and until then an oracle-only case compares the oracle to itself. A close-driven emit rule is not a builder operation, so the pinned-emit case cannot be written in the case grammar as it stands. Feasibility would add the emit rule as a first-class case attribute now, breaking the one-to-one builder translation; adversarial would cut update-stream mode, AE5 and the pinned-emit case to a reserved, unset field alongside the call-log slot until the control exists.
