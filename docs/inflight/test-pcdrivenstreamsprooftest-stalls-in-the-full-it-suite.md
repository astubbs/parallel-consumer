# `PcDrivenStreamsProofTest` stalls in the full integration suite, but not alone

**Found 2026-08-11 while re-deriving README figures. Characterised with a control arm, not yet diagnosed.**

`PcDrivenStreamsProofTest.pcDrivenOutputMatchesTheStockBaseline`, repeat `[2]`, times out on a two-minute
awaitility drain when the whole integration suite runs - **2 of 2 attempts**.

**The control arm is what makes this worth recording:** run the class in isolation and it passes **5 of 5
in about 30 seconds**. Earlier logs the same day on the same branch show it green at 22s and 20s. So this
is a **cross-class interaction inside the full integration run**, not a defect in the test or in the code
it exercises.

**What it is NOT.** No wrong output was ever observed. The drain simply never reached its expected count,
so the output-equality claim this test exists to prove is **not refuted** - it went unverified in those
runs. Worth being precise about that: a timeout is an absence of evidence, and this one has an innocent
reading (the run was slow) and a guilty one (something upstream stopped feeding it).

**Why it looks familiar.** A test awaiting a consequence that never arrives, passing in isolation and
stalling under load, is the shape this repo keeps meeting - see `docs/solutions/test-flakiness/` for the
family, and note the vocabulary already exists in `CONCEPTS.md` for telling its members apart: a
*load-tightness flake* (deadline too tight under contention) is a different thing from an *unforceable
trigger* (the awaited event may never happen) which is a different thing again from a real product
*stall*. **Which of the three this is has not been established**, and guessing is exactly what that
vocabulary was written to stop.

Note it passed in CI on PR astubbs/parallel-consumer#271 - the `Integration Tests` job runs this class and
went green - so whatever the interaction is, it is load- or ordering-dependent rather than deterministic.

## Where to start

- Establish whether it is contention or a genuine stall before anything else. The ambient progress probe
  attached to broker integration tests annotates a failure with consumer-group progress evidence, which
  answers that question directly - but only when its detectors could have fired, so check that first.
- Establish which sibling class it interacts with. It stalls in the full suite and not alone, so bisecting
  the suite is cheap and would name the interaction rather than describing it.
- Note the ordinary `test` phase does not run this class, so a green module build says nothing about it.

## Delete when

The interaction is identified and either fixed or recorded with evidence, and the class is either reliable
in the full suite or quarantined with a diagnosis.
