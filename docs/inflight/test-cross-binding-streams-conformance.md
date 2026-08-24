# Testing the Streams binding in N languages without writing the suite N times

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

Owner's question, 2026-08-25: we cannot keep hand-writing a Python test for every Kafka Streams
feature, and doing it again per binding is obviously not scalable. How do we leverage the existing
Kafka Streams test suite instead?

## The idea as posed: reflect the Java suite through the binding

Build a translation layer in both directions, run the **real Apache Kafka Streams test suite**, and
route every DSL call it makes across the boundary into our Python wrapper - which wraps back into
Java. Performance would be irrelevant; correctness is the point. Kafka Streams' own suite then
becomes the binding's conformance suite, and the years of edge cases in it come for free.

The instinct is right and the scaling problem is real. Four things break the literal form.

- **Most of that suite tests Kafka Streams internals**, which is the owner's own caveat and it is
  correct. `StreamThreadTest`, `ProcessorStateManagerTest`, `RecordCollectorTest`, `RocksDBStoreTest`
  and their neighbours have nothing to reflect *to*, and proving them proves Kafka works.
- **The DSL-level tests reach into internals too.** The valuable subset - `KStreamImplTest`,
  `KTableImplTest`, the join tests - routinely cast to `KStreamImpl`, assert on generated node names
  and inspect `ProcessorTopology`. There is no host-side object for any of that.
- **Reflection has to be call-for-call, and this wire deliberately is not.** The design's whole point
  is a small set of builder calls with the engine holding the objects. The suite calls hundreds of
  overloads - `mapValues(ValueMapper)`, `(ValueMapperWithKey)`, `(..., Named)`, `(..., Materialized)`.
  `feats/ks-streams-refuse-unsupported-surface` (astubbs#255) catalogued **59 unsupported DSL
  overloads** for the sibling project, which is a direct measure of that width. Reflecting the suite
  means implementing essentially all of it, so the harness stops being a way to *test* the binding
  and becomes the reason to *build* one.
- **The return direction is the expensive half.** For a Java assertion to pass, Python must hand back
  something Java can inspect - a full object-graph proxy, not a value protocol. That is the
  re-entrancy problem in
  [`streams-coupling-dimensions.md`](streams-coupling-dimensions.md), generalised to every type.

## The design that works: reflect the SCENARIO, not the API

**Reflect the scenario, not the API.** A DSL test is almost always *given this topology and these
inputs, assert these outputs* - which is data, not a call trace. Write the conformance case once,
declaratively: topology description, input records, expected outputs, and the expected foreign-call
log. Each binding needs one small driver that reads the spec and issues the calls.

That is not a new mechanism here. **`parallel-consumer-proxy-conformance` already does exactly this
for the proxy clients**, so the work is extending a shape the repo runs rather than inventing one.

Then the good part of the original idea comes back: **use `TopologyTestDriver` as the oracle.**
Author a spec, run it through plain Java Kafka Streams to record what a real topology produces, and
that recording becomes the expected result for every binding. Kafka Streams stays the source of
truth about correctness; the assertion is written once; each binding contributes only a driver.

**A second, independent lever: differential testing.** Generate random topologies from the supported
DSL grammar, run each both natively and through the binding, and assert identical output. That is a
fuzzer over the boundary, it needs no per-feature test authoring at all, and it automatically finds
the class of bug that had to be hand-caught during the join work - two same-typed arguments
transposed, which compiles, runs, and returns a plausible wrong answer. The generator is the cost,
and the supported grammar is small enough to make it tractable.

### The four parts, stated once

1. **A conformance case is data, not code.** Topology description, input records, expected outputs,
   and the expected foreign-call log - what crossed the boundary, in what order, with which
   arguments. Nothing in it is language-specific.
2. **`TopologyTestDriver` is the oracle.** The expected outputs are *recorded* by running the spec
   through plain Java Kafka Streams, never hand-written. Kafka Streams stays the source of truth
   about what a topology does; we only assert that a binding reproduces it.
3. **Each language contributes a driver, not a suite.** One small program per binding that reads a
   spec and issues the calls. Adding a language costs a driver; adding a feature costs one spec,
   once, for every language at the same time.
4. **Differential testing sits beside it, not instead of it.** Generate topologies from the
   supported DSL grammar, run each natively and through the binding, assert identical output. It
   needs no per-feature authoring at all.

## Earmarked: this is a product feature, not only an engineering convenience

**Owner's direction, 2026-08-25.** When the bindings ship, the first honest question a reader asks
is whether they are generated slop, and
[`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md)
**owns that requirement** for the proxy clients - one suite, one set of scenarios, run identically
against every language, stated in the documentation as a feature. The Streams binding inherits it,
and the oracle above makes a *stronger* claim available than the proxy clients can make: the
expected results are not ours at all, they are Apache Kafka's own engine's, recorded.

Three extractions to make from this when it is built, and they are different audiences:

- **Architecture documentation** - the conformance mechanism itself: spec-as-data, recorded oracle,
  per-language driver. This is a reusable pattern, not a Streams detail.
- **User-facing documentation** - the trust answer. "Every binding runs the same scenarios, and the
  expected output was produced by Kafka Streams itself" is a checkable claim, and checkable claims
  are what the docs corpus and its gate exist to hold.
- **Promotional material** - the differential-testing arm is the vivid one: random topologies,
  native versus binding, byte-identical output. It demonstrates the wrap-rather-than-reimplement
  bet in one sentence and one picture.

## The reframe worth arguing about first

The fear assumes each binding has a large surface to test. **It does not, and that is the bet.** The
engine is shared; the Python side is a few hundred lines. What genuinely needs per-binding coverage
is enumerable: every builder call reaches the engine, every invocation kind dispatches to the right
function with its arguments in the right order, every failure surfaces rather than being swallowed.
That is tens of cases, not Kafka's suite.

If that holds, it is not merely a testing convenience - it is evidence for the wrap-rather-than-
reimplement claim itself, and belongs in `STRATEGY.md` rather than only here. If it turns out false,
the bet is weaker than the document says. Either way it is a claim someone should test on purpose,
and [`pr-strategy-doc-merge-triggers.md`](pr-strategy-doc-merge-triggers.md) is where that
obligation is tracked.
