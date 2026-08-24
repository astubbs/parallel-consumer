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

## What survives, and is worth building

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
