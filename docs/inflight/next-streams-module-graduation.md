# Graduating the streams module out of "spike"

The owner's notes on what `parallel-consumer-streams` (astubbs#255) needs before it is a module
people are asked to take seriously rather than a published experiment. The ranked technical worklist
is `pr-ks-spike-next-work.md`; packaging, naming, licensing and the user-facing documentation are in
`next-fork-packaging-docs-and-licensing.md`. This file is only the "stop calling it a spike" half.

## Demonstrate the seam in the example module

`parallel-consumer-examples/parallel-consumer-example-streams` currently shows the *old* answer to slow
processing in Kafka Streams: a topology preprocesses, writes to a topic, and a separate Parallel
Consumer instance does the slow work concurrently downstream. The new module removes the need for that
hop, and the examples say nothing about it. In the owner's words, "it's a pretty big deal" - so the
example should demonstrate it.

**Unresolved: what "or invert it" meant.** Two readings, both plausible, and the note does not settle
which:

- Make the PC-driven topology the **primary** streams example, with the existing hand-off-downstream
  pattern demoted to the alternative, rather than bolting the new demo on as an addendum.
- Invert the **example itself**: today Streams hosts the pipeline and hands off to PC downstream; the
  module inverts that relationship so PC drives the topology from the inside. The example would then
  run the same workload both ways, making the inversion the thing being demonstrated.

Worth deciding before any code is written, because the two produce quite different examples. Whichever
wins, this is also the natural home for the realistic-domain workload in
`next-fork-packaging-docs-and-licensing.md` - check there before building a second one.

## Tag the public surface as evolving

The module's public API should say outright that it is unstable. `parallel-consumer-core` already has
the mechanism: `ParallelConsumerOptions` carries Kafka's own
`org.apache.kafka.common.annotation.InterfaceStability.Evolving`. Reuse that rather than inventing an
annotation, and apply it across the module's public types.

It pairs with the per-module maturity story in `next-fork-packaging-docs-and-licensing.md`, and it is
the stronger half of that pairing: an annotation sits at the place a user actually touches, where a
maturity note lives in a README nobody opens.

## Open question, deliberately unanswered: what happens if we release this and it works?

Recorded because the owner asked it, not because anyone has an answer:

> "If we release this, and it works, what do you think will happen?"

What is being asked is what a working, published version actually sets off: how the Kafka Streams
community and upstream Apache Kafka react to a fork that patches Streams internals, what adoption would
do to the burden of carrying those patches, and what it would mean for what this fork is. It deserves a
considered take rather than an answer in passing. The hostile review
(`pr-ks-spike-hostile-streams-review.md`) is a cheap input to it, since the reaction to expect is
roughly the one that review is written to provoke.

## Delete when

The module no longer calls itself a spike, the example demonstrates the seam, the public surface is
annotated, and the release question has an answer recorded somewhere durable.
