# Benchmark ordinary DSL work with no artificial delay - and mind the joins problem

**Requested 2026-08-11.** Every benchmark so far pays a simulated cost per record - a sleep standing in
for a blocking service call. That is legitimate, because blocking IO is the case this module exists for,
and it is also the shape a sceptic attacks first: *you only win because you invented the wait.*

So: a topology doing **ordinary Kafka Streams work with no injected delay at all** - real serde cost, real
CPU in the processor, real state access - and measure whether PC dispatch helps, hurts, or does nothing.

## Why this is worth more than a completeness exercise

**If it helps, the claim changes category.** "Faster for blocking-IO workloads" becomes "faster for
ordinary Streams workloads", which is a far larger audience and a much stronger reason to adopt. Stock
Streams processes one record at a time per partition **whether that record is blocked or computing** -
that is the mechanism, and it does not care why the record takes time. The CPU-bound cell already hints at
this: 3.85x on an idle box, 1.19x at equal thread count.

**If it hurts, we learn the overhead number**, which is what bounds honest adoption advice. The seam costs
a pool handoff and a completion feedback per record; with nothing to overlap, that cost is pure. Publishing
it is what makes the favourable numbers credible.

Either answer is worth having. Predict before running.

## The joins problem - read this before scoping it

**Joins, windowed operators and suppression are now REFUSED on the dispatch path** and throw
`UnsupportedOperationException` (astubbs#255, the API gating work). So a joins benchmark cannot be run
PC-driven at all today: it would compare stock-with-joins against PC-refuses. That is not a measurement.

What is inside the envelope and can be measured now:

- stateless transforms (`map`, `filter`, `flatMap`, `transformValues`)
- **non-windowed aggregation** - `groupByKey().count()` and friends, already exercised by
  `PcDrivenStatefulProofTest`
- real serde work, real state store access, realistic record shapes

**And this is where it gets interesting for the roadmap.** If dispatch turns out to help ordinary DSL work,
that is an argument for *making joins correct* rather than refusing them permanently - the refusal plan
already carries a reinstatement gate and an open question about the wider unit surface. A benchmark showing
a large win on the supported subset would reprioritise that from "someday" to "the next thing", because the
refused constructs are exactly where the remaining users are.

Conversely if it shows nothing on non-blocking work, refusing joins costs us far less than it appears to,
and that is worth knowing before anyone spends a quarter on stream-time correctness.

## Sequencing: do the supported-envelope half NOW, the joins half LATER

Split it, because half of this is blocked and half is not:

- **Now.** Stateless transforms and non-windowed aggregation with real serde and real CPU, no injected
  sleep. Entirely inside the supported envelope, needs nothing that does not exist, and answers the
  question that actually changes the claim's category.
- **Later, gated on the joins reinstatement.** Anything using joins, windows or suppression cannot be
  measured PC-driven until those constructs work rather than throw. Do not attempt it before then and do
  not treat the refusal as a benchmark result - "it threw" is not a performance number.

**Leave a marker on the joins half rather than losing it**, because it is the more valuable measurement of
the two and it will become possible: the moment the reinstatement work lands, the first question should be
whether dispatch helps the constructs that were refused. If it does, that retroactively justifies the whole
reinstatement effort; if it does not, the refusal was cheap and should stay.

## Delete when

The measurement exists for the supported envelope, with both arms and the overhead stated plainly, and its
implication for the joins reinstatement question is recorded.
