# Build a realistic case we expect NOT to win, and publish it

**Requested 2026-08-11.** We have unfavourable numbers already - single key 0.99x, steady-state below
saturation 0.99x, CPU at equal thread count 1.19x, Zipf 1.5 at 2.00x. But every one of them is a
**synthetic floor**: a degenerate configuration chosen to isolate a property.

What does not exist is a **plausible business workload, built in good faith, that we expect to gain little
from** - and the honest result of running it.

## Why this is worth building deliberately

The credibility argument is the same one that made the single-key control valuable next to the 57x
headline, but stronger: a synthetic floor reads as a boundary condition, while a realistic workload that
gains little reads as *these people will tell you when their thing does not help*. That is the difference
between a benchmark suite and marketing, and it is very hard to fake.

It is also the honest input to adoption advice. Right now the guidance we can give is "helps when
saturated, with blocking work and unskewed keys". A realistic case that does not win tells a reader what
their own workload has to look like far better than a list of degenerate floors does.

**Pick it the way a hostile reviewer would**, and say what makes it unfavourable before running: low key
cardinality, work that does not block, a rate below saturation, or a cost distribution tight enough that
there is nothing to overlap. Any of those alone should be enough.

**Predict the outcome first, and publish it whichever way it goes.** If it unexpectedly wins, that is a
genuinely interesting finding and worth chasing rather than quietly filing. The prediction is the point.

## Delete when

Such a workload exists in the suite, its expected-and-actual outcome is recorded, and the adoption guidance
in the module README reflects what it showed.
