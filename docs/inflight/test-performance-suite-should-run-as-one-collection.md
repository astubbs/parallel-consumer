# The Kafka Streams performance suite should run as one collection

**Requested 2026-08-11.** The benchmarks are accumulating - head-of-line blocking, the realistic domain
workload, the synthetic matrix (key distribution, processing profile, payload size), backlog catch-up with
a depth sweep, and partition composition. More are queued.

Right now getting the current numbers means knowing which classes exist, invoking each with the right
flags, and reading them out of separate outputs. **That is how a number goes stale without anyone
noticing** - and several already have: `68/101` is recorded in six places while the real figure moved.

What is wanted: **one invocation runs the collection and prints a single comparable report**, so the
current numbers can be quoted without changing inputs, re-deriving anything, or maintaining a spreadsheet.

Requirements worth stating, since they are what makes it useful rather than merely tidy:

- **One command, one report.** Every arm, every cell, current figures, in one place a human can read.
- **The unflattering cells appear alongside the flattering ones.** Non-negotiable - it is what the whole
  suite's credibility rests on, and a report that omits them is worse than no report.
- **Whole-batch drain printed in every cell**, since that is the statistic a sceptic computes first and the
  one that nearly went unmeasured.
- **State which figures are reproduced and which are single-run.** See
  `test-benchmark-figures-that-are-single-run.md` - most cells are still anecdotes, and a report that
  presents them identically to the reproduced ones is misleading by layout.
- **Stays out of the gating lane.** These are long (the realistic suite alone is about 20 minutes) and
  belong on the self-hosted `highcpu` runners, non-gating, beside the Chaos Pain Suite.

Partly in place already: the new benchmarks carry `@Tag("performance")` and the repo excludes that group
from the normal lane. What is missing is the collection-level runner and the single report.

**Worth considering:** emit the report as a tracked data file the docs read from, the way
`docs/data/testing-evidence.yaml` already works, rather than as console output someone must copy. That is
what stops the README and the plan drifting from the measurements again.

## Delete when

One command runs the collection and produces a single current report, and the module's published figures
are read from it rather than transcribed.
