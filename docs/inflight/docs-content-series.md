# Content series: engineering investigations where PC happens to be the instrument

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - after v6, the announcement funnel comes first -->

From the Codex strategy review of 2026-08-22/23 (breakdown in
[`core-engine-thesis.md`](core-engine-thesis.md)). Distinct from the v6 announcement funnel, which
[`release-v6-phoenix-theme-and-announcement.md`](release-v6-phoenix-theme-and-announcement.md) owns:
that is the news event; this is the six months of material after it. **Deliberately not
promotional posts** - each is an investigation whose data ships with it
([`docs-research-program.md`](docs-research-program.md) is the supply side), each targets an
audience that does not yet care about the project, and all roads lead back to one repository.

## The candidates, grouped - the review produced ~25 titles; these are the strongest

**Partition myths** (conceptual entry points, good search traffic):
- *Your Kafka partition count is not your concurrency* - separate assignment / key / I/O / useful
  concurrency; the thesis introduction without naming PC until halfway.
- *Do you really need more Kafka partitions?* - partition-count planning is a universally
  recognised annoyance.
- *Why adding consumers sometimes does absolutely nothing.*

**Hot keys and tails:**
- *The hot key isn't the problem. Its neighbours are* - collateral head-of-line blocking, measured.

**Adaptive control** (reaches control-systems and platform people):
- *Why maxConcurrency=100 is probably wrong.*
- *Backpressure is not a number.*
- *The consumer knows something Kubernetes doesn't* / *Kafka lag is lying to your autoscaler* -
  identical lag, radically different consumability; why `lag > X -> replicas++` misfires.
- *Virtual threads don't tell you how much concurrency you can afford* - cheap concurrency vs
  useful concurrency; reaches well beyond Kafka.

**Boundary engineering** (reaches native-interop people):
- *Why we didn't port Parallel Consumer to Python* - "we gave Python PC by refusing to implement
  PC in Python".
- *We removed the RPC. Was it actually faster?* - the FFI-vs-sidecar result, unrevealed in the
  title.
- *One bug, ten languages* - the shared-engine argument: one fix corrects every binding; the
  alternative is ten implementations drifting semantically.
- The RPC-overhead piece (working title used a ~400µs figure - **verify against the measured
  numbers before using any figure**): quantify when the hop matters, at 100ms handlers vs 1ms.

**Streams:**
- *We ran Kafka Streams' test suite against a different execution model* - the sober one; failures
  almost as interesting as passes. Avoid antagonistic framings toward maintainers; the underlying
  line worth keeping is *architecture arguments eventually become executable hypotheses*.

**Method:**
- The AI-era maintainership piece: not boosterism - the observed economics. Agents are workers,
  branches are records, maintainer attention is the ordering key; implementation concurrency high,
  conceptual concurrency low. Written after the release post, from this repo's actual practice.

## Publication mechanics

Data and runnable benchmarks with every post, so readers can disagree while reproducing the
experiment. Sequence so each earns its own audience before the umbrella project needs them.

## The branding line, flagged rather than adopted

The review proposed reserving *"Kafka Can Linger. Hasten Doesn't."* for a rebrand announcement
("Hasten" being a floated project name, the first recorded candidate). Two problems, surfaced so
the owner decides with them in view: it puns on `linger.ms` but lands adjacent to the competitor
llingr, and [`next-reclaim-the-category.md`](next-reclaim-the-category.md) rules the category is
*"won by building, never by attacking anyone else's work"*. A rename is the owner's call and is not
otherwise tracked here.
