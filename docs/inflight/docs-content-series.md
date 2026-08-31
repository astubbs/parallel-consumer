# Content series: engineering investigations where PC happens to be the instrument

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - after v6, the announcement funnel comes first -->

From the Codex strategy review of 2026-08-22/23 (breakdown in
[`core-engine-thesis.md`](core-engine-thesis.md)). Distinct from the v6 announcement funnel, which
[`release-v6-phoenix-theme-and-announcement.md`][release-v6-phoenix-theme-and-announcement] owns:
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
- From the 2026-08-29/30 follow-up ([`core-partition-advisor.md`](core-partition-advisor.md)):
  *Choose partitions for Kafka. Let PC choose parallelism for your application* - and the
  conference-slide form, *"How many partitions will you need in three years?" Wrong question.*

**Hot keys and tails:**
- *The hot key isn't the problem. Its neighbours are* - collateral head-of-line blocking, measured.

**Adaptive control** (reaches control-systems and platform people):
- *Why maxConcurrency=100 is probably wrong.*
- *Backpressure is not a number.*
- *The consumer knows something Kubernetes doesn't* / *Kafka lag is lying to your autoscaler* -
  identical lag, radically different consumability; why `lag > X -> replicas++` misfires.
- *Virtual threads don't tell you how much concurrency you can afford* - cheap concurrency vs
  useful concurrency; reaches well beyond Kafka.

**Scaling unit** (from the follow-up conversation, 2026-08-29/30 -
[`core-per-function-capacity-arbitration.md`](core-per-function-capacity-arbitration.md) is the
feature behind these):
- *Stop scaling your whole Kafka application because one part of it is busy* - the strongest
  headline of the follow-up: scaling the pod duplicates every cold consumer to feed one hot one.
- *Your application is not a scaling unit* / *Every function deserves its own concurrency.*
- *Stop provisioning concurrency. Start discovering it.*
- The polyglot reframe belongs with these: not eleven SDKs but one runtime replacing eleven
  ecosystems' worth of concurrency machinery - a sharper form of the "engine every language
  re-implements badly" line already in [`core-auto-scaling.md`](core-auto-scaling.md).
- Broadest, candidate positioning rather than a post title: *Kafka tells us what work we own.
  Parallel Consumer figures out how to run it.*
- The category framing, sharpest of the lot: *the real competitor is manual concurrency
  management* - `maxConcurrency = 100` as this generation's manual memory management, expected to
  look as archaic as hand-sizing a web server's per-endpoint thread counts.

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
llingr, and [`next-reclaim-the-category.md`][next-reclaim-the-category] rules the category is
*"won by building, never by attacking anyone else's work"*. A rename is the owner's call and is not
otherwise tracked here. Noting for the record: the 2026-08-29/30 follow-up conversation used
"Hasten" throughout as though the rename had happened, and closed on the same tagline - an
assumption in a transcript, not a decision.

<!-- These notes live on `research/market-analysis-recut`, not master. Pinned to a commit
     so the links keep resolving after the branch moves or merges. -->
[next-reclaim-the-category]: https://github.com/astubbs/parallel-consumer/blob/cd2156ce9/docs/inflight/next-reclaim-the-category.md
[release-v6-phoenix-theme-and-announcement]: https://github.com/astubbs/parallel-consumer/blob/cd2156ce9/docs/inflight/release-v6-phoenix-theme-and-announcement.md
