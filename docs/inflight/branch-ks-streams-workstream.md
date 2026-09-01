# Kafka Streams on PC (astubbs#255): master sees the machinery, not the workstream

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->


A signpost, not a handover. **What is on `master` is the machinery and the minimal execution seam** -
the `parallel-consumer-streams` module, its patch/regenerate discipline, the upstream-suite oracle,
and (second rung) `PcTaskDispatcher`, wake-on-work and the `StreamTask`/`StreamThread` hunks that
reach them. Records do go through PC, **with the seam switched on**, which it is not by default.

**What is still off `master`** is most of what makes that seam usable: refusing unsupported topology
shapes (so today an unsupported one is dispatched rather than refused - the reason the seam defaults
off), task lifecycle and rebalance, stream time and punctuation, the benchmarks and the seam-on
upstream evidence lane, the example module, and the plan documents and in-flight notes of the
workstream. The module is **not published** either way. So an agent listing this directory still
sees sideways references to most of the work and no way in.

**What it is.** Give a Kafka Streams topology PC's per-key concurrency by replacing Streams' record
selection with PC's `WorkManager` and running the processor chain on PC's worker pool, applied as a
build-time patch to a fork of Kafka's `processor.internals`. Swapping in a PC-backed `Consumer` via
`KafkaClientSupplier` was tried and does not work - Streams serialises above the consumer. The issue
carries the assessment and the tiered cost.

**Where it is.** Draft PR astubbs#271, head `feats/ks-on-pc-spike` - **that head is the base, not
the tip.** The live work sits on a dozen sibling branches (`git branch -a | grep -E 'ks-|streams-'`)
merged forward from that base and deliberately **never rebased**, because they build on each other.
Reading the PR head as the state of the work is the mistake this note exists to prevent.

**How it reaches master.** Not by merging that forest. The decomposition plan
(`docs/plans/2026-08-31-001-process-god-branch-decomposition-plan.md`, Wagon B) reconstructs it as a
fresh stack cut from `master`, taking content from the forest by copy: the forest stays as the
evidence record, and the PRs document what the design *is* rather than how it was discovered. The
machinery described above is the first rung; the seam is the second, and both have landed. The forest
branches are not retired by any of this and are still where the unlanded work lives.
<!-- file-refs: N/A - the decomposition plan arrives on master with its own PR, not with the first rung -->


**Before touching any of them, read the branch's own handover:**
`git show abcc811e6:docs/inflight/branch-ks-streams-handover.md` - branch topology, the build traps
that have already cost whole runs, the ranked open defects, and the decisions settled so they are
not relitigated. That file is the detail; keeping it there is deliberate, so it travels with the
code it describes.

Two things worth knowing without opening any of it:

- **The cluster predates the package rename.** Its sources are still `io.confluent.*`, while
  astubbs#271's own head has been renamed. Each branch runs `bin/rename-packages.sh` *before*
  merging master - the rule in [`AGENTS.md`](../../AGENTS.md), and this is the largest set of
  branches it still applies to.
- **It does not gate 0.6.0.0**, settled. Whatever state it is in when the release cuts is what
  ships. Merging is cheap to reverse - it is a leaf module - and publishing is not.

**The outstanding decision is packaging, not code**: the patched Streams jar and stock Kafka Streams
on one classpath, which is a coordinates problem rather than a fork problem. Parked, with the
options recorded in the plan on the branch.

## Delete when

The reconstructed stack has landed far enough that `master` carries the seam and the handover it
points at - at which point that handover supersedes this file. Landing the machinery alone does not
qualify: the forest, and the reason this signpost exists, both outlive it.
