# Kafka Streams on PC (astubbs#255): master sees the machinery, not the workstream

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->


A signpost, not a handover. **What is on `master` is the fork/build machinery only** - the
`parallel-consumer-streams` module shell, its patch/regenerate discipline and the upstream-suite
oracle, landed as the base of a reconstructed stack. It patches Kafka's processor context and record
collector for thread safety and stops there: no PC execution seam, no dispatcher, no records through
PC, and the module is **not published**. Everything else - the seam, the semantics, the measurements,
the plan documents and the workstream's own in-flight notes - is still off `master`, so an agent
listing this directory sees only sideways references to it and no way in.

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
machinery described above is the first rung; the seam is the second. The forest branches are not
retired by any of this and are still where the unlanded work lives.
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
