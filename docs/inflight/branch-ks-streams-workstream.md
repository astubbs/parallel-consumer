# Kafka Streams on PC (astubbs#255): the workstream exists, and master cannot see it

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->


A signpost, not a handover. None of this work is on `master` - not the
`parallel-consumer-streams` module, not its plan documents, not its own in-flight notes - so an
agent listing this directory sees only the sideways references other notes make to it and no way in.

**What it is.** Give a Kafka Streams topology PC's per-key concurrency by replacing Streams' record
selection with PC's `WorkManager` and running the processor chain on PC's worker pool, applied as a
build-time patch to a fork of Kafka's `processor.internals`. Swapping in a PC-backed `Consumer` via
`KafkaClientSupplier` was tried and does not work - Streams serialises above the consumer. The issue
carries the assessment and the tiered cost.

**Where it is.** Draft PR astubbs#271, head `feats/ks-on-pc-spike` - **that head is the base, not
the tip.** The live work sits on a dozen sibling branches (`git branch -a | grep -E 'ks-|streams-'`)
merged forward from that base and deliberately **never rebased**, because they build on each other.
Reading the PR head as the state of the work is the mistake this note exists to prevent.

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

astubbs#271 merges - which brings its own handover onto master, and that supersedes this file.
