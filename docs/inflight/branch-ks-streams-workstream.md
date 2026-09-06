# Kafka Streams on PC (astubbs#255): the workstream exists, and master cannot see it

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->


A signpost, not a handover. None of this work is on `master` - not the
`parallel-consumer-streams` module, not its plan documents, not its own in-flight notes - so an
agent listing this directory sees only the sideways references other notes make to it and no way in.

**Where it actually lives, as of 2026-09-02.** This note was a signpost that named no branches, so it
said the workstream exists without saying how to reach it. Found by `bin/inflight.mjs branch`, which
reported `origin/feats/ks-streams-reconciled` as tracked nowhere; it is not an orphan, it is the
integration branch, and the shape is:

| Branch | PR |
|---|---|
| `origin/feats/ks-streams-fork-machinery` | astubbs/parallel-consumer#379 |
| `origin/feats/ks-streams-execution-seam` | astubbs/parallel-consumer#388 |
| `origin/feats/ks-streams-refusal-envelope` | astubbs/parallel-consumer#389 |
| `origin/feats/ks-streams-example` | astubbs/parallel-consumer#391 |
| `origin/feats/ks-streams-task-lifecycle` | astubbs/parallel-consumer#394 |
| `origin/feats/ks-streams-error-surfacing` | astubbs/parallel-consumer#395 |
| `origin/feats/ks-streams-stream-time-punctuation` | astubbs/parallel-consumer#396 |
| `origin/test/ks-streams-evidence-suite` | astubbs/parallel-consumer#398 |

**`origin/feats/ks-streams-reconciled` fully contains all eight and has no PR of its own** - it is
where they are integrated, which is why nothing tracks it and why it should not be read as stranded
work. It carries 13 in-flight notes `origin/master` has never had. Above it,
**astubbs/parallel-consumer#271** (`origin/feats/ks-on-pc-spike`) contains the reconciled branch in
turn, at 120 commits and 29 notes off master.

Reproduce with `bin/inflight.mjs branch origin/feats/ks-streams-reconciled`; containment is exact
rather than inferred from names, so this stays true as branches move.

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
