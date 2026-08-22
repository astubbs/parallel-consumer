# Kafka Connect on PC (astubbs#240): stacked on the Streams work, and nothing delivers yet

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->


A signpost, not a handover. Like the Streams workstream it points at
([`branch-ks-streams-workstream.md`](branch-ks-streams-workstream.md)), none of it is on `master`.

**What it is.** Run Kafka Connect sink tasks under PC so distinct keys process concurrently within a
partition, by patching Connect's `WorkerSinkTask` at build time. The alternative direction -
embedding hand-built `SinkTask`s inside a PC app - was tried, written up and **rejected**: one task
per partition caps concurrency at the partition count, and a patched runtime inherits SMTs, DLQ,
ConfigProvider and plugin isolation for free.

**Where it is.** Draft PR astubbs#269, head `feats/connect-on-pc-spike`. **Its base is
`feats/ks-on-pc-spike`, not `master`** - Connect is stacked on the Streams spike and inherits that
whole module, so its diff against master is not its own work and it cannot land before the Streams
PR does. The branch's own note is the detail:
`git show 64595129c:docs/inflight/pr-connect-on-pc.md`.

**The two spike branches are not two designs.** `feats/connect-on-pc-spike-codex-version` is an
earlier snapshot of the same line as a Codex session left it on 2026-08-09, not a competing
approach: the plan document is byte-identical to the live branch's copy, and its code differs only
by the `connectspike` -> `connect` package rename. The live branch is a strict superset. **So the
open question is only whether to delete it, and nobody has recorded an answer** - do not spend time
comparing them as alternatives.

**What state it is really in.** It compiles, its tests pass, and the offset-composition rule has a
broker-backed crash-restart arm. But the dispatch bridge is hard-disabled and no record has ever
been delivered through it - `git show 64595129c:parallel-consumer-connect/README.md` opens by saying
so. The plan for the live delivery path is written, reviewed and unimplemented. Treat any
compatibility claim as a prediction until the branch says otherwise.

**Publishing is blocked on a human**, not on code: the fork packaging, licensing and trademark
questions gate publication of both experimental modules, and the standing decision is to publish
nothing - releases or snapshots - until they are answered.

**Prior art nobody re-read.** The 2022 pre-fork branch `origin/features/connect-in-pc` is the embed
direction as a runnable example app, and it is what the upstream comment quoted in the issue means
by "working hacks". The 2026 rejection of that direction was reasoned from source without opening
it.

## Delete when

astubbs#269 merges, bringing its own note onto master.
