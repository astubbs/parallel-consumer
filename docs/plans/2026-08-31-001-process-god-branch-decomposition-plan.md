---
title: God-branch decomposition - staggered PR stacks for the proxy and Streams continents
type: process
date: 2026-08-31
topic: god-branch-decomposition
---

# God-branch decomposition: staggered PR stacks for astubbs#293 and the Streams forest

Two integration branches have grown past the point where a reviewer can hold them: the
foreign-language proxy (astubbs#293 and its descendant stack) and Kafka Streams on PC
(astubbs#271 and its sibling forest). This plan turns each into a sequence of focused PRs that
land on `master` first, leaving the god PR as the residue that reviews only its actual thesis.

The repository has already proven this model three times, and the plan deliberately copies it:

- **astubbs#322** was split into a stack of three, and the whole stack has now merged; its
  coordination record lived as an inflight note and was deleted with the work, per that
  directory's rules - read it with `git show 6b5b9d044^:docs/inflight/pr-322-split-plan.md`.
- **astubbs#363** (`perf/engine-concurrency`) is explicitly the campaign branch whose reviewable
  children were cut out as their own PRs; its intended end state is documentation residue.
- **astubbs#29** is shedding its accumulated clusters the same way - astubbs#375 and astubbs#376
  have already merged out of it.

The governing rule for what to copy out: **anything whose correctness can be reviewed without
understanding the whole.** The god PR keeps only what genuinely needs the whole in view.

## Verified state (corrections to the source analysis)

This plan was seeded by an external analysis. Its claims were checked against the repo before
anything below was written; most held, and these did not survive contact:

- **`feats/native-image-sidecar` is NOT independently landable.** It contains
  `feats/proxy-requirements` and the whole demo chain through `feats/polyglot-demos` - it is a
  short run of commits on top of that chain, a *sibling* of astubbs#340's branch
  (`feats/go-vendored-pc`), not a free-standing branch. It can be PR'd immediately only as a rung
  in the existing stack; landing it on `master` independently requires re-cutting it after the
  proxy module shell (extraction A5 below) exists on `master`.
- **astubbs#295 (verdict-free work return) is buried, not lost.** Its closing comment says the
  commit was cherry-picked onto `feats/proxy-requirements` unchanged - it is `4b4ff1968` on that
  branch, already package-renamed. Resurrection is a cherry-pick of that commit onto a fresh
  `master`-based branch, NOT a reopen of the original `feats/proxy-verdict-free-return` branch,
  which predates the package rename.
- **The Streams forest predates the package rename** (sources still `io.confluent.*`), which the
  analysis did not weigh. This is an argument *for* its copy-don't-preserve strategy: branches cut
  fresh from `master` are born renamed, and the notebook forest is never rebased (a settled
  decision - see the handover below).
- The rest checked out: astubbs#293 stacks 328 → 331 → {340, native-image-sidecar} → 334 (334 on
  340); astubbs#338 and astubbs#339 are the model extractions that already merged; the ks-streams
  branch map matches the on-branch handover,
  `git show abcc811e6:docs/inflight/branch-ks-streams-handover.md`.

## Wagon A: the proxy / foreign-language continent (astubbs#293)

astubbs#293 currently bundles the sidecar engine, the frozen protocol, eleven language clients,
the conformance matrix, and build/repository machinery. The extractions, in the order that removes
the most mechanically-reviewable bulk first:

| # | Extraction | Contents | Why it is safe to isolate |
|---|---|---|---|
| A1 | Verdict-free work return | cherry-pick `4b4ff1968` from `feats/proxy-requirements` | Core engine semantic, dependency-free by its own design ("U4"); a core primitive should not first land inside a remote-runtime PR |
| A2 | Polyglot build scaffolding | eleven language module/toolchain skeletons, hello-world fixtures, the root reactor/profile machinery (`-Dpc.foreignClients`, absent-toolchain reporting) | Mechanical, near-zero PC semantics, objectively verifiable: every language builds and runs its fixture from a clean checkout |
| A3 | Frozen protocol contract | proto module, spec, client-authoring guide, golden-byte fixtures, `buf lint`/`buf breaking` and the gate self-tests | Reviewable as a wire contract without reading the engine. Generated foreign bindings go in a follow-up (or fold into A2 if purely mechanical) - the review question is "did generation work", not "read the generated lines" |
| A4 | Hygiene audit | sweep astubbs#293 for remaining repo/build-only machinery unrelated to the proxy | astubbs#338 (copyright gate) and astubbs#339 (low-disk hook) prove this class exists; find the next ones |
| A5 | Minimal proxy module/server shell | module, executable Main, bootstrap, gRPC service lifecycle - no reconnect/lease machinery | Establishes the packaging/runtime boundary |
| A6 | Native-image sidecar | re-cut `feats/native-image-sidecar`'s delta against A5 | Executable yes/no proof; also cleans up astubbs#340's story (FFI embedding becomes "now eliminate the process boundary too") |
| A7 | Java reference client | Java API surface, direct and gRPC transports | One canonical client before eleven foreign ones |
| A8 | Conformance framework + core control arm | scenario definitions, harness, core-as-control | Makes the behavioural oracle independently reviewable |
| A9 | Eleven dispatch-only clients | thin bindings implementing negotiated dispatch only | Boring once A2+A3+A8 exist - which is the point |
| A10+ | Advanced protocol behaviours | security posture, capability negotiation, epochs/reconnect, leases/heartbeats, shutdown drain, produce path | Each deserves its own semantic review |

A1-A4 are independent of each other and of everything else: **start all four concurrently.** A5
onward is a stack (each `depends on` its parent). A6 has two routes: as an interim measure it can
open now as a rung between astubbs#331 and a re-parented astubbs#340 (that re-parenting matches
astubbs#340's own narrative); the durable route is the re-cut against A5.

**What stays in astubbs#293:** the dispatch-wave engine, session/service orchestration,
connect-time configuration, the epoch/manifest/reconnect machinery until its seams are visible,
integration tests spanning multiple capabilities, and the plan/inflight reasoning. Its eventual
description should be able to say: build systems, protocol, conformance, packaging and language
shells are each reviewed elsewhere - *this PR reviews the remote Parallel Consumer execution
model.* That is a reviewable claim; today's is not.

The dispatch-only discipline already in the branch (every client negotiates only dispatch, while
the wire and engine carry leases, heartbeats, reconnect, drain) is the seam that makes A7-A9
cuttable: dispatch is a complete first feature slice.

## Wagon B: Kafka Streams on PC (astubbs#271 and the `ks-streams-*` forest)

The forest is one feasibility study decomposed into semantic experiments - a research notebook,
not a set of merge units. Its topology, traps, settled decisions and ranked open defects are
owned by the on-branch handover: `git show abcc811e6:docs/inflight/branch-ks-streams-handover.md`
(and `docs/inflight/branch-ks-streams-workstream.md` on `master` signposts it). Two of its settled
decisions bind this plan: **merge, never rebase** the existing branches, and **the module does not
gate 0.6.0.0**.

**The branches document how the design was discovered. The PRs should document what the design
is.** So: reconstruct, do not replay. The forest stays untouched as the evidence record; each PR
below is cut fresh from `master` (born package-renamed), taking content from the forest by
copy/cherry-pick.

| # | PR | Contents | Source branches |
|---|---|---|---|
| B1 | Fork/build machinery | patch generation and regeneration discipline, upstream source/test acquisition, seam ON/OFF, publication disabled, upstream-test oracle | `feats/ks-on-pc-spike` |
| B2 | Minimal PC execution seam | PcTaskDispatcher, records through PC, wake-on-work, basic configuration | spike + `ks-streams-wake-on-work` |
| B3 | Supported semantic envelope | refuse unsupported APIs, fail clearly, EOS refusal | `ks-streams-refuse-unsupported-surface` |
| B4 | Task lifecycle + rebalance + commit frontier | ownership, completion mailbox, close/revive/suspend, rebalance | `ks-streams-task-lifecycle-and-rebalance` |
| B5 | Stream time + punctuation semantics | low-water mark, punctuation, effect survival, restart/refire, commit-coverage findings | the `stream-time-lowwater` and three `punctuator-*` branches, `postcommit-checkpoint-gap` |
| B6 | Evidence suite | seam-on upstream gate, PC integration laws, realistic-domain benchmark | `ks-streams-seam-on-upstream-gate`, `test/ks-streams-realistic-domain-benchmark` |
| B7 | Example / preview module | runnable demo | `ks-streams-pc-example` |

B1 is the best early land in the whole campaign: it asserts nothing more controversial than "we
can reproducibly patch Kafka Streams and prove the unmodified path still behaves like upstream",
and it also de-risks astubbs#269 (Connect), which piggybacks the same fork machinery.

Two constraints from the handover that the reconstruction must respect:

- astubbs#271 is blocked by unresolved review threads, several of which map to specific units
  (backpressure, stream-time punctuation, `revive()`). The reconstruction answers them PR by PR,
  in-thread, as the owning PR lands - not in one omnibus reply.
- The handover's ranked open defects (the `WALL_CLOCK_TIME` punctuator commit-coverage hole above
  all) travel WITH their owning PR - B5 does not open while claiming semantics its own evidence
  refuted.

## Wagons deliberately left alone

- **astubbs#363** already is the model - its children merged; it shrinks toward residue.
- **astubbs#29** is mid-extraction (astubbs#375, astubbs#376 merged); its own PR body carries the
  remaining cut list. No new plan needed.
- **astubbs#333** is god-shaped by ancestry, not responsibility; it collapses to a normal PR when
  the perf train lands.

## Mechanics that bind every extraction

- **Copy → review → merge → merge `master` forward into the god PR.** The god PR's diff shrinks
  only as each extraction merges and `master` is merged back in. No history surgery on
  `feats/proxy-requirements` or the forest - the payoff arrives through ordinary merges.
- **One worktree per extraction**, never the main checkout. Branch names describe the extraction
  (`feats/polyglot-build-scaffolding`, `feats/proxy-protocol-contract`), not the campaign.
- **Stacked rungs carry `depends on astubbs/parallel-consumer#N`**, one line per parent.
- **Every extraction PR names the god PR it was copied from, and the god PR gains a comment naming
  the extraction** - both directions, per the supersession-linking rule in `AGENTS.md`.
- **Roadmap stages move with the PRs.** `docs/data/roadmap.yaml` carries
  `language-proxy-sidecar`, `polyglot-proof-demo` and `streams-parallelism-preview`; an extraction
  that advances one moves its stage in the same change.
- **Extractions from the forest run `bin/rename-packages.sh` implications by construction**:
  cut from current `master`, copy content in, never merge an un-renamed branch into a renamed one.
- **Each agent dispatched onto an extraction gets the branch-context rule applied in its prompt**:
  read the source branch's own commits, PR body and comments before cutting - the proxy and
  Streams PR bodies defend, by name, decisions a fresh pair of eyes would reverse on sight.

## Sequencing

Start four concurrently (no conceptual overlap): **A1, A2, A3, A4**. B1 can also start in
parallel - it touches entirely different files. Then A5 → A6/A7 → A8 → A9, and B2 → B3 → B4 →
B5/B6/B7, each opening as its parent merges. Reassess the residual astubbs#293 and astubbs#271
once the mechanical mass is out; their remaining seams will be visible then, and this plan should
not pretend to see them now.

Coordination state lives in `docs/inflight/process-god-branch-decomposition.md` (created with this
plan); this document is the reasoning and does not track live status.
