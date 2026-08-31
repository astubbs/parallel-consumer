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
  branch, already package-renamed. Resurrection route (owner's call): **reopen the existing PR
  FIRST, then force-push** the renamed cherry-pick, rebased onto current `master`, to
  `feats/proxy-verdict-free-return`. The ordering is load-bearing - GitHub refuses to reopen a PR
  whose head branch was force-pushed after it closed. Reopening keeps the PR's review history and
  gives the stack a real `depends on` anchor.
- **The Streams forest predates the package rename** (sources still `io.confluent.*`). Not a
  deciding argument for any strategy - the rename mechanics run on predated branches routinely,
  that being how the fork migrated in the first place. It is simply a step any copy or promotion
  must include.
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

A1 and A4 are `master`-based singletons - genuinely context-free. **Everything else is one stack,
cut by partitioning the tip's tree**, in order A2 → A3 → A5 → A6/A7 → A8 → A9: rather than
disentangling content onto master-fresh branches (which forces every extraction to re-establish
context, and made the last decomposition harder than it needed to be), each rung is cut on top of
its parent largely as a path-scoped checkout from `feats/proxy-requirements` - the layers align
with module and directory boundaries, and every rung inherits the working context below it. Each
rung carries `depends on` its parent, plus astubbs#295 where it needs the verdict-free path.

**The retarget move, once the stack exists:** merge the top rung into `feats/proxy-requirements`
(both sides added the same bytes, so it mostly auto-resolves) and retarget astubbs#293's base onto
that rung. The merge-base moves, so astubbs#293's displayed diff collapses to its residue
immediately - no waiting for bottom-up merges to reach `master`. Merges to `master` then proceed
bottom-up as each rung is reviewed.

**What stays in astubbs#293:** the dispatch-wave engine, session/service orchestration,
connect-time configuration, the epoch/manifest/reconnect machinery until its seams are visible,
integration tests spanning multiple capabilities, and the plan/inflight reasoning. Its eventual
description should be able to say: build systems, protocol, conformance, packaging and language
shells are each reviewed elsewhere - *this PR reviews the remote Parallel Consumer execution
model.* That is a reviewable claim; today's is not.

The dispatch-only discipline already in the branch (every client negotiates only dispatch, while
the wire and engine carry leases, heartbeats, reconnect, drain) is the seam that makes A7-A9
cuttable: dispatch is a complete first feature slice.

**The descendants (astubbs#328, astubbs#331, astubbs#340, astubbs#334) stay as they are.** Each is
already one idea stacked on its prerequisite; their review burden is inherited ancestry, and it
shrinks as the base beneath them decomposes, without anyone touching them. astubbs#334
(`research/kafka-streams-foreign-wrappers` - foreign wrappers around *stock JVM* Streams, not
Streams-on-PC) is additionally **disposition-pending**: when its turn comes, the question is
whether it lands as a module, is re-cut as a smaller preview, or stays a research record - not how
to decompose it.

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

**B1-B7 are cut as a fresh stack** (B1 the base on `master`, each rung on its parent), by the same
tip-partition method - `feats/ks-streams-task-lifecycle-and-rebalance` is the most advanced
integration point and the natural tip to partition B2-B4 from. Promoting the existing forest
branches into PRs directly was considered and rejected: their merge topology is non-linear
(branches cut from different points, reconciled by merging the base forward, never rebased) and
several deliberately retain refutations, so stacked PRs on them would ask reviewers to read the
notebook rather than the design.

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

Start four concurrently (no conceptual overlap): **A1 (the astubbs#295 resurrection), A2 (the
Wagon A stack's bottom rung), A4 (the hygiene audit), and B1** - it touches entirely different
files. A3 starts as soon as A2's branch exists to stack on; then A5 → A6/A7 → A8 → A9, and B2 →
B3 → B4 → B5/B6/B7, each rung opening once its parent is cut (merging waits for review,
bottom-up). Reassess the residual astubbs#293 and astubbs#271 once the mechanical mass is out;
their remaining seams will be visible then, and this plan should not pretend to see them now.

Coordination state lives in `docs/inflight/process-god-branch-decomposition.md` (created with this
plan); this document is the reasoning and does not track live status.
