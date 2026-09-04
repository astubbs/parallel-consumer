# InFlight adjacent systems: where it fits, and whether the standalone claim is novel

<!-- inflight-type: register -->
<!-- inflight-impact: blind-spot -->

The instrument [`ci-inflight-standalone-thesis.md`](ci-inflight-standalone-thesis.md)'s open
decision waits on. It answers one question about each system in the field, and it is not *who is
better*: **where does InFlight fit beside it, and is the thing InFlight would be worth building and
novel?** A system entering the vicinity is as likely to become an input adapter as a rival, so every
row ends with what to consume rather than compete with.

[`docs/inflight-vision.md`](../inflight-vision.md) binds this register to the rest of the harness
corpus, and its law 5 is the one the evidence rule below implements.

Opened 2026-09-04 from the conversation the thesis note captures. The two dated investigations that
precede it - [`docs/plans/2026-09-01-001-investigate-beads-comparison.md`](../plans/2026-09-01-001-investigate-beads-comparison.md)
and [`docs/plans/2026-09-02-001-investigate-adopt-or-build-re-run.md`](../plans/2026-09-02-001-investigate-adopt-or-build-re-run.md) -
own their findings; rows below cite them and do not restate them. This is the living document those
two could not be: a dated plan may not be rewritten, and this register must accrue.

## The evidence rule

Every row carries one of two marks, and the distinction is the whole point:

- **verified** - run, or read from the primary source, by someone in this repository, with the
  document that did it cited. The 09-02 re-run is the model: it ran the binary.
- **claimed** - asserted in the 2026-09-04 conversation by an external model with web access, and
  not checked here. A claimed row is a lead, not a finding. Verifying it is the first sweep's job,
  and until then it must not be quoted as established -
  [`docs/solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md`](../solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md)
  owns why a literature reading and a market reading are different claims.

**The criterion for a genuine alternative**, from the conversation and sharpened by the 09-02
finding: not *does it have a knowledge graph* or *does it track issues across branches*, but **does
it construct the same derived view** - every active branch and allowlisted fork simultaneously
queryable, disagreement between versions retained rather than reconciled, external authorities
represented rather than copied, edges carrying provenance, the result delivered on agent lifecycle
events and exposed back over MCP. A product with GitHub and Jira adapters is not close enough. A
graph over one checked-out tree is not close enough. A distributed issue tracker is not close
enough. Each may still contribute an adapter or a lesson.

## The question set - ask every system the same twelve

1. What problem does it solve, and what is its primary object model?
2. What is authoritative in it, and what is replicated?
3. What is its unit of identity, and does it survive a fork?
4. Does it index code, work state, development knowledge, or a combination?
5. Does it understand multiple repositories? Multiple refs of one repository at once?
6. Does it preserve conflicting versions, or choose a winner?
7. Does it treat forks as alternate knowledge histories?
8. Does it ingest GitHub, GitLab, Jira, Linear, Backlog as authorities, or re-home their facts?
9. Does it infer relationships over explicit ones, and does the inference carry provenance?
10. Does it inject context on agent lifecycle events, or wait to be searched?
11. Does it expose its integrated model back through MCP or an API?
12. What has its maintainer explicitly decided not to do - and could we consume it instead of
    building that capability?

Question 12 is the one that turned Backlog.md from a rival into a settled non-overlap: two of the
capabilities this tool depends on were proposed there and closed `NOT_PLANNED` (GitHub Issues sync
and lifecycle hooks - the 09-02 document's §6 owns the numbers; a third, Beads integration, was
closed for a different reason and is not a refusal of the same kind).

## The families, and the rows so far

### Git-native and distributed work trackers

| System | Relation to InFlight | Evidence |
|---|---|---|
| **Backlog.md** | Reconciles concurrent task versions to one winner; in-tree and single-truth. Tracks the work; InFlight tracks what the work knows. Input adapter candidate; never a query layer. | **verified** - 09-02 §2, §6, §9 |
| **Beads** | Out-of-tree shared store built for a fleet of agents ("50 First Dates"); correctly designed for a problem that is not this one. Its dependency graph and context injection are what `ci-issue-index-has-no-edges.md` once thought might already contain a GitHub link cache; they do not travel with a branch. | **verified** - 09-01 §8, 09-02 |
| **git-bug** | Issues as Git objects in their own refs with GitHub, GitLab and Jira bridges; the strongest prior art for keeping machine state under refs and letting fetch distribute it, which is the multi-fork mechanism the thesis note records. Imports GitHub into a parallel tracker rather than making GitHub's graph queryable. | **verified** for the bridges and ref storage - 09-01 §8, `ci-node-query-client.md` |
| **git-issue** (dspinellis) | Same shape as git-bug, own store outside the tree. | **verified** - `ci-node-query-client.md` |
| **Claude Code Tasks** | First-party, per-user, outside the repo, invisible to other harnesses and humans. *Watch, do not adopt.* This is the row the durability assumption in the thesis note points at. | **verified** - 09-01 §8 |
| **OpenClaw** | Agent-side chronological memory injected at session start; the closest to the *idea*, on the wrong axis - it follows the agent, not the branch. | **verified** - 09-01 §8 |
| **git-appraise, Bugs Everywhere, Fossil tickets** | Older distributed-metadata designs; the conversation named them as lessons in what happens when Git-as-metadata-store is pushed hard, and for the dead-project lessons current AI products lack. | **claimed** - not surveyed |

The finding these rows share, stated once in 09-02 §7 and load-bearing for the thesis: **nothing
surveyed is in-tree and multi-truth.** The axis is authority, not location.

### Forges and federation

| System | Relation to InFlight | Evidence |
|---|---|---|
| **Radicle** | Peer-to-peer Git collaboration: replicated repositories, collaborative objects for issues, patches and discussions, identities. Solves a larger and lower-level problem; the owner resized it from alternative to **input** after the conversation oversized it. Its causal-history and replicated-object machinery is worth reading for the multi-fork design. | **claimed** |
| **ForgeFed / ActivityPub, Gitea and Forgejo federation** | Cross-forge identity and federation prior art; relevant to what a fork boundary means. | **claimed** - not surveyed |
| **GitHub, GitLab** | Authorities. Own PR, issue, review and comment state; never re-homed. The tunnel that reads them is `ci-node-query-client.md`'s. | **verified** as the existing surface |

### Code intelligence and code graphs

| System | Relation to InFlight | Evidence |
|---|---|---|
| **GitNexus** | A code and system graph - symbols, calls, processes, impact - with repository groups and cross-repo contract matching. Multi-*repo* (many codebases, one system) where InFlight is multi-*fork* (many perspectives, one codebase). The owner read the project page and ruled it not a competitor; an input for symbol-level edges. The conversation read it two ways in one sitting, so nothing about its current feature set is established here. | **claimed** |
| **ckg** | Local-first code knowledge graph and MCP retrieval for coding agents; working-code structure, little workstream knowledge. A model for cheap, bounded, local MCP retrieval. | **claimed** |
| **Understand-Anything** | Has an open request for a multi-repository workspace preserving repository boundaries and evidence on inferred cross-repo edges - meaning "federated" alone is not a novelty claim. | **claimed** |
| **Sourcegraph and code search** | Code across repositories and history; code truth, not living investigations or branch disagreement. A future source of code-level edges. | **claimed** |

### Harness frameworks

| System | Relation to InFlight | Evidence |
|---|---|---|
| **sd0x-harness** (was sd0x-dev-flow) | Nearest neighbour on the *harness* thesis - tiered rules, re-injection after compaction, git-level gates - and it contradicted an earlier claim that nobody ships push-time reminders. Injects session state, not a curated repository corpus. The hooks half of the adopt question, still deferred, is about this row. | **verified** - 09-01 §8-9 |
| **karanb192/claude-code-hooks** | Guardrails; ships the config-guard this repo lacks and a dead-rules audit that is this repo's rules-into-mechanisms thesis, shipping. | **verified** - 09-01 §8 |
| **Superpowers, ECC, wshobson/agents** | Capability bundles; none ships a record of what is true about a repository now. | **verified** - 09-01 §8 |

### Work-state authorities, agent memory, and prior art that is not a product

| System | Relation to InFlight | Evidence |
|---|---|---|
| **Jira, Linear** | Authorities; adapters that preserve native structure rather than normalising to a universal task. | **claimed** as adapter targets; not surveyed |
| **Agent-memory and RAG products** | Broad ingestion, fuzzy retrieval; typically flatten provenance and version topology, so a "relevant chunk" erases exactly the disagreement this tool exists to keep. Semantic retrieval belongs *over* the provenance graph, not instead of it. | **claimed** - the category, not a named product |
| **Engineering-knowledge and developer-portal products** (PRs, tickets, docs, ownership, incidents linked to code) | The family the conversation flagged as the one most likely to overlap more than GitNexus does, and the least surveyed. | **claimed** - not surveyed; first sweep |
| **CRDTs and multi-version concurrency research** | Not products; the prior art for preserving concurrent facts without choosing a winner. | **claimed** - not surveyed |

## What this changes about the thesis

Dated entries, newest first. An entry names the row that moved and what it does to
[`ci-inflight-standalone-thesis.md`](ci-inflight-standalone-thesis.md)'s open decision.

- **2026-09-04** - register opened. The only verified rows are the trackers and harness frameworks
  the two September investigations ran or read; every code-graph, forge and portal row is a lead.
  Standing verdict, unchanged from 09-02: the individual ingredients are not novel; the potentially
  novel object is the multi-version development-knowledge graph where branches and forks are
  simultaneous perspectives, external systems stay authoritative, and inferred edges carry
  provenance. That claim is specific enough to be disproved, and the first sweep should try to.

## The first sweep, when someone runs it

Verify every *claimed* row against its primary source with the twelve questions, adding the
engineering-knowledge and developer-portal family, which was not surveyed at all. Same instrument
standard as the 09-02 re-run: run or read the source, not the marketing page. Record what each row
changes above, including "nothing" - and if one system constructs the same derived view, that is the
finding that decides the thesis note's open decision, in either direction.
