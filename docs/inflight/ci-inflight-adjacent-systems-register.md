# InFlight adjacent systems: where it fits, and whether the standalone claim is novel

<!-- inflight-type: register -->
<!-- inflight-impact: blind-spot -->

The instrument [`ci-inflight-standalone-thesis.md`](ci-inflight-standalone-thesis.md)'s open
decision waits on. It answers one question about each system in the field, and it is not *who is
better*: **where does InFlight fit beside it, and is the thing InFlight would be worth building and
novel?** A system entering the vicinity is as likely to become an input adapter as a rival, so every
row ends with what to consume rather than compete with.

[`docs/inflight-vision.md`](../inflight-vision.md) binds this register to the rest of the harness
corpus, and its law 6 is the one the evidence rule below implements.

Opened 2026-09-04 from the conversation the thesis note captures. The two dated investigations that
precede it - [`docs/plans/2026-09-01-001-investigate-beads-comparison.md`](../plans/2026-09-01-001-investigate-beads-comparison.md)
and [`docs/plans/2026-09-02-001-investigate-adopt-or-build-re-run.md`](../plans/2026-09-02-001-investigate-adopt-or-build-re-run.md) -
own their findings; rows below cite them and do not restate them. This is the living document those
two could not be: a dated plan may not be rewritten, and this register must accrue.

## The evidence rule

Every row carries one of three marks, and the distinction is the whole point. The third was added
2026-09-04 when the first sweep ran, because collapsing it into either neighbour would have been a
lie in one direction or the other:

- **verified** - run, or read from the primary source, by someone in this repository, with the
  document that did it cited. The 09-02 re-run is the model: it ran the binary.
- **surveyed** - read from the system's own published sources - its docs, README, release notes,
  source - by the 2026-09-04 sweep, but **not run**. Stronger than a lead, weaker than a finding,
  and it must say so: the 09-01 survey was documentation-derived, concluded something about a market
  from evidence about a literature, and had to retract. A surveyed row is good enough to rank
  against and not good enough to settle a build decision on.
- **claimed** - asserted in conversation and not checked anywhere. A lead. Until verified or
  surveyed it must not be quoted as established -
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

## Two classes of system, and neither is privileged

A distinction added 2026-09-04, because it decides what a row *means* rather than how it scores. A
**primary-source** system carries what an authority asserts - Git, GitHub, GitLab, Jira, Linear,
Slack, Radicle. A **derived-intelligence** system carries what something else inferred - Cortex,
GitNexus, ckg, Atlassian's Teamwork Graph, Glean. InFlight consumes both and records which is which,
so a derived system's conclusions enter the graph as *inferred by X* and never as facts. A row's
class is noted where it is not obvious.

## What may never be claimed as novel

The sweep's most useful output, and the reason to keep it at the top: every one of these has
abundant prior art, and any pitch containing one is disprovable on sight.

*A software-development knowledge graph. Federated developer data. GitHub plus Jira plus Slack plus
docs in one graph. MCP context for coding agents. Semantic relationships between engineering
artifacts. Multi-repo context. Git-native distributed metadata. Branch-aware agent memory.
Contradiction detection. Provenance-aware knowledge graphs. A software-evolution graph.*

**"Nobody has built a federated software knowledge graph" is false**, and was the shape of the claim
before the sweep. What survives is narrower and stated below.

## What still looks unusual, after trying to find it

Eight properties. None is individually novel; the perspective-preserving *combination* is what has
not turned up:

1. **Every relevant Git ref is live knowledge** - not only checked-out HEAD, not only the baseline.
2. **Forks are admitted as additional perspective namespaces**, in one searchable object universe.
3. **Divergence is retained rather than reconciled** - two conflicting versions are two facts about
   perspectives, not candidates for a winner.
4. **Source authorities stay authoritative** - indexed, never replaced.
5. **Explicit links bootstrap semantic inference**, seeded from the corpus's own registers.
6. **Inferred relationships retain provenance**, so the model can say why.
7. **Context is delivered by what the agent is doing now** - not primarily a dashboard or a search
   product.
8. **The fused graph is itself a service to other tools** over MCP.

**Better framing than "knowledge graph"**, which is implementation language and lands in a category
with hundreds of entries: *a live index of the knowledge distributed across software-development
workstreams*. And the unit each neighbour names differently is worth owning - Atlassian has
*teamwork graph*, Glean *enterprise context*, Cortex *organizational memory*, SEON *software
evolution knowledge*; InFlight's is **workstream knowledge**, which is the one that has a location
and a provenance, and which can hold contradictory beliefs.

That suggests the core model is not an object graph but **objects plus assertions and perspectives
over those objects** - which is the architectural reason not to normalise. Atlassian folds a Jira
item and an Asana work item into one common work-item type; InFlight would keep `native-type =
JiraIssue` and *derive* the neighbourhood `issue / work-item / ticket` when it is useful.

## The axis that actually classifies this field

Added 2026-09-05 by the second sweep, and it is sharper than the four lineages below. Nearly every
system here **creates a curated memory store and then makes it branch-aware**. InFlight's move is the
inverse: **the development environment already contains the memory - stop hiding most of it from the
agent.** An investigation written on branch X does not have to be promoted into a memory store before
branch Y can find it, because the branch is already part of the knowledge universe. That is precisely
the failure that produced this tool.

So the question to ask every row is no longer *is it branch-aware* - many now are - but: **does
knowledge have to be authored into its store first, or does it discover what the refs already hold?**

## Four lineages, and the intersection

The defensible claim, replacing the disproved one. InFlight sits where four existing lineages meet,
and each already owns its half far better than a new project would:

- **Atlassian Teamwork Graph** - federation, graph and agents.
- **Radicle** - distributed histories and perspectives.
- **Cortex** - cross-tool memory, contradiction detection, proactive injection.
- **Backlog.md** - Git-native work knowledge in the tree.

## The families, and the rows so far

### Git-native and distributed work trackers

| System | Relation to InFlight | Evidence |
|---|---|---|
| **Backlog.md** | Reconciles concurrent task versions to one winner; in-tree and single-truth. Tracks the work; InFlight tracks what the work knows. Input adapter candidate; never a query layer. **Also the leading fork candidate for the *shell*** - MIT, mature CLI, Markdown parsing, search, TUI and browser UI, packaging, tests - with its manifesto's one-coherent-model invariant as the crisp thing to fork against. Gated on the winner-selection archaeology the thesis note names. | **verified** for the reconcile finding - 09-02 §2, §6, §9; the fork option is **claimed**, and the archaeology has not been run |
| **Beads** | Out-of-tree shared store built for a fleet of agents ("50 First Dates"); correctly designed for a problem that is not this one. Its dependency graph and context injection are what `ci-issue-index-has-no-edges.md` once thought might already contain a GitHub link cache; they do not travel with a branch. | **verified** - 09-01 §8, 09-02 |
| **git-bug** | Issues as Git objects in their own refs with GitHub, GitLab and Jira bridges; the strongest prior art for keeping machine state under refs and letting fetch distribute it, which is the multi-fork mechanism the thesis note records. Imports GitHub into a parallel tracker rather than making GitHub's graph queryable. | **verified** for the bridges and ref storage - 09-01 §8, `ci-node-query-client.md` |
| **git-issue** (dspinellis) | Same shape as git-bug, own store outside the tree. | **verified** - `ci-node-query-client.md` |
| **Claude Code Tasks** | First-party, per-user, outside the repo, invisible to other harnesses and humans. *Watch, do not adopt.* This is the row the durability assumption in the thesis note points at. | **verified** - 09-01 §8 |
| **OpenClaw** | Agent-side chronological memory injected at session start; the closest to the *idea*, on the wrong axis - it follows the agent, not the branch. | **verified** - 09-01 §8 |
| **git-appraise** | **Direct prior art for the machine-ref decision.** Stores review requests, comments, CI information and analysis in machine-oriented refs under `refs/notes/devtools/...`, explicitly because humans are not meant to interact with those refs - which is the owner's *refs are for machines now* ruling, already shipped by somebody else. It validates bringing trusted fork remotes into an InFlight-controlled namespace rather than cloning into subdirectories, and letting Git do dedup, ancestry and transport. Owns review state narrowly; not an alternative. | **surveyed** 2026-09-04 |
| **Bugs Everywhere, Fossil tickets** | Older distributed-metadata designs; named as lessons in what happens when Git-as-metadata-store is pushed hard, and for the dead-project lessons current AI products lack. | **claimed** - not surveyed |

The finding these rows share, stated once in 09-02 §7 and load-bearing for the thesis: **nothing
surveyed is in-tree and multi-truth.** The axis is authority, not location.

### Forges and federation

| System | Relation to InFlight | Evidence |
|---|---|---|
| **Radicle** | Peer-to-peer Git collaboration: replicated repositories, collaborative objects for issues, patches and discussions, identities. Solves a larger and lower-level problem; the owner resized it from alternative to **input**. Its collaborative-object histories are unioned **non-destructively** across peers, and Radicle itself compares the model to CRDT behaviour - so it is the closest existing thing to the distributed-histories-and-perspectives half. **Study the COB internals before inventing any InFlight-native replicated object**: causal histories, non-destructive union, signed provenance, per-peer namespaces, concurrent updates. | **surveyed** 2026-09-04 from radicle.dev |
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

### Organisational memory and derived-intelligence systems

| System | Relation to InFlight | Evidence |
|---|---|---|
| **Cortex** | Apache 2.0, so forkable, and a buffet of things InFlight eventually wants: GitHub/Jira/Linear/Slack connectors, event ingestion, a Neo4j graph, semantic and temporal/episodic stores, contradiction detection, trust scoring, context ranking and injection, MCP, causal chains. **Its epistemic stance is close to opposite**: it says *capture decisions, not documents*, extracting normalised memory objects from source events and treating those as the organisational memory - where InFlight preserves the primary evidence and native structure and records inference as a derived, provenanced edge over it. So forking it means inheriting an architecture centred on the normalised store you then spend effort un-centering. Recommended treatment, 2026-09-04: **integrate rather than fork**, quarry the connectors, contradiction heuristics, ranking and MCP shape, and make every imported part submit to InFlight's epistemic model. It also cannot satisfy the falsifier's first clause - the ref topology as a knowledge dimension is not something a decision-extraction pipeline extends into - which is the evidence that the falsifier tests architecture rather than features. Derived-intelligence class. | **surveyed** 2026-09-04 - read from its own README and repository, not run. The sweep confirms the extraction model (decisions with status, rationale, people, affected systems and causal triggers, into Neo4j) and confirms it is extremely young at one star, so its README is not market validation. The risk it names is the one that matters: **the extraction layer becomes an arbiter of what the source meant** - excellent prior art for contradiction detection, temporal memory and active injection, and the wrong truth model to copy |
| **Atlassian Teamwork Graph** | **The closest system found, and the one to watch** - closer than GitNexus, Radicle or any distributed tracker, which is the sweep's most consequential correction. A unified graph over work items, documents, messages, branches, commits and pull requests, including third-party objects; a relationship model that already distinguishes **canonical, activity, logical and inferred** edges; and, from August 2026, multi-repository code understanding pushed into that graph specifically so coding agents can reason about code together with the surrounding work, through its CLI and plugins. Overlaps the federation-plus-graph-plus-agents half almost entirely. **The difference is fundamental rather than cosmetic, and is the first claim to try to falsify**: it appears object-centric and current-state-centric - a branch is an *object* in the graph - where InFlight's move is that a branch or fork is a *perspective from which an entire body of knowledge may differ*. The sweep looked specifically for multi-ref or fork epistemic state and found branches represented and related to repositories and work items, but no retention of simultaneously divergent repository knowledge. Code Context entered open beta recently, so this row goes stale fastest of any here. | **surveyed** 2026-09-04 from Atlassian developer documentation and release notes |
| **Glean** | Enterprise search and context: connects GitHub/GitLab, Jira/Linear, Slack, Confluence/Notion and Drive, and exposes that to engineering agents over MCP, pitched as starting every task with the full picture. **This is why "connect all the engineering systems and retrieve context for a coding agent" is established commercial territory and not an InFlight claim.** A retrieval and search layer, not a multi-version development-history model. | **surveyed** 2026-09-04 from its own documentation |

### The nearest neighbours - candidates to JOIN, not to survey

Found by the 2026-09-05 sweep, which was run to falsify the idea rather than support it. These are
the rows that could make building InFlight separately the wrong move, and the owner's stated
preference is to join an existing effort where the architectural destination matches.

| System | How close | The distinction, and the question to put to its maintainer | Evidence |
|---|---|---|---|
| **agent-memory** | **Strongest join candidate.** Local, Git-native project memory for coding agents; its current release is a *federation* release - repositories reference Git-pinned shared "landscape" stores, retrieval blends them while preserving source and commit provenance and trust boundaries, plus branch-scoped working notes and a section-aware Git merge driver for concurrent edits. Philosophically extremely compatible: Git-native, local, reviewable, provenance-preserving, MCP. | It **curates** a memory store and branches it. Ask: *would you treat all active refs as simultaneously queryable project knowledge, including conflicting versions, rather than requiring knowledge to be promoted into the store first?* A yes makes joining the obvious move; a no makes the boundary real. | **surveyed** 2026-09-05 |
| **Engram** | **Closest on the branch insight itself** - it says outright that Git branches encode work-in-progress boundaries prior memory tools ignore, and supports querying across all branches, with contradiction detection and relationship graphs. | Its memories *have* a branch attribute; the proposal here is that the branch is a **dimension over the development graph**, so everything reachable from a ref contributes - docs, code, commits, cross-links, PR associations. Potentially a very short conceptual distance. | **surveyed** 2026-09-05 |
| **ctxpipe** | **Closest on the federated engineering-context graph**: repos plus docs plus tools into a self-learning org-scoped knowledge graph behind one MCP, with Git as the source of truth for decisions and instructions. | Unknown and decisive: what does repo ingestion actually mean - HEAD, history, branches, PR heads, forks? Does it preserve divergent knowledge? **Source archaeology, not a README read.** | **surveyed** 2026-09-05 |

### Systems that validate a piece of the design

| System | What it establishes | Evidence |
|---|---|---|
| **Atomic** | Replaces Git's change model with a semantic change graph, and its terminology is **"Views, Not Branches"** - each agent gets an isolated view of one underlying graph, with agent-turn provenance and intent. **This is the branch-as-perspective correction, independently reached.** The difference is direction: Atomic says Git's model is insufficient and replaces branches with graph views; the position here is that Git branches already *are* perfectly good graph views, so use them. Not a join candidate - it attacks source control itself, a far larger project - but its design documents on view identity, provenance and agent concurrency are worth reading. | **surveyed** 2026-09-05 |
| **GitLab Knowledge Graph / Orbit** | **Evidence the all-branches problem is recognised, not imagined.** GitLab's design explicitly discusses indexing active branches, focuses on the default branch first because indexing every active branch at their scale means an enormous number of definitions and relationships, and proposes storing active-branch graph data cold and materialising on demand. **Their framing is N versions of the code graph; the optimisation available here is that Git already says precisely what diverged** - a shared base plus divergent overlays, which the substrate almost forces. Primarily a code graph rather than a development-knowledge graph. | **surveyed** 2026-09-05 |
| **Mnemograph** | Persistent event-sourced knowledge graph for coding agents where the memory itself can branch, commit, diff and revert - relevant prior art for versioned graph semantics. | **surveyed** 2026-09-05 |
| **ForgeDock** | Treats GitHub issues, PRs and comments as persistent structured agent knowledge. Essentially one very good InFlight input dimension, already built. | **surveyed** 2026-09-05 |
| **2context** | Git history into a provenance-bearing knowledge graph, generic adapter architecture, PRs/issues/ADRs planned. Oriented toward extracting a graph *from* history rather than preserving simultaneous ref perspectives. | **surveyed** 2026-09-05 |
| **memory-mcp** | Branch-scoped recent work, `branch:*` queries, shared across worktrees. Its own memory, not arbitrary branch knowledge. | **surveyed** 2026-09-05 |
| **Codastre, Symvanta** | Branch-aware code knowledge graphs; primarily code intelligence, so inputs rather than alternatives. | **surveyed** 2026-09-05 |
| **BranchMind** | Git-style version control for LLM *conversations* - parallel branches with a shared knowledge graph and cross-branch queries. Wrong domain, but branch-dimensions plus shared graph plus cross-branch retrieval already exists as an idea. | **surveyed** 2026-09-05 |

### Coding-agent memory - a crowded family, and closer than first thought

| System | Relation to InFlight | Evidence |
|---|---|---|
| **agent-memory** | Local, Git-native project memory for coding agents: plain Markdown as source of truth, branch-aware, MCP-served, structured updates. | **surveyed** 2026-09-04 |
| **agentmemory** | One shared memory server across many coding agents, with knowledge graphs, confidence, lifecycle and hooks. | **surveyed** 2026-09-04 |
| **repo-agent-context** | **The closest of this family.** Snapshots GitHub/GitLab issues, PRs, CI, comments, diffs and branches-ahead into local Markdown/JSON, and detects issue-to-PR textual relationships for agents. A snapshot and export model rather than a live multi-version graph. | **surveyed** 2026-09-04 |

The family's collective lesson: persistent agent memory, branch-aware context, local Git-backed
memory, MCP exposure, proactive hooks and forge-data snapshots are **all** established. What none of
them appeared to do is treat the complete set of active refs and forks as a distributed knowledge
corpus in itself.

### Software-evolution research and data platforms

| System | Relation to InFlight | Evidence |
|---|---|---|
| **SEON** (2012) | An ontology of software evolution: source code, version control, issue trackers, developers and changes, with cross-domain relations so *which release fixed this bug and which source changes implemented it* traverses sources. **Shows the problem is at least fourteen years old**, and it already diagnosed the trap InFlight is avoiding - a central repository database imposing one rigid universal schema, which is why it built layered extensible ontologies instead. The 2026 improvement on it is precisely not to formally ontology-model what can now be handed to a model: normalise what is objectively true, preserve native data, give inference provenance. The LLM is what makes the last semantic mile affordable without the enormous ontology. | **surveyed** 2026-09-04 |
| **GrimoireLab** | Years of ingesting Git, GitHub, Jira, Bugzilla, GitLab, Slack, mailing lists, Jenkins and more, enriched and queryable. **Proves "build adapters for all the developer systems" is not novel either**, and that an adapter ecosystem is a project in its own right - do not underestimate source heterogeneity. Its purpose is metrics and analytics, so it is an adapter-experience teacher rather than a rival. | **surveyed** 2026-09-04 |

### Memory research, which is prior art rather than product

Reached through Cortex's own references and worth being nodes in this register rather than a
dead end at the project that cited them: **Graphiti / Zep**, **A-MEM**, **MAGMA**. All **claimed**,
none surveyed. The question to ask each is the register's usual twelve, but the one that matters is
whether any of them treats concurrent versions as simultaneously true rather than as a history to
collapse.

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

- **2026-09-05, second sweep, run to falsify rather than to support.** The honest answer to *are you
  sure nobody is attempting this* is now **no**. The space is converging fast, and several people
  have independently reached pieces of the same realisation - agents need persistent project
  knowledge, Git gives identity and versioning, branches matter, development systems hold the
  context, graphs help, MCP is the delivery interface. That much is zeitgeist, not insight. Engram
  reached the branches-are-knowledge-boundaries point independently; Atomic reached
  views-not-branches independently; GitLab is designing active-branch indexing and treating scale as
  the obstacle. **The claim narrows again**, to the curate-versus-discover axis above, and the
  number of qualifiers now needed to state it is itself the argument against claiming any large
  fundamental novelty. The likely true situation: many teams walking to the same destination from
  different directions, and this route starts from Git's existing multi-version topology rather than
  from a new memory database. **Consequence for the open decision: joining now precedes building.**
  Three candidates for source archaeology and then a maintainer conversation - agent-memory, ctxpipe,
  Engram - and the falsifier below is the instrument for those conversations.
- **2026-09-04, first sweep run.** The sweep the section below asked for was run, and it moves the
  thesis more than any single row. **The novelty claim as it stood is disproved**: a federated
  software-development knowledge graph exists, has adapters, typed and inferred edges, and serves
  coding agents - see the not-may-be-claimed list above, which is now the register's most-used
  section. **Atlassian Teamwork Graph replaces Backlog.md as the closest system**, which nothing
  before this had suggested; the falsifier's first clause is the only place it visibly does not
  reach, and that is now the single claim to attack. What survives is the eight-property
  combination and the four-lineage intersection, both above - a much more defensible claim than the
  one it replaces, because it names who owns each half. Two implementation decisions gained external
  validation rather than argument: git-appraise already ships machine-only refs for exactly the
  owner's stated reason, and Radicle already solved non-destructive union of concurrent
  collaborative-object histories, so neither should be invented here. SEON says the problem is
  fourteen years old and names the schema trap; GrimoireLab says the adapter universe is real work.
  **The verdict is still yes, worth taking seriously - but for a different reason than novelty of
  the graph**: the underserved thing is the multi-agent coordination failure, where knowledge
  becomes distributed across concurrent histories faster than it can be merged, which agents made
  cheap and which this repository hit in practice before anyone designed for it.
- **2026-09-04, continuation** - Cortex added, and it moves the thesis rather than only the table.
  It is the first surveyed system that plausibly satisfies several falsifier clauses at once - forge
  and chat ingestion, a graph, contradiction detection, MCP - which makes the clause it *cannot*
  satisfy the sharp one: the Git ref topology as a simultaneous knowledge dimension. That
  strengthens the novelty claim by narrowing it, and it is also the first row whose right treatment
  is *integrate to prove the thesis* rather than *survey and dismiss*. Two adjacent moves recorded
  with it: the primary-source versus derived-intelligence split above, and Backlog.md becoming a
  candidate **shell** as well as an adapter, which is a decision the register cannot settle - it
  needs the winner-selection archaeology, not a comparison. The memory-research leads arrived the
  same way and are unsurveyed.
- **2026-09-04** - register opened. The only verified rows are the trackers and harness frameworks
  the two September investigations ran or read; every code-graph, forge and portal row is a lead.
  Standing verdict, unchanged from 09-02: the individual ingredients are not novel; the potentially
  novel object is the multi-version development-knowledge graph where branches and forks are
  simultaneous perspectives, external systems stay authoritative, and inferred edges carry
  provenance. That claim is specific enough to be disproved, and the first sweep should try to.

## The next sweep, and it is not a sweep

The 2026-09-05 pass changed the shape of the remaining work. Surveying more products has diminishing
returns; the open questions now need **source archaeology and conversations**, in this order:

1. **agent-memory** - read the federation release properly, then ask its maintainer the question in
   its row. This is the one that could make building separately the wrong move.
2. **ctxpipe** - source archaeology on what repo ingestion means. HEAD, history, branches, PR heads,
   forks? Divergence preserved or collapsed? A README cannot answer this.
3. **Engram** - whether it would move from *memories scoped to branches* to *branches as dimensions*.
   Possibly a short conceptual distance, and worth asking rather than assuming.
4. **Backlog.md winner-selection archaeology** - a source reading that answers a build decision.
5. **Atomic's design documents** and **Radicle's COB internals** - read before inventing view
   identity, provenance or replicated-object semantics here.
6. **Atlassian Teamwork Graph** stays the watch item and the fastest-staling row.

Untouched leads: Graphiti/Zep, A-MEM, MAGMA. Nothing in this register except the Backlog.md
reconcile finding has been *run*.

Record what each changes above, including "nothing". If one of the three join candidates has the same
architectural destination, that settles the thesis note's open decision by making it unnecessary.
