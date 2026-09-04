# InFlight as a standalone project: the thesis, the laws it would run on, and the decision left open

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-state: deferred - the physical split waits on the register's novelty verdict and a second repository of a different shape; nothing is scheduled by recording it -->

<!-- post-merge: checked-begin -->
Source: an external-model conversation on 2026-09-04 about extracting `bin/inflight.mjs` and
`bin/lib/` as an open-source project, stepped through with the owner. Captured the way
astubbs/parallel-consumer#367 captured the Codex strategy conversations, and landed on that same
branch: what it concluded beside what it got wrong, taking no decision it left open.
<!-- post-merge: checked-end -->
It asked for one instrument, and that has its own note -
[`ci-inflight-adjacent-systems-register.md`](ci-inflight-adjacent-systems-register.md), which owns
the landscape and the novelty question. This note owns the thesis, the candidate laws, the directions
the owner settled in conversation, and the list of what is still open.

The laws below are stated here and bound to the rest of the harness corpus by
[`docs/inflight-vision.md`](../inflight-vision.md), which owns the connections between these notes
and no fact any of them owns.

What it does not own, because a note already does: the tool's queued commands
([`ci-inflight-next-commands.md`](ci-inflight-next-commands.md)), the GitHub tunnel
([`ci-node-query-client.md`](ci-node-query-client.md)), the missing edges
([`ci-issue-index-has-no-edges.md`](ci-issue-index-has-no-edges.md)), and the adopt-or-build verdict
([`process-adopt-external-harness.md`](process-adopt-external-harness.md) with
[`docs/plans/2026-09-02-001-investigate-adopt-or-build-re-run.md`](../plans/2026-09-02-001-investigate-adopt-or-build-re-run.md)).

## The record this reopens, and why it is reopened rather than overridden

`process-adopt-external-harness.md` opens by saying the extraction question was asked on 2026-08-19
and answered *no*: the space had products with real distribution, so the interesting move was
adopting one. That answer was given against hooks projects and trackers. Two things have moved since.
The 2026-09-02 re-run found that the nearest tracker, Backlog.md, **reconciles** concurrent versions
to one winner (`chooseWinners` in its task loader) where this repo's contract requires the
disagreement to be **reported** - and its §7 states the axis that separates the whole surveyed field
from this tool: not where state lives but **authority**, whether a branch is allowed to disagree.
*Nothing surveyed is in-tree and multi-truth.* And the conversation captured here added a direction
the 08-19 survey never evaluated: the multi-version graph below, which changes what category the tool
is in.

**The owner's ruling, 2026-09-04: the extraction decision is reopened, not taken.** The 08-19 "no"
stays on the record with this reasoning beside it. What would settle it, in the conversation's own
terms:

1. the cross-ref engine runs against an arbitrary Git repository with no PC constants;
2. corpus paths, the baseline/live/archival ref classification and the tag vocabulary are
   configuration;
3. the core queries - divergence, prior art, stranded, branch context - work without Claude Code;
4. this repo's hooks are one adapter consuming those APIs;
5. **a second real repository uses it, chosen for a shape as unlike this one as possible** - because
   the second repo exposes every accidental PC assumption faster than speculative abstraction does.

Items 1-4 are buildable inside this repo today and are not scheduled by this note. Item 5 is the gate,
and **the evidence today is single-repository**: asked directly, the owner named nothing outside
parallel-consumer that this tool would have caught. `STRATEGY.md` already says so in its own words -
*"Whether that generalises beyond this repo is untested"* - and that sentence is the claim the
decision tests. It is not falsified, so it is not edited.

## The problem statement, with no Kafka in it

An agent working in a repository does not know only what the checked-out tree holds. It needs what
the repository knows across every active workstream. `bin/lib/prior-art.mjs`'s header carries the
measurement that made this tool exist - most of the knowledge base lives on unmerged branches, so a
working-tree search returns false negatives carrying the authority of a completed check. A human team
absorbs that by osmosis: heads, Slack, PR threads, "oh, Alice is on that". An agent arrives with the
code and nothing else, every session, and now there are dozens of them at once.

The thesis in one line: **unmerged does not mean unknowable.** In an agent-heavy repository a branch
is not only code awaiting integration; it is a concurrent working-memory shard carrying
investigations, refuted hypotheses, measurements, open decisions and cross-branch dependencies - and
Git is already the replication mechanism, so no coordination service has to be invented. An agent
records what it learned on its branch; another worktree's query finds it before anything merges.

The product test the conversation proposed, worth keeping as the acceptance test for anything built
here: **could an agent arrive cold, on the wrong branch, and still discover the important thing
another workstream already learned?**

A sharper falsifier was stated in the same conversation and is worth keeping in the form it was
restated on 2026-09-04, because it names the clause the adjacent systems cannot satisfy:

> Given 100 active branches across several trusted forks plus GitHub/Jira/etc., can the system tell
> an agent that the document it just opened has 17 divergent versions carrying additional relevant
> conclusions, connect those versions to PRs/issues/code/inferred families, and deliver the
> disagreement without reconciling it?

The figures are the test's shape, not a measurement of this repository. **Its original statement is
not captured anywhere** - the continuation says to keep it verbatim, which means a turn between the
recorded conversations was never written down; this is a restatement, and if the original surfaces it
supersedes this block. What makes it a good falsifier rather than a feature checklist: a system with
forge ingestion, a graph, contradiction detection and MCP satisfies most clauses and still fails the
first, because treating the ref topology itself as a simultaneous knowledge dimension is not
something a decision-extraction pipeline can be extended into.

The coordination failure this addresses is the one parallel agents create: A discovers assumption X
is false; B is already building on X; C fixes the cause on a third branch; D rediscovers it. An
orchestrator stuffing summaries into prompts scales worse than making the discovery an artefact
attached to the workstream and querying the whole active graph - asynchronous by construction, since
A never needs to know B exists.

## Candidate laws - recorded, not adopted

Each one is already the tool's practice or the corpus's rule somewhere; the conversation named them
as the principles a standalone project would state up front.

1. **A ref is a dimension the graph is observed through, not a node inside it.** The same logical
   document, assertion, link or semantic cluster - potentially the entire reachable graph - can
   differ by ref perspective, and Git ancestry is what relates those projections to each other. So
   the model is not *branch A contains document X* but *the graph at ref A* beside *the graph at ref
   B*. **Divergence is then native to the coordinate system rather than computed after the graph is
   built**, and the map/reduce over forks falls out for free: shared ancestry is a shared graph
   region, divergent commits are a differentiated one, identical blob ids are exact equality, and a
   fork remote is one more namespace of the same dimension. Git is not merely an input adapter in
   this half of the system - it supplies one of the coordinate systems. **This supersedes the
   version-as-node framing recorded here on 2026-09-04**; see the corrections section.
2. **Integrate knowledge, not state.** *Federate authorities, preserve disagreement, derive
   connections.*
3. **Never reconcile disagreement unless the domain genuinely has one authoritative state.** Git
   owns whether commit X is an ancestor of Y. GitHub owns whether an issue is open. A branch owns what
   that workstream currently believes. A dated investigation owns what was observed at that time.
   InFlight owns none of those facts - it owns the edges and indexes that make them jointly legible.
   A node with a "current state" property is `chooseWinners` rebuilt.
4. **If another system already owns a fact, index it; never recreate it.** Corollary: InFlight
   creates knowledge; it does not create replicas of facts merely for discoverability. A GitHub issue
   gets an adapter-maintained node, not an in-flight note written to make it greppable; a note exists
   only when the project adds something - an investigation, a hypothesis, a decision - and links to
   the node. The generated [`issue-index.md`](issue-index.md) is the interim form of this, and its
   own header already says its rows go stale silently.
5. **Could-not-ask must never look like asked-and-found-nothing.** Freshness is metadata: an issue
   node with GitHub unreachable says *state unavailable, last observed N hours ago*, and does not
   vanish from a traversal. `bin/inflight.mjs`'s header states the exit-code half of this rule.
6. **Every edge carries its provenance and its epistemic class.** Authoritative (PR heads branch),
   declared (a note says *depends on astubbs#333*), inferred (an association derived from reachable
   explicit edges, with the path that produced it). A literal `depends on` never has the same
   standing as an embedding's guess, and an agent can always ask *why does InFlight think these are
   connected?*
7. **One implementation of intelligence, many implementations of ergonomics.** Shared verbatim with
   law 4 of [`docs/w2-vision.md`](../w2-vision.md). Here it means: the kernel speaks in context
   events - session starts, a term is mentioned, a document is read, a branch is inherited, a merge
   is attempted - and Claude Code hooks, Codex, MCP and a plain CLI are adapters. Today's
   `.claude/settings.json` wires four event types (SessionStart, UserPromptSubmit, PreToolUse,
   PostToolUse) to the tool; that is the provider-neutral surface, discovered rather than designed.
8. **Do not replace the substrate; index it by the semantics the user actually needs.** The same
   instinct the Hasten corpus reached over Kafka - `docs/w2-vision.md`'s *"an inverted index, not a
   cache"* - reached again over Git. Not shared code; a shared design rule, recorded so the two
   projects can cite one statement of it.

## Directions the owner settled in conversation - none scheduled

- **Kernel and configuration.** A repository model (baseline, live and archival refs, merge bases,
  document versions), a knowledge corpus (configurable areas, metadata schema, lifecycle), queries
  (prior art, divergence, stranded, branch context, search) and delivery (CLI, machine-readable
  output, event adapters). This repo then supplies a configuration: which paths are corpora, what
  the tag vocabulary is, which refs are archival, what the baseline is. The PC-specific surface is
  already small and mostly gathered: `bin/lib/repo.mjs` holds `REPO`, `NOTES_DIR` and `DOC_AREAS`
  and its header says they are the only facts that would change - with two that escaped it, the
  Codecov URL baked into `bin/lib/codecov.mjs` and the `origin/master` baseline in
  `bin/lib/git.mjs`. The impact ranking and closed label set in `bin/lib/inflight-tags.mjs` are this
  repo's taxonomy, and become configuration rather than the product.
- **The ref-dimensional graph.** Law 1 restated as a build direction, and **corrected on
  2026-09-04**: the earlier framing here made the *version* the node - blob, path, ref context - with
  the logical document an identity grouping them. That is not wrong so much as one dimension short.
  A ref is not a coordinate stamped onto a node; it is the axis the whole graph is read along, so
  `bug-857-family.md`'s divergent copies are the same logical document seen from many ref positions,
  and ancestry relates those positions. The practical consequence is unchanged and now has a reason:
  no node may carry a current state. Entities and who owns them: refs, commits and blobs (Git); PRs, issues, reviews (GitHub);
  logical documents and workstreams (derived here); optionally the agent session that produced a
  branch (the `Claude-Session:` trailers already in the log). Edges start with what can be proven
  mechanically - Git ancestry and containment, GitHub's closes/references/heads, the corpus's own
  declared `depends on`, `supersedes`, `cites` - and inferred edges come later and weaker. The tunnel
  that fetches GitHub's half is `ci-node-query-client.md`'s, the missing edges are
  `ci-issue-index-has-no-edges.md`'s; this note adds one requirement to both: **the graph is
  multi-version or it is Backlog.**
- **Multi-fork, by refs and not by clones - the owner's ruling.** Allowlisted forks are fetched into
  a machine-owned namespace, conceptually `refs/inflight/forks/<owner>/<branch>`, with upstream
  included automatically. Git then supplies object dedup, ancestry, merge bases, fetch and prune for
  free; the query maps over each fork's divergent region only and reduces by collapsing identical
  objects while keeping differing versions with their provenance. *Reduce means deduplicate evidence,
  not reconcile truth.* The cost is ref noise, accepted in as many words: refs are for machines now.
  This is a different axis from a multi-repository code graph - many perspectives on substantially
  one codebase, not many codebases forming one system.
- **No shadow objects - the owner's ruling.** With adapters, "create an in-flight note for GitHub
  issue N" stops being a thing anyone does; the issue is already in the graph through the adapter's
  kept-fresh observation of the source system.
- **Do not normalise - the owner's ruling, cutting the model's proposed ontology.** Preserve each
  source's native type, identity, fields and relationships; normalise identity and facts only where
  they are objectively the same; let the model infer meaning from native data. Where normalisation
  earns its place is retrieval, as configurable synonym sets (issue, ticket, task, story), possibly
  suggested from the corpus with project configuration winning.
- **Two kinds of adapter, and neither is privileged.** A **primary-source** adapter carries what an
  authority *asserts* - Git, GitHub, GitLab, Jira, Linear, Slack. A **derived-intelligence** adapter
  carries what another system *inferred* - Cortex, GitNexus, Atlassian's Teamwork Graph, code
  intelligence generally. InFlight consumes both and records which is which, so an edge reads as
  *GitHub stated this* or *Cortex inferred this* or *InFlight inferred this*. That is law 6 applied
  to adapters, and it is what lets a derived system be consumed without its conclusions being
  promoted to facts.
- **Adapters in, MCP out.** Inputs: Git, GitHub, GitLab, Jira, Linear, Backlog.md, Radicle, CI
  results, and code graphs such as GitNexus and ckg for symbol-level edges - none of which InFlight
  should reimplement. Output: an MCP surface that exposes InFlight's own objects (workstream,
  evidence, divergence, concept cluster) so review bots, IDEs and other agents build on the fused
  view. Candidate queries the conversation listed: context for a symbol, related work for an issue,
  explain a relationship, divergent knowledge on a topic, workstreams touching files, authoritative
  state of an issue, knowledge since a commit, unresolved disagreements.
- **Semantic inference by propagation over explicit links.** A register that links issues,
  documents and PRs seeds a concept; the neighbourhood reachable through those explicit edges is
  labelled with it, each label carrying the path that justified it. Graph proximity, co-reference,
  shared files and symbols and citation direction do most of the work without a model; embeddings
  handle the ambiguous tail. Everything inferred is derived and disposable, recomputed when the
  inference improves.
- **Build on somebody's shell, but never on their epistemics - the 2026-09-04 continuation's
  recommendation, not a decision.** Three separate questions that must not be conflated: fork
  Backlog.md, fork Cortex, or consume either as an adapter. Its reading, recorded for the owner:
  **Backlog.md is the plausible codebase to fork**, because the boring mature surfaces are exactly
  what a standalone project would otherwise rebuild - CLI, Markdown parsing, task and document
  primitives, search, TUI and browser UI, packaging, cross-platform binaries, release
  infrastructure, tests - and because its manifesto states a governing invariant crisp enough to
  fork *against*: it exists to maintain one coherent model over human-readable Markdown, which is
  why reconciling is right for it and wrong here. **Cortex is the quarry, not the foundation**, and
  **Cortex is more valuable unforked**: integrating it demonstrates the federation thesis in a way
  copying its Slack connector never would. The shape that follows is a Backlog-derived shell, an
  InFlight kernel, and Cortex, GitNexus and the forges as sensors.
  **The one archaeology pass that decides the Backlog half**, and it is a question about their
  source rather than their features: *how deeply is winner-selection baked into loading, indexing
  and querying?* Localised in branch aggregation - fork it. Pervading every storage and query API -
  take the ideas instead, because every internal API expecting one task per id is an assumption this
  project exists to reject.
- **A strangler extraction, boundary now and mechanics later.** The mechanical split is cheap under
  the agentic cost model; the risk is freezing the wrong model. The conversation's own list of what
  not to pre-decide: what a knowledge item is; which lifecycle states are universal; whether Git
  refs remain the only coordination substrate; which query primitives are fundamental; what
  configuration belongs to the kernel versus a repository; whether automatic context injection is
  opinionated. Those are conceptual, and the second repository is what answers them.
- **Lineage, later.** Context delivery has provenance too - session, prompt term, query, document
  version, originating investigation - so a change could one day record which knowledge influenced
  it. Named, not planned; the corpus's existing worry about plausible artefacts built on a wrong
  premise is why it is worth naming.

## What it is not - the adjacent products that would be the wrong one to build

A generic agent-memory product (preferences, Slack, arbitrary company documents - the boundary is
repository-native development knowledge, versioned and inspectable with `git show`); a better
Backlog.md (it tracks the work; this tracks what the work knows); a better Linear or Jira (they are
authorities to index); a code graph (GitNexus and ckg are inputs); a forge (Radicle owns
collaboration state and replication, and is an input). The rule that keeps the scope from
metastasising is law 4.

## Positioning lines, none chosen

*Git remembers the code; InFlight remembers what the work knows.* / *InFlight lets every agent know
what the rest of the work already knows.* / *Your agents only know the branch they are looking at;
your project knows much more.* / *A live view of knowledge across concurrent Git histories.* The
conversation's advice, kept: do not lead with "knowledge graph" - it is implementation language and
puts the project in a category with hundreds of entries.

## Open decisions, all the owner's

1. **Extract at all** - reopened above, gated on the register and a second repository.
2. **The name.** InFlight is the working name throughout and is uncommitted, the way the codenames
   <!-- post-merge: checked -->
   astubbs/parallel-consumer#367 introduced are.
3. **The second repository**, and it should look nothing like this one.
   **Fork Backlog.md as the shell?** - gated on the winner-selection archaeology above, and
   deliberately separate from the extraction decision: the shell question can be answered while
   item 1 stays open.
4. **Whether context injection is opinionated in the kernel or left to adapters.**
5. **Repository, licence and the relationship to this fork** once, and if, item 1 is taken.

## What the conversation got wrong, recorded so nobody inherits it

- **Its first landscape was agent knowledge graphs only.** The owner widened it to distributed
  issue trackers, forges and federation, engineering-knowledge products and multi-agent
  coordination; the model then oversized Radicle as an alternative before the owner resized it to
  an input. The register carries the corrected families.
- **GitNexus was read twice and both readings are unverified here.** First as "users are requesting
  multi-repo", then corrected to "already shipping cross-repo impact"; then the owner, having read
  the project page, ruled it not a competitor at all. The register marks its row *claimed*.
- **The normalisation layer was over-designed** before the owner cut it - see the ruling above.
- **Every product claim about GitNexus, ckg and Understand-Anything came from a model with web
  access, and none was checked in this repo.** The register's evidence rule exists because of this.
- **The version-as-node framing was one dimension short, and it was mine, not the conversation's.**
  The 2026-09-04 capture wrote the version as the graph's native node. The continuation corrected it
  to the ref as a dimension the graph is observed through - law 1 - which is a stronger claim and
  changes what gets built, because divergence stops being a computation over nodes and becomes a
  property of the coordinate system. Recorded here rather than silently rewritten, because the
  superseded framing is the one a reader would otherwise reconstruct.
- **The shorthand changed with it.** *Multi-version knowledge graph* became **version-dimensional
  development knowledge graph** - the point being that refs are not entries in the graph beside
  issues and PRs. Both remain working phrases, and neither is the user-facing line; law 1 and the
  positioning section own those halves.
- **An assumption recorded rather than probed:** the near-term shift most likely to falsify the
  thesis is a first-party harness - Claude Code, Codex, GitHub - shipping repository-scoped or
  cross-branch memory. The 2026-09-01 survey already noted Claude Code Tasks as *watch, do not
  adopt*. The register's "what this changes" section is where that evidence lands when it arrives.
