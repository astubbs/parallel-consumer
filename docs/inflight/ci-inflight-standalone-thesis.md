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

**Superseding move, 2026-09-05: the question is no longer extract-or-not but JOIN, fork, or build -
and joining goes first.** A sweep run specifically to falsify the idea found the space converging
quickly, and the owner's stated preference is explicit: *"I'd rather join someone else's efforts."*
Three projects are close enough that source archaeology and a maintainer conversation should happen
**before** any decision to build separately - agent-memory, ctxpipe and Engram - and the register
owns their rows, the distinction in each case, and the question to put to each maintainer. If one of
them has the same architectural destination, this note's open decision is settled by becoming
unnecessary.

**The sharpest statement of the difference, and the reason the boundary might still be real.** Nearly
every neighbour **creates a curated memory store and then makes it branch-aware**. The inverse move
is the one that has not turned up: *the development environment already contains the memory - stop
hiding most of it from the agent.* An investigation on branch X should not have to be promoted into a
memory store before branch Y can find it, because the branch is already part of the knowledge
universe. **That is exactly the failure that produced this tool**, and it is now the falsifier's
decisive clause.

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

**The 2026-09-04 prior-art sweep changed the reason, not the answer.** It disproved the claim the
case had been resting on - a federated software-development knowledge graph with adapters, typed and
inferred edges and an agent interface already exists, and the register now carries a list of eleven
things that may never be claimed as novel here. What the sweep left standing is narrower and better:
the underserved problem is not graph-building but **knowledge becoming distributed across concurrent
histories faster than it can be merged**, which agents made cheap and which this repository hit in
practice before anyone designed for it. So the case for extraction rests on a coordination failure
with a worked instance, not on a category nobody has entered.

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

The falsifier, **revised 2026-09-05** after a sweep run to falsify the idea rather than support it.
The added clause is the decisive one, because it is where the memory products differ most:

> Point the tool at an existing repository with 100 active branches across several trusted forks,
> **without first curating a separate memory store**. Can an agent opening a document discover that
> 17 divergent versions contain relevant additional conclusions, connect those versions to live
> PRs/issues/code/external systems and inferred conceptual families, explain the provenance of those
> connections, and present the disagreements without reconciling them away?

The figures are the test's shape, not a measurement of this repository. **The owner endorsed the
earlier form outright** - *"Your falsifier is excellent"* - and this revision only sharpens it.

It earns its place by discriminating, clause by clause, against systems that each satisfy most of
it: agent-memory fails *without first curating a memory store*; Cortex fails the ref-dimensional
*existing repository* clause; GitLab's knowledge graph fails the development-knowledge and external
semantic context clause; Backlog.md fails *preserve divergence*; Atomic changes the underlying
version-control model rather than reading it; Atlassian Teamwork Graph goes furthest on federation
and, on what has been found, fails *Git perspectives as first-class simultaneous knowledge*.
[`ci-inflight-adjacent-systems-register.md`](ci-inflight-adjacent-systems-register.md) owns those
rows. **This is now the instrument for maintainer conversations**, not only for surveys.

## The scenario that would demonstrate it

The concrete form of the falsifier, and the thing to build a demo around rather than a feature list.
An agent is working on `fix/producer-race`, and InFlight surfaces, as one subgraph with every
assertion's origin attached:

- a divergent investigation on another local branch;
- an upstream fork's branch carrying a *different* fix;
- the GitHub PR, and a Jira incident;
- a decision Cortex extracted six months ago from a Slack thread;
- GitNexus saying the affected method sits on a particular runtime path;
- Backlog.md saying a task is complete, while another branch holds a finding that contradicts it.

**That last pair is the whole thesis in one line** - two authorities disagree, and the answer is to
show both with their provenance rather than to decide. The scenario is also the argument against
absorbing the neighbours: presenting it demonstrates that InFlight is a layer *above* heterogeneous
intelligence systems, which is a far stronger validation than having copied somebody's Slack
connector.

The coordination failure this addresses is the one parallel agents create: A discovers assumption X
is false; B is already building on X; C fixes the cause on a third branch; D rediscovers it. An
orchestrator stuffing summaries into prompts scales worse than making the discovery an artefact
attached to the workstream and querying the whole active graph - asynchronous by construction, since
A never needs to know B exists.

## The laws - stated once, in the vision doc

**[`docs/inflight-vision.md`](../inflight-vision.md) owns the laws and the connections between
notes**, which is the whole reason it exists; this note used to restate them and that was a
duplicate, caught 2026-09-05 by the owner asking what the difference between the two documents was.
The vision doc's tripwire names exactly this failure - *a claim restated rather than linked; cut it
to the link* - so it is cut.

What that file states, and this note runs on: a ref is a dimension the graph is observed through
rather than a node inside it (its law 2, and the labelled-axis half); never reconcile disagreement
unless the domain genuinely has one authoritative state (3); index what another system owns rather
than recreating it (4); could-not-ask must never look like asked-and-found-nothing (5); every edge
carries its provenance and epistemic class (6); one implementation of intelligence, many
implementations of ergonomics (8); and do not replace the substrate, index it (9).

The one line that is this note's own, because it is a compression rather than a law:
**integrate knowledge, not state** - *federate authorities, preserve disagreement, derive
connections.*

**What this note owns, and the vision doc must never state:** whether InFlight becomes its own
thing, what would settle that, what the conversations concluded and got wrong, and the decisions
still open. The vision doc binds a corpus; this is one item in it.

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
  *GitHub stated this* or *Cortex inferred this* or *InFlight inferred this*. That is the vision doc's law 6 applied
  to adapters, and it is what lets a derived system be consumed without its conclusions being
  promoted to facts.
- **Adapters in, MCP out.** Inputs: Git, GitHub, GitLab, Jira, Linear, Backlog.md, Radicle, CI
  results, and code graphs such as GitNexus and ckg for symbol-level edges - none of which InFlight
  should reimplement. Output: an MCP surface that exposes InFlight's own objects (workstream,
  evidence, divergence, concept cluster) so review bots, IDEs and other agents build on the fused
  view. Candidate queries the conversation listed: context for a symbol, related work for an issue,
  explain a relationship, divergent knowledge on a topic, workstreams touching files, authoritative
  state of an issue, knowledge since a commit, unresolved disagreements.
- **Semantic inference by propagation over explicit links - and ref identity comes first.** The
  cheapest and strongest theme signal is not inferred at all: it is the workstream's own declaration
  of what it is doing, read off the ref per the vision doc's law 2 - the commits since the merge-base, earliest
  first, then the PR title, the linked issue and any branch note, with the branch name last because
  it is a slug fixed before the work was understood. That answers *what is this divergence about* at
  **declared** strength, so a question like "what else knows about the lock problem" resolves from
  ref identity before a model is loaded, and across fork namespaces without reading content.
  Inference proper then extends it: a register linking issues, documents and PRs seeds a concept,
  the neighbourhood reachable through those explicit edges is labelled with it, and each label
  carries the path that justified it. Graph proximity, co-reference, shared files and symbols and
  citation direction do most of the remaining work without a model; embeddings handle the ambiguous
  tail. Everything inferred is derived and disposable, recomputed when the inference improves - and
  it never overwrites a declared theme, per the vision doc's law 6.
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
  **Why a fork rather than a plugin, stated precisely:** its manifesto treats its own CLI and product
  model as canonical and MCP as an optional or legacy adapter, so the semantics InFlight needs to
  mutate sit deeper than any extension point can comfortably reach - and the directions it needs were
  among the ones already declined. That combination is exactly when a friendly OSS fork is the right
  move rather than a rude one.
  **The two pipelines, side by side**, which is the real reason Cortex is a sensor and Backlog a
  shell:

  ```
  Cortex     raw event -> extraction -> canonical-ish memory object -> graph
  InFlight   native evidence -> objective normalisation -> graph layer per ref/perspective
             -> derived assertions and inferences, each with provenance -> context projection
  ```

  **The layer breakdown that follows**, recorded so a later session does not re-derive it: a
  **Backlog-derived shell** (CLI, native docs, task and work UX, Markdown lifecycle, packaging,
  browsing UI); an **InFlight kernel** (the ref-dimensional graph, multi-fork perspectives,
  divergence, provenance, semantic inference, traversal, lifecycle context delivery); and **sensors**
  - Cortex for cross-tool organisational memory, GitNexus for the structural code graph, and
  GitHub/Jira/Linear as primary authorities.
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
metastasising is the vision doc's law 4.

## The unit, and why it is not an object graph

Each neighbour names its unit differently - Atlassian *teamwork graph*, Glean *enterprise context*,
Cortex *organizational memory*, SEON *software evolution knowledge*. InFlight's is **workstream
knowledge**, and it is the one with a location and a provenance - a named branch, a named fork, one
PR, one investigation, one review, one CI result. Critically, workstreams can know contradictory
things.

That is why the model is better described as **objects plus assertions and perspectives over those
objects** than as an object graph, and it is the architectural reason behind the no-normalisation
ruling rather than a taste for it. Atlassian folds a Jira item and an Asana work item into one common
work-item type; the alternative here is to keep `native-type = JiraIssue` and *derive* the
neighbourhood `issue / work-item / ticket` when a query needs it. Cheaper, and it preserves the
information the fold discards.

## Positioning lines, none chosen

*Git remembers the code; InFlight remembers what the work knows.* / *InFlight lets every agent know
what the rest of the work already knows.* / *Your agents only know the branch they are looking at;
your project knows much more.* / *A live view of knowledge across concurrent Git histories.* The
conversation's advice, kept: do not lead with "knowledge graph" - it is implementation language and
puts the project in a category with hundreds of entries.

## Open decisions, all the owner's

1. **Join, fork, or build** - restated 2026-09-05, and joining is evaluated first. Gated on the
   archaeology and maintainer conversations the register lists, then on a second repository. The
   older form of this decision was *extract at all*, which assumed building was the only path.
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
  to the ref as a dimension the graph is observed through - the vision doc's law 2 - a stronger claim, and
  changes what gets built, because divergence stops being a computation over nodes and becomes a
  property of the coordinate system. Recorded here rather than silently rewritten, because the
  superseded framing is the one a reader would otherwise reconstruct.
- **The shorthand changed with it.** *Multi-version knowledge graph* became **version-dimensional
  development knowledge graph** - the point being that refs are not entries in the graph beside
  issues and PRs. Both remain working phrases, and neither is the user-facing line; that law and the
  positioning section own those halves.
- **An assumption recorded rather than probed:** the near-term shift most likely to falsify the
  thesis is a first-party harness - Claude Code, Codex, GitHub - shipping repository-scoped or
  cross-branch memory. The 2026-09-01 survey already noted Claude Code Tasks as *watch, do not
  adopt*. The register's "what this changes" section is where that evidence lands when it arrives.
