# The InFlight vision: the laws, the boundaries, and how the notes string together

**InFlight** is the working name for the query-and-delivery half of this repository's agent harness -
`bin/inflight.mjs`, the libraries under `bin/lib/`, the `docs/inflight/` corpus they read, and the
hooks that put the answers in front of an agent. The name is uncommitted, exactly like the product
codenames [`w2-vision.md`](w2-vision.md) runs under;
[`inflight/ci-inflight-standalone-thesis.md`](inflight/ci-inflight-standalone-thesis.md) owns the
naming decision and records it as open.

**Live document, not a record.** The dated investigations under `docs/plans/` say what was known on
one day and may not be rewritten ([`citations.md`](citations.md)); this binds a note set that keeps
moving. Statements here that *are* point-in-time carry their own date inline.

## What this owns, and the rule that keeps it DRY

It owns **the generative laws and the connections between notes** - which law a claim follows from,
what implies what, what must come before what, and what contradicts what. It owns **no fact a note
owns**. A law is not a duplicate because no note owns it, and a connection is not a duplicate because
**no note can own the relation between two notes**. When a note and this file disagree about a fact,
the note wins and this file is wrong.

The same contract [`w2-vision.md`](w2-vision.md) runs under, deliberately, because this corpus had
the same disease: the laws lived inside `bin/lib/` header comments and one-line asides in
`AGENTS.md`, where nothing connected them and nothing could cite them.

**This is a sibling, not a parent.** Four topic docs already own their territory and win where this
disagrees:

- [`agent-harness.md`](agent-harness.md) **owns the layer map** - which mechanism fires when, what
  each can inject or block, and that only git hooks and CI bind non-Claude actors. Its central claim
  is that `.claude/hooks/` is runtime programming, not tooling.
- [`inflight-tool.md`](inflight-tool.md) **owns the worked examples** - what the answers look like on
  real questions, and why the working-tree version of each answer is wrong. It also carries the
  founding measurement; this file cites it rather than restating it.
- [`inflight/AGENTS.md`](inflight/AGENTS.md) **owns how a note is written** - the prefix table, the
  three tag axes and what each value means, and the four outcomes when a PR resolves a note.
- [`compound-engineering.md`](compound-engineering.md) **owns the loop** this tooling exists to
  serve: a mistake is finished when it cannot happen again without something going red.

**Nothing enforces the DRY rule, so here are its tripwires**, each naming its own fix:

- **A paragraph that names no note.** It is a fact in the wrong file; move it to the note that should
  own it, or write that note.
- **A claim restated rather than linked.** Cut it to the link. If the link is not enough, the note is
  the thing to fix.
- **A number written down that a command could answer.** [`inflight/AGENTS.md`](inflight/AGENTS.md)
  states that rule and this file is the likeliest place to break it - a corpus measurement feels like
  a finding exactly when it is about to go stale.
- **A law with no notes beside it.** Either it generates nothing - retire it - or the work it governs
  is untracked.

What *is* enforced: `bin/check-file-refs.sh` breaks when a linked note is moved or renamed.

## Sources

The primary sources are dated records and stay frozen - where one of them and a note disagree, flag
it, never silently reconcile:

- [`plans/2026-09-01-001-investigate-beads-comparison.md`](plans/2026-09-01-001-investigate-beads-comparison.md)
  and
  [`plans/2026-09-02-001-investigate-adopt-or-build-re-run.md`](plans/2026-09-02-001-investigate-adopt-or-build-re-run.md) -
  the adopt-or-build pair, the second of which drove the binary rather than reading its documentation.
- [`plans/2026-09-02-002-feat-refactor-window-signal-plan.md`](plans/2026-09-02-002-feat-refactor-window-signal-plan.md) -
  the refactor-window signal.
- The `docs/solutions/` write-ups named against each law below. Those are the incidents; the laws are
  what was left behind so nobody has to have them again.
- The header comment blocks in `bin/inflight.mjs` and `bin/lib/`, which are where several of these
  laws were actually first written down.

Two documents this file wants to cite are not on this branch yet and arrive with master: the
docs-context query plan (`docs/plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md`) and
the rank/backlog-view pair carried on `feats/inflight-rank-cli`. Reach either with
`node bin/inflight.mjs docs show <path>`, which is the tool answering a question about itself.
<!-- file-refs: N/A - both paths are named deliberately as documents this branch does not carry; the docs-context plan arrives with master and the rank pair lives on feats/inflight-rank-cli, which is the point being made -->

## The spine

One question at successively larger knowledge boundaries. Each rung is a boundary a conventional
tool stops at, and the whole thesis is that stopping there returns a false negative carrying the
authority of a completed check.

```
working tree   what does this checkout say?
ref            what does this branch believe?
repository     what does every live branch know, including the ones that disagree?
fork set       what do the other histories of this same codebase know?
authority      what do GitHub, the tracker and CI assert right now, and how stale is that?
session        what does this agent need, at the moment it needs it, without asking?
```

`grep` answers rung one. Git answers rung two. `bin/inflight.mjs` answers rung three today. Rungs
four and five are
[`inflight/ci-inflight-standalone-thesis.md`](inflight/ci-inflight-standalone-thesis.md)'s
multi-fork and adapter directions, neither built. Rung six is what the hooks already do, and it is
the rung that makes the rest worth having - a corpus nobody opens is a corpus that does not exist.

## The architectural laws

Each line is the law, then what it **governs**; what any governed claim actually says belongs to the
note named with it.

1. **Unmerged does not mean unknowable.** A branch in an agent-heavy repository is not only code
   awaiting integration - it is a concurrent working-memory shard carrying investigations, refuted
   hypotheses, measurements and open decisions. Git is already the replication mechanism, so no
   coordination service has to be invented. The founding law: the whole tool exists because a
   working-tree search was returning authoritative-looking false negatives.
   [`solutions/workflow-issues/prior-art-lives-on-branches-2026-09-01.md`](solutions/workflow-issues/prior-art-lives-on-branches-2026-09-01.md) ·
   [`inflight/ci-inflight-standalone-thesis.md`](inflight/ci-inflight-standalone-thesis.md) ·
   [`inflight-tool.md`](inflight-tool.md)

2. **Never reconcile disagreement unless the domain genuinely has one authoritative state.** Git owns
   ancestry; GitHub owns whether an issue is open; a branch owns what that workstream believes; a
   dated investigation owns what was observed that day. This tool owns none of those facts - it owns
   the edges and indexes that make them jointly legible. **The law that separates this from every
   tracker surveyed**, and the one the 2026-09-02 re-run established by driving the nearest
   competitor rather than reading about it: nothing surveyed is in-tree *and* multi-truth. Its
   failure mode has a name in someone else's source, and a node with a current state is that failure
   rebuilt.
   [`plans/2026-09-02-001-investigate-adopt-or-build-re-run.md`](plans/2026-09-02-001-investigate-adopt-or-build-re-run.md) ·
   [`inflight/AGENTS.md`](inflight/AGENTS.md) ·
   [`inflight/ci-issue-index-has-no-edges.md`](inflight/ci-issue-index-has-no-edges.md) ·
   [`inflight/ci-node-query-client.md`](inflight/ci-node-query-client.md)

3. **If another system already owns a fact, index it; never recreate it.** The corollary is the scope
   discipline that keeps the tool from becoming a worse GitHub plus a worse Jira: it creates
   knowledge, not replicas of facts merely for discoverability. The generated issue index is the
   interim form and says so in its own title - a discovery aid, not a source of truth - and its
   rows go stale silently, which is the cost this law is paid to remove.
   [`inflight/issue-index.md`](inflight/issue-index.md) ·
   [`inflight/ci-issue-index-has-no-edges.md`](inflight/ci-issue-index-has-no-edges.md) ·
   [`inflight/ci-inflight-adjacent-systems-register.md`](inflight/ci-inflight-adjacent-systems-register.md)

4. **Could-not-ask must never look like asked-and-found-nothing.** The most load-bearing law here,
   and the one with the most incidents behind it. Every section prints the size of the corpus it
   searched; a run that could not search returns a distinct code rather than a clean empty. A title
   grep is not a search. A check that reports success without having run is worse than no check. An
   instrument that could not have said yes cannot be quoted as having said no. It generalises past
   this tool - it is why `misdirection` outranks every other impact in the ranking that
   [`inflight/AGENTS.md`](inflight/AGENTS.md) owns, above data loss, because everything else is
   measured through the instruments.
   [`solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md`](solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md) ·
   [`solutions/workflow-issues/a-title-grep-is-not-a-search-2026-08-31.md`](solutions/workflow-issues/a-title-grep-is-not-a-search-2026-08-31.md) ·
   [`solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`](solutions/workflow-issues/a-check-that-reports-success-without-having-run.md) ·
   [`solutions/workflow-issues/a-harness-that-cannot-tell-never-ran-from-ran-and-agreed-2026-09-02.md`](solutions/workflow-issues/a-harness-that-cannot-tell-never-ran-from-ran-and-agreed-2026-09-02.md) ·
   [`solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md`](solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md) ·
   [`solutions/workflow-issues/an-inert-analysis-config-reads-as-a-clean-codebase.md`](solutions/workflow-issues/an-inert-analysis-config-reads-as-a-clean-codebase.md) ·
   [`inflight/ci-quarantine-lane-not-run-cannot-name-a-broken-build.md`](inflight/ci-quarantine-lane-not-run-cannot-name-a-broken-build.md)

5. **Every edge carries its provenance and its epistemic class.** Authoritative, declared, inferred -
   and never flattened together, so an agent can always ask *why does it think these are connected?*
   Governs the edge graph that does not exist yet, and the register's verified-versus-claimed
   marking, which is the same law applied to the landscape research itself.
   [`inflight/ci-issue-index-has-no-edges.md`](inflight/ci-issue-index-has-no-edges.md) ·
   [`inflight/ci-inflight-adjacent-systems-register.md`](inflight/ci-inflight-adjacent-systems-register.md) ·
   [`inflight/ci-inflight-standalone-thesis.md`](inflight/ci-inflight-standalone-thesis.md)

6. **Deliver at the moment of use; a corpus nobody opens does not exist.** A rule in a document takes
   effect only if somebody opens the document and thinks to apply it, so the query half is worthless
   without the delivery half. This is why the tool's consumers are hooks on session start, prompt
   submission and tool use rather than a command an agent must remember to run.
   [`agent-harness.md`](agent-harness.md) **owns which layer fires when** and wins on any conflict
   here; what this law adds is that context delivery is the *product*, not a convenience wrapper
   over it.
   [`agent-harness.md`](agent-harness.md) ·
   [`inflight/ci-inflight-absorbs-the-query-half.md`](inflight/ci-inflight-absorbs-the-query-half.md) ·
   [`solutions/workflow-issues/read-the-commits-you-inherit-2026-08-10.md`](solutions/workflow-issues/read-the-commits-you-inherit-2026-08-10.md) ·
   [`solutions/workflow-issues/duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md`](solutions/workflow-issues/duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md)

7. **One implementation of intelligence. Many implementations of ergonomics.** Shared verbatim with
   law 4 of [`w2-vision.md`](w2-vision.md), and the point at which the two corpora meet. Here it
   means the kernel speaks in **context events** - a session starts, a term is mentioned, a document
   is read, a branch is inherited, a merge is attempted - and Claude Code hooks, other harnesses, MCP
   and a plain CLI are all adapters over one engine. The event set is not designed; it is the one the
   hooks already discovered.
   [`inflight/ci-inflight-standalone-thesis.md`](inflight/ci-inflight-standalone-thesis.md) ·
   [`agent-harness.md`](agent-harness.md) ·
   [`w2-vision.md`](w2-vision.md)

8. **Do not replace the substrate; index it by the semantics the reader actually needs.** Git is not
   replaced any more than Kafka is in the sibling corpus, and the derived index is disposable by
   construction - throw it away and recompute when the derivation improves. The second point at which
   the two corpora agree, and the reason the standalone thesis records the parallel rather than
   proposing shared code.
   [`inflight/ci-inflight-standalone-thesis.md`](inflight/ci-inflight-standalone-thesis.md) ·
   [`w2-vision.md`](w2-vision.md)

9. **A note is where knowledge is staged, never where it is buried.** When the work a note tracks
   lands, what outlives it migrates to a durable owner first - `docs/solutions/` for a settled
   problem, `CONCEPTS.md` for vocabulary, the topic doc for a rule - and deleting is one of four
   outcomes, not the rule. [`inflight/AGENTS.md`](inflight/AGENTS.md) **owns those four outcomes**;
   what this law adds is that the lifecycle is the thing a reconciling tracker cannot express, which
   ties it back to law 2.
   [`inflight/AGENTS.md`](inflight/AGENTS.md) ·
   [`compound-engineering.md`](compound-engineering.md) ·
   [`citations.md`](citations.md)

10. **Never write down what a command can answer.** Counts of any kind included. A copied answer is
    correct until exactly one copy changes, and nothing goes red at that moment. Governs both the
    corpus (a note that tabulates open PRs is a second tracker that is wrong within a day) and the
    code (a constant written three times is the bug that produced the one file now holding the
    repository's own facts).
    [`inflight/AGENTS.md`](inflight/AGENTS.md) ·
    [`inflight/ci-pr-lookup-is-copied-into-three-hooks.md`](inflight/ci-pr-lookup-is-copied-into-three-hooks.md) ·
    [`inflight/ci-what-else-folds-into-the-rule-table.md`](inflight/ci-what-else-folds-into-the-rule-table.md)

## The rules the code states about itself

Not architectural laws - engineering constraints the implementation already enforces, gathered here
because they are scattered across header comments and a reader has no way to find them. Each is
quoted in the file that owns it; `bin/AGENTS.md` owns the shell conventions beside them.

- **The libraries return findings; the views render them; the exit code belongs at the process
  boundary.** `bin/inflight.mjs` is the only file that may exit, and the self-test asserts no library
  under `bin/lib/` contains a process exit at all. A library that exits has decided something that is
  not its to decide.
- **Git is never cached, because git is already a cache.** The cache layer is for network answers
  only, and freshness is its job rather than the caller's.
- **Exact answers only.** Blob identity over rename heuristics; plumbing over porcelain. Where git
  offers an exact answer and a heuristic one, take the exact one.
- **Adding a tool is adding a row.** A tool reachable only by knowing its filename is the state the
  front door exists to end.
- **Not a gate, and deliberately not named `check-*`.** "No prior art found" is a successful run;
  nothing in CI depends on it; and the prefix is granted to something else by pattern.
- **Nothing is remembered.** No stored verdict, no comparison against the last run - a stored answer
  is a second thing that can be wrong.

## The beats

**1. The corpus exists.** One file per item beside the code rather than beside the agent, tagged on
three axes, with the filename carrying an area and never a status. This is the part that already
works, and the part a standalone extraction would have to make configurable rather than impose.
[`inflight/AGENTS.md`](inflight/AGENTS.md)

**2. The query reaches every ref.** Prior art, note drift, stranded work and branch context over one
fan-out, with divergence defined as divergence rather than difference. The measurement that motivated
it and the traps it had to avoid are `bin/lib/prior-art.mjs`'s and `bin/lib/notes.mjs`' headers.
[`inflight-tool.md`](inflight-tool.md) ·
[`inflight/ci-node-query-client.md`](inflight/ci-node-query-client.md) ·
[`inflight/ci-inflight-next-commands.md`](inflight/ci-inflight-next-commands.md)

**3. The answers arrive unasked.** Session index, prompt-term injection, per-read divergence headers,
branch context at dispatch, push-time drift. Beat 3 is what makes beats 1 and 2 pay, and it is where
law 6 lives.
[`agent-harness.md`](agent-harness.md) ·
[`inflight/ci-inflight-absorbs-the-query-half.md`](inflight/ci-inflight-absorbs-the-query-half.md)

**4. The graph gets edges.** The tracker's nodes exist and its edges do not, so an agent can find an
issue by keyword and still not learn what it is attached to - and the edges are where the reasoning
lives. The requirement law 2 imposes: edges hang off document *versions*, never off a node with a
current state.
[`inflight/ci-issue-index-has-no-edges.md`](inflight/ci-issue-index-has-no-edges.md) ·
[`inflight/issue-index.md`](inflight/issue-index.md) ·
[`inflight/ci-node-query-client.md`](inflight/ci-node-query-client.md)

**5. The boundary widens past this repository.** Allowlisted forks fetched into a machine-owned ref
namespace; external authorities as adapters rather than shadow objects; the fused view exposed back
over MCP. None of it built, all of it recorded, and gated on the register.
[`inflight/ci-inflight-standalone-thesis.md`](inflight/ci-inflight-standalone-thesis.md) ·
[`inflight/ci-inflight-adjacent-systems-register.md`](inflight/ci-inflight-adjacent-systems-register.md)

**6. It becomes somebody else's tool, or it does not.** The extraction decision, reopened and not
taken.
[`inflight/process-adopt-external-harness.md`](inflight/process-adopt-external-harness.md) ·
[`inflight/process-quarantine-lane-foss.md`](inflight/process-quarantine-lane-foss.md)

## What the harness knows about its own failures

The tool is also a subject. These notes are open defects and gaps in the machinery this file
describes, and a session extending the harness should read the ones it is about to touch - the class
of bug here is *the instrument lied*, which law 4 ranks above everything else.

- **Guards that answer the wrong question**:
  [`inflight/ci-branch-behind-guard-answers-for-the-sessions-branch.md`](inflight/ci-branch-behind-guard-answers-for-the-sessions-branch.md) ·
  [`inflight/ci-claude-trigger-fires-on-prose.md`](inflight/ci-claude-trigger-fires-on-prose.md) ·
  [`inflight/ci-inherited-this-branch-phrases-on-master.md`](inflight/ci-inherited-this-branch-phrases-on-master.md)
- **Gaps in what the sweep reaches**:
  [`inflight/ci-nothing-asks-what-a-pr-closes.md`](inflight/ci-nothing-asks-what-a-pr-closes.md) ·
  [`inflight/ci-networked-checker-in-reviewer-grant.md`](inflight/ci-networked-checker-in-reviewer-grant.md) ·
  [`inflight/process-fork-branch-archaeology.md`](inflight/process-fork-branch-archaeology.md)
- **The review and dispatch machinery**:
  [`inflight/ci-review-agent.md`](inflight/ci-review-agent.md) ·
  [`inflight/ci-strict-review-gate-freshness.md`](inflight/ci-strict-review-gate-freshness.md) ·
  [`inflight/ci-agent-self-review-as-blocking-pr-comments.md`](inflight/ci-agent-self-review-as-blocking-pr-comments.md) ·
  [`inflight/ci-subagent-model-defaults-to-inherit.md`](inflight/ci-subagent-model-defaults-to-inherit.md)
- **Agent tooling leaking**:
  [`inflight/ci-agent-polling-loops-need-a-bound.md`](inflight/ci-agent-polling-loops-need-a-bound.md) ·
  [`solutions/workflow-issues/silent-cwd-reset-runs-git-in-the-wrong-checkout.md`](solutions/workflow-issues/silent-cwd-reset-runs-git-in-the-wrong-checkout.md) ·
  [`solutions/workflow-issues/compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md`](solutions/workflow-issues/compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md)
- **What to build next, and what is already ranked**:
  [`inflight/ci-inflight-next-commands.md`](inflight/ci-inflight-next-commands.md) ·
  [`inflight/process-candidate-ranking.md`](inflight/process-candidate-ranking.md) ·
  [`inflight/process-compounding-candidates-2026-08-20.md`](inflight/process-compounding-candidates-2026-08-20.md)

## Sequencing, in one line each

Nothing here is scheduled, and the product release run outranks all of it. The order the notes imply:
the edges before the graph, because the graph without edges is the index that already exists; the
register's first sweep before any extraction move, because it is the only thing that can disprove
the novelty claim; the kernel-and-configuration split before a physical split, because the mechanical
extraction is cheap and the conceptual boundary is not; and a second repository before anything is
called a project, because the evidence today is single-repository and the owner said so when asked.
The hooks half of the adopt-or-build question stays deferred behind the release run, untouched by any
of this.

## Risks register

Recorded 2026-09-04 so a future session can correct course. Each entry: the risk, the tell that it is
materialising, and the correction already on the record.

- **The tool is judged by its author.** Every claim about how well this works comes from the
  repository that built it, and the corpus is unusually favourable - many concurrent branches, real
  consequences when something is missed. *Tell:* a benefit asserted in a note with no incident behind
  it. *Correction:* the second-repository gate in
  [`inflight/ci-inflight-standalone-thesis.md`](inflight/ci-inflight-standalone-thesis.md), and the
  fact that `STRATEGY.md` already states the generalisation as untested rather than assumed.
- **Harness work displacing product work.** The machinery is more fun than the library, and it
  compounds visibly, which makes it feel like progress. *Tell:* a strategy or tooling weekend while
  the release run slips. *Correction:* the deferral in
  [`inflight/process-adopt-external-harness.md`](inflight/process-adopt-external-harness.md) is
  release-gated for exactly this reason, and this file's Sequencing says the same.
- **Building what somebody already ships.** The 2026-08-19 survey answered *no* to extraction on
  these grounds and was partly right; the 09-02 re-run reversed one half by running the binary.
  *Tell:* a novelty claim made from a documentation reading. *Correction:* the register's evidence
  rule, which marks every unverified row as a lead.
- **The corpus outgrowing what anyone reads.** Injected context competes for attention exactly as a
  rule in a file does; a poke that fires on everything gets skimmed like a check that is always red.
  *Tell:* an injection that fires on every call and says nothing non-obvious. *Correction:* stated in
  [`inflight/ci-issue-index-has-no-edges.md`](inflight/ci-issue-index-has-no-edges.md) as a
  constraint on the consumer, before anyone writes it.
- **This file becoming a second corpus.** The disease it was written to cure.
  *Tell:* the tripwires at the top. *Correction:* they each name their own fix, and the file-refs
  gate catches the half a machine can see.

## Open decisions, all the owner's

Extraction at all; the name; the second repository; whether context injection is opinionated in the
kernel or left to adapters; and repository, licence and the relationship to this fork if extraction
is taken. Each is recorded where it arose -
[`inflight/ci-inflight-standalone-thesis.md`](inflight/ci-inflight-standalone-thesis.md) - and this
file only lists them.
