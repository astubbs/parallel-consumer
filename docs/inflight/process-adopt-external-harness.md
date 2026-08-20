# Extracting the agent harness as FOSS - researched, and mostly answered NO

<!-- inflight-type: feature -->
<!-- inflight-state: closed - the space is occupied; what is left is one hook to steal and a convention worth contributing -->

**Signpost, not a plan.** The agent harness has grown into something product-shaped and nothing
tracked that, so this exists to stop it being rediscovered.

## What there is

Seven hooks in `.claude/hooks/` (session-start knowledge injection, the merge guard, the push
reminder, the history-rewrite guard, the pre-commit gate, the squash-subject guard, the merge-checklist
injector), fifteen `bin/check-*.sh` gates, and thirteen `bin/test-check-*.sh` self-tests. Plus the
inflight tracker the index reads - the tag vocabulary, its gate, and `docs/inflight/AGENTS.md`.

**Almost none of it is parallel-consumer-specific**: each hook mentions Kafka or this project at most
once, and those are examples in comments. The gates are about *how a repository is worked on* - do
citations resolve, has a human reviewed it, can this gate fail, is work in flight - not about
consumers or offsets.

## Researched 2026-08-19: do not build this

Prior-art search found the category already has a name we did not give it - **"harness engineering"**,
coined February 2026, with an awesome-list, a compatibility matrix and published pieces from Anthropic
and Martin Fowler. Two of our three layers are crowded:

- **Hooks as a product: occupied.** [`sd0xdev/sd0x-dev-flow`](https://github.com/sd0xdev/sd0x-dev-flow)
  (188 stars) describes itself as "the harness layer for Claude Code" - six hooks over five events, a
  five-layer chain, state gates that survive context compaction.
  [`karanb192/claude-code-hooks`](https://github.com/karanb192/claude-code-hooks) (480 stars) ships
  twenty marketplace-installable plugins. Its `dead-rules-audit` tallies which rules the agent ignores
  and flags them "to promote into a deterministic hook" - **that is our central thesis, already
  shipping as someone else's product.**
- **The tracker: decisively occupied.** [Beads](https://github.com/steveyegge/beads) has 26.5k stars,
  dependency graphs, context injection and memory decay. Backlog.md, git-issues, tkr and ai-trackdown
  cover the markdown-file variant. A fifth is not defensible.

**And the differentiator is weaker than this note originally claimed.** "Gates that prove they can
fail" is known and advocated - tslint documented rule-testing years ago, and
[`dshakes/compass`](https://github.com/dshakes/compass) is eval-gated in CI against a 61-case labelled
corpus plus a 147-case *bypass* corpus. What nobody matches is the density - thirteen
`test-check-*.sh` paired one-to-one with their gates - so the **discipline** is distinctive while the
**idea** is not. Two smaller claims did survive the search: nothing found makes a filtered view
declare what it hid, and nothing carries incident provenance inside the guard itself. Both are real
and both are thin as a product thesis.

## What to do instead

- **Steal one hook.** `karanb192`'s `config-guard` / `instructions-audit` stop the agent editing its
  own hooks and settings. We have no such guard, and every hook here was written by an agent - that is
  a real hole, and one hook closes it.
- **Contribute the pairing convention rather than launch a project.** "A gate you have not watched go
  red is not a gate" is small enough to land in an existing collection, where it would reach people.
- Everything comparable is MIT, so adopting rather than rebuilding is open. One exception:
  `netresearch/agent-harness-skill` dual-licenses its *documentation* CC-BY-SA-4.0, so lifting its
  prose would infect ours.

Full search list, negative results with the searches that produced them, and the eight repositories
read are in the research output; the conclusions above are what survived.

## Why it looked worth extracting

The differentiator is the same one [`process-quarantine-lane-foss.md`](process-quarantine-lane-foss.md)
claims for its subset: the loop is closed **in CI**, not in a document. Every gate has a self-test
verified red against a broken version; the session index refuses to hide what it filtered; the guards
name what would be lost rather than merely refusing. That discipline is the product, more than the
shell.

## What makes it hard, and why it is deferred

- **It is coupled to one harness's hook contract** - `PreToolUse` payloads, `permissionDecision: deny`
  on stdout, `additionalContext`. Extracting means either pinning to that or inventing an abstraction
  over a contract that still moves.
- **The conventions are half the value and do not travel as code.** The tag vocabulary, the
  now-vs-later boundary, "a filename carries no status" - a repo adopting the scripts without those
  gets gates enforcing rules it never agreed to.
- **It is still changing weekly.** Three of the seven hooks were written or substantially fixed in a
  single session, and two gates gained bypass fixes in the same window. Extracting a moving target
  means maintaining two copies of something not yet settled.

Deferred until after v6 and until the harness stops changing that fast. The quarantine-lane note is a
narrower version of this question and should be decided together with it, not separately - one of
them is a subset of the other.
