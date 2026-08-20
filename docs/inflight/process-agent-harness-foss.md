# Extract the agent harness as its own FOSS project

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - after v6, and after the harness stops changing weekly -->

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

## Why it might be worth extracting

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
