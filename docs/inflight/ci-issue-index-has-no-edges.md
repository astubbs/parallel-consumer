# The issue index carries nodes but not edges

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

[`issue-index.md`](issue-index.md) put the tracker inside `git grep`'s reach, and stopped at the
boundary of a single issue. It materialises number, state, title and labels - **nodes**. What it does
not materialise is the **edges**: which PR closes which issue, which issue references which, which
comment says the fix is elsewhere. So an agent can now find an issue by keyword and still has no way
to learn, without reaching GitHub, what that issue is attached to.

That matters because the edges are where the reasoning lives. A PR body and its comments are
routinely the only record of why a fix does or does not close an issue -
[`pr-mirror-fixes-and-what-they-close.md`](pr-mirror-fixes-and-what-they-close.md) records a merged
PR whose "what this closes, and what it does not" section exists nowhere a reader of the issue will
meet it. `.claude/hooks/inject-branch-context.sh`'s header records the same class one level down:
five agents were dispatched with a PR's changed files and not its body, and each was one edit away
from reverting a decision that body defended by name.

**One edge is already materialised, by accident, and it is the commonest one.** A mirror row's title
begins `confluentinc#NN:`, so fork-issue -> upstream-issue is a grep away. Nothing else is, which is
why any consumer of this is nearly free for mirrors and needs new data for everything else.

## The consumer this is for

A `PreToolUse` hook on `gh issue view` / `gh pr view` that injects the related items alongside the
call - the same shape as the reminders already registered, and for the same reason: an agent reading
one issue has no prompt to ask what else is attached to it. Constraints worth fixing before anyone
writes it, each already paid for once:

- **`additionalContext` on `PreToolUse` is the verified channel**; raw stdout is discarded.
  [`docs/agent-harness.md`](../agent-harness.md) owns that, and owns the standing rule that harness
  claims are tested rather than read off the documentation.
- **No `if` prefix match.** `check-squash-subject.sh` shipped with one and missed every command shape
  it existed for; the hook self-filters instead.
- **Cache-first, no network in the common path.** The rate limit is shared by every session running
  in parallel here, and a hook that spends it on each `gh` call taxes the sessions that did nothing
  wrong.
- **Rows, not prose, and silent when nothing is non-obvious.** Injected context competes for attention
  exactly as a rule in a file does; a poke that fires on every `gh` call gets skimmed like a check
  that is always red.

## The decisions

1. **Extra columns in `issue-index.md`, or a second generated file.** Columns keep one file and one
   regeneration; a second file keeps the index short, which is the property its own header says its
   usefulness depends on.
2. **Comment bodies are the richest source and the one that cannot be cached cheaply.** Decide whether
   the poke names comment count and authors - what `inject-branch-context.sh` already does for a
   branch's own PR - or fetches them.
3. **Gated on [`process-adopt-external-harness.md`](process-adopt-external-harness.md).** Beads
   advertises dependency graphs and context injection; if a GitHub link cache is already in it, this
   is an adoption decision and not a build. **Run on 2026-09-02** -
   [`docs/plans/2026-09-02-001-investigate-adopt-or-build-re-run.md`](../plans/2026-09-02-001-investigate-adopt-or-build-re-run.md)
   owns the verdict: build, do not adopt, for the query layer; and Beads keeps its store outside the
   tree, so nothing in it is a branch-travelling link cache. This item is a build. One requirement
   arrived with [`ci-inflight-standalone-thesis.md`](ci-inflight-standalone-thesis.md): the edges
   are carried per document *version*, never on a node with a current state - a reconciled node is
   the thing the 09-02 re-run rejected.

## Delete when

The edges are materialised and something consumes them, or the adopt decision rules out building.
