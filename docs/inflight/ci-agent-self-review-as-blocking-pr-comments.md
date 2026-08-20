# Proposal: the agent reviews its own PR, and its must-dos become blocking review comments

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->
<!-- inflight-state: deferred - after v6, agent tooling proposal -->


**Status: proposed, not built.** Owner's idea, recorded so the design argument is not re-derived.
Tune once it exists rather than up front.

## The failure it targets

astubbs/parallel-consumer#31 merged roughly ten minutes before a background agent finished the
broker-level reproduction of confluentinc#909 - the exact gap that PR's own description declared open
under "Known gap". The result: the fix and the evidence proving it are in two PRs
(astubbs/parallel-consumer#322), and an inflight note was stale on master the moment it landed.

**The agent was not ignorant.** It knew the agent was running and knew the description declared a
gap. What it lacked was any artefact that outlived the moment the question "are the commits ready?"
was asked. Knowledge held only in a conversation does not survive the turn it was formed in.

`docs/merge-checklist.md` was loaded in that very turn, by `inject-merge-checklist.sh`, and did not
help: a checklist prompts for the things you remember to check against it, not for the thing you
have forgotten you are waiting on. `.claude/hooks/check-merge-outstanding-work.sh` (astubbs#324)
catches the narrow live-background-task case. This proposal is the general form.
<!-- file-refs: N/A - the hook lands with astubbs#324, the tooling half of the astubbs#322 split -->

## The mechanism

The agent posts its own pre-merge must-dos as **review comments on the PR**. GitHub blocks merge on
unresolved conversations, so each one has to be explicitly resolved - a deliberate act by a human or
by the agent having actually done the thing. It mirrors how the maintainer already works: he reviews
his own PRs.

## The admission test - this is the whole design

> **Something I know must happen before this merges, that I am not working on right now.**

Each clause is load-bearing:

- **must happen before this merges** - in scope for THIS PR. Work that belongs in a future PR is not
  a blocking comment; it is a `docs/inflight/` note or a `docs/refactoring.md` entry. Mixing the two
  is how a blocking comment becomes unresolvable.
- **that I am not working on right now** - the point is the gap between knowing and doing. Something
  actively being done needs no marker; it will be in the diff. An earlier draft of this rule said
  "not doing in this PR", which is self-contradictory: if it is not in the PR it cannot block the
  PR.
- **I know** - not speculation, not "might be nice". A thing the agent can already name.

Applied to astubbs#31 it admits exactly one comment - *load-level evidence is outstanding in a
background agent; either wait for it or accept the split deliberately* - which is precisely what was
lost.

## The noise risk, and the guard

A blocking comment is only worth anything while resolving one is a considered act. An agent posting
every nit trains the maintainer to resolve reflexively, and the mechanism is then worse than nothing
because it looks present.

So: **the agent proposes its candidate comments in chat and the maintainer approves before any are
posted.** Never auto-posted. That keeps the human as the filter on volume and keeps the blocking
power meaningful.

## Open questions

- **Does unresolved-conversation blocking actually apply here?** It is a repository setting;
  `astubbs/duplicate-code-cross-check`'s ruleset carries `required_review_thread_resolution: true`.
  If `astubbs/parallel-consumer` does not, the comments are advisory and the mechanism has no teeth.
  Check before building.
- **Where does the prompt come from?** A `UserPromptSubmit` injection on merge-prep-shaped prompts,
  like `inject-merge-checklist.sh`, is the obvious home - it already fires at the right moment.
- **What resolves a comment?** The agent doing the thing and saying so in-thread, or the maintainer
  deciding it does not apply. Both must be explicit.

## Related

`.claude/hooks/check-merge-outstanding-work.sh` (astubbs#324) and `docs/agent-harness.md` (the
layers and what each can enforce). The incident write-up lives with astubbs/parallel-consumer#322.
<!-- file-refs: N/A - the hook lands with astubbs#324, the tooling half of the astubbs#322 split -->
