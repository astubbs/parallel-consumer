---
title: "A title grep is not a search, and an empty one is not evidence of absence"
date: 2026-08-31
category: workflow-issues
module: docs/inflight
problem_type: process_failure
component: agent_harness
severity: medium
symptoms:
  - "Confidently reported that some category of work is not tracked, when a register for it already existed"
  - "The search that 'proved' absence was restricted to headings, or used synonyms the existing document does not use"
  - "The document that would have answered it was already injected into context at session start and was scrolled past"
root_cause: process_violation
resolution_type: process_fix
tags:
  - prior-art
  - negative-claims
  - inflight-registers
  - search-method
---

# A title grep is not a search, and an empty one is not evidence of absence

## What happened

Asked whether analyser suppressions were tracked anywhere with a ranked list of rules to re-enable,
an agent answered **no such register exists** and began writing one.

Two already did:

- `docs/inflight/static-spotbugs-rule-registry.md` - `## Top 5 to turn back on whole-tree, ranked`
- `docs/inflight/static-error-prone-rule-registry.md` - `## Top 3 to turn back on whole-tree, ranked`

Both are `inflight-type: register`, both carry tiers with per-rule re-enable triggers, and both are
better developed than the replacement being drafted. The operator, who had asked for them originally,
had to say "look carefully" before they were found.

## Why the search failed, precisely

The command was, in effect:

```bash
grep -l "^# .*[Ss]uppress\|^# .*[Ee]xempt\|^# .*[Ee]xclusion" docs/inflight/*.md
```

Two independent defects, and the second is the one worth remembering:

1. **It searched HEADINGS only.** The `^# ` anchor restricts every alternative to the title line. Both
   registries contain "suppress" and "exclusion" in their bodies - a content grep finds them
   immediately. `AGENTS.md` says `grep -rl <mechanism> docs/inflight/`, which is a content search;
   the heading anchor was added by the agent and is what produced the empty result.
2. **It searched the ASKER'S vocabulary, not the domain's.** The registries are titled "what is off,
   why, and what turns it back on". The domain word is **off**. "Suppression", "exemption" and
   "exclusion" are all reasonable synonyms and none of them is in either title. A title search is a
   bet that the author chose your word for their heading, which is a bet you lose most of the time.

**And the answer was already in context.** `.claude/hooks/inject-recorded-knowledge.sh` prints a
`# Registers - standing documents, consult before choosing work` section at the top of the session
index, above open work, precisely so registers are seen before work is chosen. It was scrolled past
on the way to the impact groups.

## The rule

**A negative claim needs a stronger search than a positive one.** Finding something proves it exists;
finding nothing proves only that one query missed. Before writing "there is no X" or "nothing tracks
Y":

- **Grep CONTENT, never headings.** No `^#` anchor. If the heading form is wanted, run it *in
  addition*, never instead.
- **Search at least two vocabularies** - yours and the codebase's. Here: `suppress|exempt|exclusion`
  and `off|disabled|not enabled|re-enable`. When they disagree, the codebase's wins.
- **Read the injected register list first.** It is short, it is at the top of the session index, and
  it exists to answer exactly the question "is there a standing document about this?".
- **Say where you looked.** "None found" is only worth reading if it names the queries. Writing the
  query down is also what exposes an `^#` anchor as a mistake before it becomes a conclusion.

## What it cost, and the tell

A duplicate register was drafted and then deleted, and the operator had to correct the agent twice in
the same exchange - once on the registers existing, once on a warning the codebase had already
written down. The wasted work was small; the confident wrong answer was not, because an operator who
believed it would have concluded their earlier request had never been implemented.

**The tell to watch for in yourself: an empty search result that feels like a finding.** A search that
returns nothing has produced no evidence, and the pull to report it as a result is strongest exactly
when it completes a tidy narrative - "nothing tracks this, so I will build it".

## Related

Same family, different mechanism - a search that ran against the wrong repository and read as "no
prior art":
[`compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md`](compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md).
