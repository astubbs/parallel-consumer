# Compounding candidates mined from the 2026-08-19/20 session

<!-- inflight-type: register -->
<!-- inflight-impact: process -->

A long session produced more lessons than were worth acting on at once. The two cheapest landed with
this note; the rest are recorded so the analysis is not re-derived. **Ranked by enforceable ×
frequency**, each with the incident that motivates it - a proposal without one is speculation.

## Already covered - checked, not proposed

`check-history-rewrite.sh`, `check-merge-outstanding-work.sh`, `check-pr-ready.sh`,
`check-inflight-tags.sh`, the `pre-commit-gate.sh` scope fix, the mutation-lane skip note, the
`included.groups` trap, and the duplicate-code thread churn (fixed upstream). Listing them is the
point: it stops the same thing being proposed a third time.

## Open candidates

1. **Zero tests selected is a failure, not a pass.** Three consecutive false-green soak runs in ten
   minutes on 2026-08-18: `nohup &` returned 0 over a `BUILD FAILURE`; a run died in 2.7s on the
   parent module, which has no tests; and an empty `included.groups` selected nothing.
   `bin/chaos-test.sh` already implements exactly this check locally ("ZERO chaos tests selected -
   this run measured NOTHING"), so generalising it into `bin/lib/` is proven, not invented. Small.
2. **An inflight filename prefix must name an AREA, never a state.** `docs/inflight/AGENTS.md` says
   so in bold and nothing enforces it; `next-`/`parked-` names survived into two PRs, and a
   rename-plus-edit merge left two copies of one note. ~15 lines in `check-inflight-tags.sh`, which
   already reads every one of those files in a job that already runs. Small.
3. **Reconcile required status checks against what actually reports.** A sibling repo's PR was
   permanently unmergeable because a required CodeQL workflow had been auto-disabled for inactivity -
   and the message reads as transient. Also collapses four prose copies of "the job name is an API
   for the ruleset" into one enforced rule. **The only candidate here that DELETES documentation.**
   Medium; the networked half cannot use the `check-` prefix (see `bin/AGENTS.md`).
4. **Refuse to cut a branch from a stale local base.** The astubbs#322 split was built against a
   local master 40 commits behind and had to be discarded and rebuilt. `docs/merge-checklist.md`
   already says "fetch first, every time" - but it is injected on merge-prep-shaped prompts, and this
   was a split, so the rule existed and did not reach the moment. Medium.
5. **Two Maven runs must not share one worktree's `target/`.** An A/B soak was rendered worthless by
   two overlapping runs sharing one `target/`, both writing the same log and both starting Kafka
   containers. `bin/worktree-status.sh` reports holders; nothing consults it. Medium.
6. **`check-pr-ready.sh` should print the commit subjects and count.** A rebase-merge was recommended
   for a branch of 39 commits that were "a record of me changing my mind". Five lines, and it fits
   the script's charter exactly: give the claim a testable referent, never conclude. Tiny.
7. **The no-loss proof for a SPLIT, as a named method.** Merging all split branches back together and
   diffing against a pre-split backup tag must be empty. `docs/merge-checklist.md` has the
   single-branch analogue and no split one. Prose only, so it fires only when opened.
8. **When corrected on behaviour, suspect the injected doc.** The squash-message instruction produced
   the same wrong move three times because the fix belonged in the injected document, not the
   instance. The set of injected docs is small and enumerable, so a corrected agent has a short
   suspect list. Tiny.

## The pattern worth more than the list

**Enforcement here is concentrated at commit time and merge time, and this session's failures
concentrated in claims made in chat** - "MERGEABLE/CLEAN" reported as readiness, a merge strategy
recommended without reading the commits, a fabricated user-report narrative. None passed through a
tool call, so no gate could have fired. `bin/check-pr-ready.sh` is the only existing answer to that
class, and its design move - give the claim a testable referent and refuse to conclude - generalises
further than it is currently used. Candidate 6 is the cheapest next instance.

**A rule that lands only in an unmerged PR does not bind, and nobody can tell.** Both operator and
agent believed the squash-message instruction had been fixed; a sweep of every open branch found it
had never been touched anywhere. With seven PRs open at once, "we already fixed that" is
unverifiable without a scripted sweep.

## Delete when

The list is empty, or the remaining items have moved to `docs/refactoring.md` as ordinary backlog.
