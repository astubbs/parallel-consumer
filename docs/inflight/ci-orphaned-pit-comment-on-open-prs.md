# The PIT report comment is orphaned - nothing has posted one since July

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

A `## :x: Mutation Testing (PIT) Report - **PIT did not produce a report.**` comment sits on
astubbs/parallel-consumer#29 showing a red ❌, dated April 2026. **It contradicts the live job**,
which reports success, and it will never change again, because the step that wrote it no longer
exists.

**This was settled, and the answer is that it is not coming back.** `e188de221` deleted the whole
GitHub-hosted `mutation-testing` job - its body says so outright, "Removed the GitHub-hosted
mutation-testing job entirely" - and with it the `Post PIT summary to PR` step that produced that
exact string. A PR-scoped lane was reintroduced hours later in `6f7ce1893`, deliberately **without**
comment-posting: today's `Mutation Tests (PIT, PR-scoped)` job in `.github/workflows/maven.yml` has
two steps, neither of which calls `createComment` or `updateComment`. The verdict is the exit code
of `bin/ci-mutation-test.sh` (its header owns the contract, anchor `THE EXIT CODE IS THE VERDICT`)
plus a `::notice::`, and that is the design.

**So do not resurrect the comment while resurrecting the sticky-comment machinery.** The lane is
advisory and `continue-on-error`; its output is a score, not a verdict a reader must act on, and
adding a per-PR comment would put a third permanent comment on every PR to say something the check
row already says. The shared module now in `.github/scripts/sticky-report-comment.js` would fit
mechanically - that is not the question. Nothing about the lane wants a comment.

## What is actually left

**The stale comments have to be deleted by hand, and no code will ever touch them.** They carry the
retired step's own text, so no marker rename or lookup can reach them - the shared module only ever
finds comments carrying a marker it is given, and nothing gives it this one. On
astubbs/parallel-consumer#29 the comment is from `github-actions[bot]`, posted 2026-04-20; other
long-lived PRs opened before the July removal may carry one too. `gh api` search per PR is the only
way to enumerate them.

**Do not confuse this with astubbs#41** ("Run PIT mutation testing on self-hosted performance
runner"), which is open and is about where the lane runs, not about reporting.

## Prior art searched, and what it returned

Recorded so the next person does not repeat it. `docs/plans/` - `2026-08-03-002-mutation-testing-plan.md`
exists and does not discuss the comment. `docs/solutions/` - nothing on PIT comment posting.
`docs/inflight/` - `ci-mutation-testing.md`, `ci-mutation-lane-skip-reads-as-a-pass.md`,
`ci-mutation-survivor-list-is-unreadable.md`, none about the comment. Merged PRs touching
`mutation|pitest|maven.yml` - several, none about removing the comment. Issues `--state all` -
astubbs#41 (open) and astubbs#39 (closed), both about scope and runners.
`git log --all -S'Mutation Testing (PIT)' -- .github/` is what found `e188de221`, and it is the only
search that did: **the history, not the docs, is where this decision was recorded.**

Reading astubbs/parallel-consumer#29's comment thread top to bottom, nobody has ever mentioned the
PIT comment. It was not disputed; it was orphaned.
