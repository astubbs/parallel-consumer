# Automated PR reviewer - gaps that affect what you can trust

- **A green `review` check can mean the reviewer never ran.** `claude-code-action` refuses to run when
  the workflow file differs from the default-branch copy, and reports that skip as **success**. So any
  PR editing `claude-code-review.yml` - or a workflow it validates - gets a green review check that
  verified nothing. Correct behaviour on the action's part, invisible unless you read the job log.
  **Do not read a green `review` check as "reviewed" on a workflow-touching PR.**
- **Credential exposure is unresolved, not cleared.** The review job runs PR-authored Maven/test code
  in the same job that holds `secrets.CLAUDE_CODE_OAUTH_TOKEN`, with `pull-requests: write`. Bounded by
  fork PRs not receiving secrets (same-repo, push-access only), which is not an answer to the actual
  question. Needs confirmation from the action's docs or maintainers about token scrubbing before it
  spawns Bash subprocesses. Until then: trusted authors only. (`pull-requests: write` may also be
  droppable back to `read` if the action posts via its own app token.)
- **The grants landed, but this PR's own review is the price.** `actionlint`,
  `bin/check-ossindex-audit.sh` and `bin/test-check-ossindex-audit.sh` are now in the allowlist
  (both spellings each, per `bin/AGENTS.md`). They were parked on a catch-22 - editing
  `claude-code-review.yml` makes it differ from the default-branch copy, so the action skips and
  **reports that skip as success**, forfeiting the review of whichever PR adds them. They landed in
  astubbs/parallel-consumer#279 deliberately, *after* its review had completed and its threads were
  closed, so the forfeited review was one nobody needed. **Consequence to remember: any further
  review on astubbs/parallel-consumer#279 is a no-op, and its green review tick must not be read as
  reviewed.** The grants themselves are unexercised until the first PR after this one merges.
- **`bin/ci-integration-test.sh` is granted but unproven** against the 30-minute cap - Testcontainers
  on a 2-core hosted runner is slow, and an overrun looks like a timeout rather than a
  misconfiguration. Also unverified whether Docker works inside the action's sandbox at all.
