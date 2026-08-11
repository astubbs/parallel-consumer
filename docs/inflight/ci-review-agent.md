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
- **Granted but unproven**, in two different ways. `bin/check-ossindex-audit.sh` and
  `bin/test-check-ossindex-audit.sh` have never been exercised by the reviewer - the first PR
  after the one that granted them is the first real test. And `bin/ci-integration-test.sh` is
  unproven against the 30-minute cap - Testcontainers
  on a 2-core hosted runner is slow, and an overrun looks like a timeout rather than a
  misconfiguration. Also unverified whether Docker works inside the action's sandbox at all.
