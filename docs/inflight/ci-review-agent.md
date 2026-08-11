# Automated PR reviewer - gaps that affect what you can trust

- **A PR that edits `claude-code-review.yml` can never go green on `claude-review`, and that is a
  required check.** `claude-code-action` refuses to run when the file differs from the
  default-branch copy and reports the skip as success; `bin/check-review-posted.sh` catches the
  silent success and fails the job. The guard working as designed ([`docs/ci.md`](../ci.md)) - but
  the consequence is that the reviewer's own allowlist cannot be extended through the normal gated
  flow. Every run on such a PR is red, **no re-run can clear it** (the input is the workflow diff,
  which is fixed per commit), and landing it takes an admin ruleset bypass. Budget for that before
  adding a grant, and prefer spending the forfeit on a minimal PR over a feature branch. The cost is
  a bypassed required check, not a green tick nobody should trust - **do not weaken the gate to
  avoid it.**
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
