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
- **`claude.yml` grants nothing, so the manual fallback reviews blind.** `.github/workflows/claude.yml`
  is what runs an `@claude` request in a comment, and it is the *only* way to get a real review of a
  PR that edits `claude-code-review.yml` (see the validation skip above). Its `claude_args` block is
  entirely commented out, so it passes no `--allowed-tools` at all - which does **not** mean
  permissive. An absent allowlist means Bash is not pre-approved, and there is no interactive
  approver in CI, so every script call is refused.

  Proven in-session on astubbs#273 rather than inferred: `git log`, `grep` and `python3 --version`
  ran unprompted while both `bash bin/test-check-docs-data.sh` and `./bin/check-docs-data.sh`
  returned "this command requires approval". An earlier review round had concluded the opposite -
  that the absent restriction meant the block came from elsewhere and no workflow grant could clear
  it - and that reasoning is wrong; do not act on it.

  So the fallback path is exactly the one that most needs to run scripts and is least able to. Mirror
  the `bin/check-*.sh` / `bin/test-check-*.sh` entries from `claude-code-review.yml`'s `--allowedTools`
  into `claude.yml`'s `claude_args`. It cannot ride in a PR that also edits `claude-code-review.yml`:
  editing `claude.yml` there would break the manual review *on that same PR*, which is the only
  review it can get.
