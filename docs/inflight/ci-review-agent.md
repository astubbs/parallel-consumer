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
- **Two grants still missing:** `actionlint`, so the reviewer cannot lint workflow PRs - it said so
  itself on astubbs#102, and it ships on `ubuntu-latest` with `.github/actionlint.yaml` already present; and
  `bin/todo-index.sh`, whose script merged with astubbs#103 while the grant did not follow. Land both in a
  **non-workflow** PR, or the validation skip above means they are never exercised.
- **`bin/ci-integration-test.sh` is granted but unproven** against the 30-minute cap - Testcontainers
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
