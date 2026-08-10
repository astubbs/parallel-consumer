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
- **Three grants still missing:** `actionlint`, so the reviewer cannot lint workflow PRs - it said so
  itself on astubbs#102, and it ships on `ubuntu-latest` with `.github/actionlint.yaml` already present;
  and `bin/check-ossindex-audit.sh` + `bin/test-check-ossindex-audit.sh`, which arrived with the OSS
  Index audit lane and were left ungranted for the same reason - editing `claude-code-review.yml` in
  the PR that adds a script forfeits that PR's own review. Land all three in a **non-workflow** PR, or
  the validation skip above means they are never exercised. Grant **both spellings** of each script
  path (`bin/x.sh:*` and `./bin/x.sh:*`), per `bin/AGENTS.md`. (`bin/todo-index.sh` was listed here as
  a fourth until astubbs#279 - both spellings are in fact already granted, added in `bd021cb2`.
  Recorded so it is not re-added.)
- **`bin/ci-integration-test.sh` is granted but unproven** against the 30-minute cap - Testcontainers
  on a 2-core hosted runner is slow, and an overrun looks like a timeout rather than a
  misconfiguration. Also unverified whether Docker works inside the action's sandbox at all.
