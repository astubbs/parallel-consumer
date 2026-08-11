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

  **That bound does not hold for `claude.yml`**, and the difference is easy to miss: a comment
  trigger receives secrets even on a **fork's** PR, where `pull_request` withholds them. So the
  "same-repo only" reasoning above never applied to the fallback reviewer.

  Two gates now stand in for it, and the second exists because the first is not enough. The job runs
  only when `author_association` is `OWNER`/`MEMBER`/`COLLABORATOR`, which encodes the "trusted
  authors only" posture this entry already assumed. But that authenticates **the commenter, not the
  author of the code**: a maintainer typing `@claude` on an outside contributor's fork PR is a
  trusted trigger over untrusted content, and the fork author would choose what the allowlisted
  `bin/*.sh` contains. So a pre-checkout step refuses any pull request where `isCrossRepository` is
  not `false`. Caught in review of astubbs#286 as a P1, not by the author.

  Consequence worth knowing: **fork PRs cannot get an `@claude` review at all.** They already could
  not get a useful automatic one - `pull_request` withholds the token from forks, so the action has
  no credential - so this makes an existing gap explicit rather than creating one. Revisit only with
  a design that separates the trusted reviewer from the untrusted tree.
- **`actionlint` is still not granted**, so the reviewer cannot lint workflow PRs - it said so itself
  on astubbs#102, and it ships on `ubuntu-latest` with `.github/actionlint.yaml` already present.
  Land it in a **non-workflow** PR, or the validation skip above means it is never exercised.
  (`bin/todo-index.sh` was also listed here as missing; it is granted in both workflows now.)
- **`bin/ci-integration-test.sh` is granted but unproven** against the 30-minute cap - Testcontainers
  on a 2-core hosted runner is slow, and an overrun looks like a timeout rather than a
  misconfiguration. Also unverified whether Docker works inside the action's sandbox at all.
- **Unknown: which tree `claude.yml` actually runs its scripts against.** Now that the fallback
  reviewer can execute the gates, this decides whether the result means anything. `claude.yml`'s
  checkout names no `ref` and uses `fetch-depth: 1`, which for a comment trigger is the **default
  branch** - so a granted script could be running master's gate over master's tree while claiming to
  review a PR. Against that, the astubbs#273 round described the *fixed* guard accurately, which it
  could not have done from master's copy, so `claude-code-action` appears to check out the PR itself.
  Appears is not knows. The first granted run settles it: if a script's output describes master
  rather than the PR, the checkout needs a PR ref and `fetch-depth: 0` (as
  `claude-code-review.yml` uses) before any of its execution results are trustworthy.

  Deliberately not fixed speculatively in the same PR as the grant: a PR-head checkout in a
  comment-triggered job is a security change, not a convenience one, and it should be made knowing
  whether it is needed.
