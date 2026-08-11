# Automated PR reviewer - gaps that affect what you can trust

How the reviewer and its gate work, and the contract for asking for a review, are in
[`docs/ci.md`](../ci.md). This file is only the open gaps.

- **A review is only requested, never automatic - so "no review" is now a normal state.** The
  reviewer moved to a `workflow_dispatch` in `claude-code-review-dispatch.yml`, with
  `claude-code-review.yml` reduced to a cheap gate that reds the PR until a review exists for the
  current head. The risk this trades into
  is social rather than technical: the gate can be *satisfied* by any finished comment the reviewer
  bot posts on the PR, including its answer to some other `@claude` question, so an author
  determined to get a green check can get one. That is the same boundary the previous gate drew -
  it guards against the action failing quietly, not against the author - but it is worth re-reading
  if the review ever stops feeling load-bearing.
- **UNVERIFIED: `track_progress` on a `workflow_dispatch` event.** The gate accepts a review only
  from `claude[bot]`, and that identity comes from the action's own sticky tracking comment. On the
  old `pull_request` trigger the event payload named the PR; a dispatch does not, and it is not
  confirmed that the action can still attach and author that comment. If it cannot, the review may
  land as a `github-actions[bot]` comment via `gh pr comment`, which the gate will not accept - so
  the check could never go green. **Verify on the first dispatched run**, by checking the author of
  the resulting comment. If it does not hold, the fix is not to loosen the identity rule (that
  would let any bot report satisfy the gate) but to have the reviewer create a check run on the PR
  head SHA and gate on that instead - which is strictly better, being SHA-exact rather than
  timestamp-based, and would retire the freshness machinery below.
- **The gate runs from the PR's own checkout.** A `pull_request` job checks out the PR, so both
  the gate script and the workflow file come from the tree they are policing. Pre-existing and
  repo-wide rather than anything the on-demand split introduced: `copyright`, `shell: sigpipe`,
  the issue-reference gate and the quarantine audit all execute PR-authored code the same way,
  and on a `pull_request` trigger the workflow file is inherently PR-supplied, so no change
  confined to one workflow closes it. Checking the gate script out from the base ref would close
  the script-tampering half and leave the workflow-file half open - a half-measure worth doing
  only as part of a repo-wide move to default-branch-controlled checks. The standing bound is the
  threat model: trusted authors, and fork PRs that receive no secrets and cannot merge. Raised on
  astubbs/parallel-consumer#284.
- **Freshness is inferred from timestamps, not from a signature over the diff.** Nothing the
  reviewer posts names the SHA it reviewed, and a comment-triggered run raises no check run against
  the PR head, so the gate compares the review's creation time against the later of the head
  commit's committer date and the first check suite GitHub raised for that SHA. If the reviewer ever
  starts stamping the reviewed SHA into its comment, replace the timestamp comparison with it - that
  is strictly better, and the timestamp version can be retired the same day. Two known holes make
  that worth doing rather than merely tidy, both raised on astubbs/parallel-consumer#284: the
  check-suite timestamp is global to the SHA, so force-pushing onto a commit that already ran
  checks elsewhere lets a review of an earlier head postdate it; and the gate cannot tell a review
  from any other `claude[bot]` answer on the PR, so replying to a `@claude` question greens it.
  Both dissolve once the reviewed SHA is recorded.
- **The OSS Index audit grants must be carried across at merge.** `bin/check-ossindex-audit.sh` and
  `bin/test-check-ossindex-audit.sh` are granted on astubbs/parallel-consumer#279, in
  `claude-code-review.yml` - the file the reviewer no longer lives in. **Whichever of
  astubbs/parallel-consumer#279 and the on-demand split merges second must carry those two grants
  into `claude-code-review-dispatch.yml`**, or they are silently dropped and the reviewer will
  report, as it already did once, that it could not run the scripts. They cannot be added ahead of
  astubbs/parallel-consumer#279: a grant for a script that does not exist on master is inert, which
  is worse than no grant (see `bin/AGENTS.md`). `actionlint` was the third of that set and is now
  granted on the reviewer, since it ships with the runner and can never be inert.
- **Credential exposure is unresolved, not cleared.** The reviewer runs PR-authored Maven/test code
  in the same job that holds `secrets.CLAUDE_CODE_OAUTH_TOKEN`, with `pull-requests: write`. The
  move to `workflow_dispatch` keeps the *who* bound the old `pull_request` trigger gave for free -
  dispatching requires write access to the repository - so no new restriction was needed. That is
  still a bound on who can start it, not an answer to whether the token is scrubbed before the
  action spawns Bash subprocesses. Needs confirmation from the action's docs or maintainers. Until
  then: trusted authors only, and note that the code being executed comes from the PR branch, which
  a trusted dispatcher does not necessarily control. (`pull-requests: write` may also be droppable
  back to `read` if the action posts via its own app token.)
- **`bin/ci-integration-test.sh` is granted but unproven** against the 30-minute cap -
  Testcontainers on a 2-core hosted runner is slow, and an overrun looks like a timeout rather than
  a misconfiguration. Also unverified whether Docker works inside the action's sandbox at all.
