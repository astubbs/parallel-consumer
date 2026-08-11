# Automated PR reviewer - gaps that affect what you can trust

How the reviewer and its gate work, and the contract for asking for a review, are in
[`docs/ci.md`](../ci.md). This file is only the open gaps.

- **A review is only requested, never automatic - so "no review" is now a normal state.** The
  reviewer moved to on demand (`@claude review this`), with `claude-code-review.yml` reduced to a
  cheap gate that reds the PR until a review exists for the current head. The risk this trades into
  is social rather than technical: the gate can be *satisfied* by any finished comment the reviewer
  bot posts on the PR, including its answer to some other `@claude` question, so an author
  determined to get a green check can get one. That is the same boundary the previous gate drew -
  it guards against the action failing quietly, not against the author - but it is worth re-reading
  if the review ever stops feeling load-bearing.
- **Freshness is inferred from timestamps, not from a signature over the diff.** Nothing the
  reviewer posts names the SHA it reviewed, and a comment-triggered run raises no check run against
  the PR head, so the gate compares the review's creation time against the later of the head
  commit's committer date and the first check suite GitHub raised for that SHA. If the reviewer ever
  starts stamping the reviewed SHA into its comment, replace the timestamp comparison with it - that
  is strictly better, and the timestamp version can be retired the same day.
- **The OSS Index audit grants must be carried across at merge.** `bin/check-ossindex-audit.sh` and
  `bin/test-check-ossindex-audit.sh` are granted on astubbs/parallel-consumer#279, in
  `claude-code-review.yml` - the file the reviewer no longer lives in. **Whichever of
  astubbs/parallel-consumer#279 and the on-demand split merges second must carry those two grants
  into `claude.yml`**, or they are silently dropped and the reviewer will report, as it already did
  once, that it could not run the scripts. They cannot be added ahead of
  astubbs/parallel-consumer#279: a grant for a script that does not exist on master is inert, which
  is worse than no grant (see `bin/AGENTS.md`). `actionlint` was the third of that set and is now
  granted on `claude.yml`, since it ships with the runner and can never be inert.
- **Credential exposure is unresolved, not cleared.** The reviewer runs PR-authored Maven/test code
  in the same job that holds `secrets.CLAUDE_CODE_OAUTH_TOKEN`, with `pull-requests: write`. It is
  now bounded by an `author_association` check restricting the trigger to OWNER/MEMBER/COLLABORATOR,
  which is what replaced the old bound (fork PRs not receiving secrets) when the trigger moved from
  `pull_request` to a comment. That is a bound on *who*, not an answer to whether the token is
  scrubbed before the action spawns Bash subprocesses. Needs confirmation from the action's docs or
  maintainers. Until then: trusted authors only. (`pull-requests: write` may also be droppable back
  to `read` if the action posts via its own app token.)
- **`bin/ci-integration-test.sh` is granted but unproven** against the 30-minute cap -
  Testcontainers on a 2-core hosted runner is slow, and an overrun looks like a timeout rather than
  a misconfiguration. Also unverified whether Docker works inside the action's sandbox at all.
