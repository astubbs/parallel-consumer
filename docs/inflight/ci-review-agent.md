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
- **The DISPATCHED reviewer cannot open inline review comments; the comment route can.** The
  action installs the inline-comment MCP server only for an *entity* event. `workflow_dispatch` is
  not one, so on a dispatch the tool does not exist and the grant was removed rather than left
  inert - blocking findings arrive as a marked section in the summary comment, and
  `required_review_thread_resolution` has nothing to resolve. `issue_comment` **is** an entity
  event, so `claude.yml` grants the tool and gets real review threads. The practical answer is
  therefore routing, not a fix: ask by comment when you want findings that mechanically block.
  What stays open is the dispatch route itself, which would need a trigger carrying a PR context
  (an `issue_comment` command phrase would, and would restore tag mode wholesale). Weigh that
  against what the dispatch route uniquely gives - `-f focus` and the packaged procedure - before
  assuming it should be replaced.
- **OPEN, and it is the first thing to do after this lands: the dispatched reviewer has never
  completed an end-to-end review.** Nothing has yet observed it read a PR, post a summary comment
  as `claude[bot]`, and turn `claude-review` green in one run - only the individual mechanisms were
  proved on runners. It could not be verified before merging, because `claude-code-action` refuses
  to run unless the invoking workflow file is byte-identical to the default branch's copy: `--ref
  <your branch>` is refused and `--ref master` runs the old copy. So the very first action after
  the merge is a `--ref master` dispatch against a live PR, and **reading the run** rather than
  assuming. Until someone has done that and said so here, treat the reviewer as unproven. The same
  constraint is inherited by whoever edits the reviewer next - your change will land unverified
  too. Reviewing a PR that edits this workflow is a separate thing and does work; the mechanism is
  in [`docs/ci.md`](../ci.md).

  **The comment route is no better off, for a different reason.** It looks testable - just comment
  on a PR - but `issue_comment` workflows always run the **default branch's** copy, so a comment
  exercises master's `claude.yml`, never the one on your branch. The tool grants and the
  `refresh-gate` added there are unverified in exactly the same way. After the merge, exercise
  both: a `--ref master` dispatch, and an `@claude review this` comment on a live PR - checking
  that the comment route really does open an inline thread, and that `claude-review` clears itself.
- **DECIDED: the comment route holds the same curated allowlist as the dispatch route.** It ran
  with no grants at first and reviewed by reading, which showed up on
  astubbs/parallel-consumer#288 ("tool permissions blocked Bash execution") on a PR whose whole
  diff was check scripts. The objection was that comment text is attacker-influencable where a
  dispatch is not - `claude.yml` matches `@claude` by plain substring with no awareness of quoting
  (astubbs/parallel-consumer#286: prose *about* the trigger fired it twice). Rejected as the
  operative difference, because **both** routes execute PR-authored code, and what protects them
  is not a trusted trigger but a curated list of this repo's own scripts plus a job with no write
  grant. A narrower subset was considered and dropped: the whole value is running the checks the
  PR changed. Blanket `Bash(*)` was never on the table. Revisit if the trigger ever widens beyond
  a substring match on a comment.
- **NOT ENFORCED YET: the two tool allowlists can drift apart.** `claude.yml` and
  `claude-code-review-dispatch.yml` now carry byte-identical `--allowedTools` lists (the comment
  route adds the inline-comment tool), and nothing checks that. A grant added to one and missed on
  the other produces a reviewer that silently cannot run the check it was asked about - the same
  class of near-miss as astubbs/parallel-consumer#273's grants nearly vanishing in a merge. The
  fix is a small `bin/check-review-tool-grants.sh` that extracts both lists and diffs them, with
  the usual `bin/test-*` self-test, wired into the hygiene job. Cross-reference comments in both
  workflows are the stopgap.
- **The two `refresh-gate` jobs are near-duplicates and must be changed together.** One is in
  `claude-code-review-dispatch.yml`, one in `claude.yml`. Sharing the body would mean checking out
  the script, and a no-checkout job is precisely what both of them are - the write grant must not
  share a filesystem with code the workflow just ran. So the duplication is deliberate and the
  coupling is manual: fix one, check the other.
- **The gate is still timestamp-based, and a check run against the head SHA would be better.** If
  the identity or freshness rules ever need reopening, the fix is not to loosen who may satisfy the
  gate (that lets any bot report through) but to have the reviewer raise a check run on the reviewed
  SHA and gate on that: SHA-exact rather than timestamp-based, and it retires the freshness
  machinery described below.
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
  the PR head, so the gate compares the review's creation time against when the head appeared. That
  moment is the earliest check suite GitHub raised for the SHA, with the commit's committer date used
  **only as a fallback when no suite exists** - a preference, deliberately *not* the later of the
  two. (An earlier revision of the gate did take the later, and this note still described that after
  the code changed; the two disagree exactly where it matters, on a future-dated commit, which under
  max() holds the check red with no review able to clear it.) If the reviewer ever
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
  move to `workflow_dispatch` bounds *who can start it* - dispatching requires write access to the
  repository - but an earlier revision of this entry concluded from that that no new restriction
  was needed, which was wrong: dispatch permission says the DISPATCHER is trusted and says nothing
  about the branch. `pull_request` withheld secrets from fork PRs for free; `workflow_dispatch` does
  not. So the reviewer now **refuses fork heads explicitly** (its "Validate inputs and refuse fork
  heads" step), which re-establishes the old boundary. What remains open is the narrower question:
  whether the token is scrubbed before the action spawns Bash subprocesses, which needs confirmation
  from the action's docs or maintainers. Until then, in-repo heads only - and note that even an
  in-repo PR branch is code a trusted dispatcher does not necessarily control.
  (`pull-requests: write` may also be droppable back to `read` if the action posts via its own app
  token.)
- **Fork PRs have no green path, and a secretless reviewer is the only real fix.** Refusing fork
  heads means the gate - which accepts only a `claude[bot]` comment - can never go green on a fork
  PR, so those merge with `claude-review` red unless the commits are moved to a branch in this repo.
  Raised on astubbs/parallel-consumer#284 and documented as a known limit in `docs/ci.md`. The fix
  is a review job that runs the untrusted checkout WITHOUT the credential; letting a maintainer
  assert the PR was reviewed was rejected, being the same self-asserted escape the gate refuses
  everywhere else.
- **`claude.yml` grants nothing, and that is now fine - but know what it means.** The `@claude`
  mention handler passes no `--allowed-tools` at all, and an absent allowlist is not permissive:
  Bash is simply not pre-approved and there is no interactive approver in CI, so every script call
  is refused. Proven in-session on astubbs/parallel-consumer#273 rather than inferred - `git log`,
  `grep` and `python3 --version` ran unprompted while both `bash bin/test-check-docs-data.sh` and
  `./bin/check-docs-data.sh` returned "this command requires approval". An earlier review round had
  concluded the opposite, that no workflow grant could clear it; that reasoning is wrong, do not act
  on it.

  **The recommendation that came with this finding - mirror the `bin/check-*` grants into
  `claude.yml` - is retired by astubbs/parallel-consumer#284 rather than outstanding.** It rested
  on `@claude` being the only way to review a PR that edits `claude-code-review.yml`, which was true
  only while that file invoked the action. It no longer does: it is a gate, the reviewer is
  `claude-code-review-dispatch.yml` dispatched `--ref master`, and the grants live there. Mirroring
  them into `claude.yml` now would widen a trigger that anyone can fire on a public repo, to no
  benefit. What survives is the caveat: a `@claude` mention produces a free-form answer that cannot
  run this repo's scripts, so it is not a substitute for a dispatched review.
- **`bin/ci-integration-test.sh` is granted but unproven** against the 30-minute cap -
  Testcontainers on a 2-core hosted runner is slow, and an overrun looks like a timeout rather than
  a misconfiguration. Also unverified whether Docker works inside the action's sandbox at all.
