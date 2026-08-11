# Automated PR reviewer - gaps that affect what you can trust

How the reviewer and its gate work, and the contract for asking for a review, are in
[`docs/ci.md`](../ci.md). This file is only the open gaps.

- **A review is only requested, never automatic - so "no review" is now a normal state.** The
  reviewer is asked for - by dispatch, or by an `@claude` comment - with
  `claude-code-review.yml` reduced to a cheap gate (its contract is in
  [`docs/ci.md`](../ci.md), stated once there). The risk that trades into
  is social rather than technical: the gate can be *satisfied* by any finished comment the reviewer
  bot posts on the PR, including its answer to some other `@claude` question, so an author
  determined to get a green check can get one - and one such comment counts for the life of the
  PR. That is the same boundary the previous gate drew -
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
- **The duplication scanners are pointed away from where agents duplicate.** `dups: clones` and
  `dups: similarity` scan `parallel-consumer-*/src` only, and the similarity job additionally
  filters `file_extensions: 'java'` - so `docs/`, `.github/`, `bin/` and `AGENTS.md` are scanned by
  neither. Both were green throughout astubbs/parallel-consumer#287 while one contract sat restated
  in nine files, four of them stale. Two separate follow-ups, and they are not substitutes:
  (a) point a clone engine at `docs/` and `.github/` - jscpd handles markdown - which catches
  verbatim copy-paste between docs, a frequent agent behaviour, but **not** paraphrase; and (b) for
  a contract specifically, a narrow guard asserting the canonical phrasing appears in its one home
  and nowhere else, which is more reliable than any similarity metric. Full write-up:
  [`docs/solutions/workflow-issues/duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md`](../solutions/workflow-issues/duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md).
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
- **A check run raised on the reviewed SHA would retire the last write scope in the review
  system.** The gate is produced by a `pull_request` workflow, so a review posted after the last
  push cannot clear it without something re-running it - which is why a `refresh-gate` job holding
  `actions: write` still exists on both routes. If the reviewer instead raised a check run on the
  SHA it reviewed, that job, its token and its fork guards all go, and the check becomes SHA-exact
  rather than a judgement about comment metadata. This is now the single highest-value change left
  in this area. Do not confuse it with restoring head-freshness as a *rule* - that is a separate,
  deliberately parked decision in
  [`parked-strict-review-gate-freshness.md`](parked-strict-review-gate-freshness.md).
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
- **Nothing the reviewer posts names the SHA it reviewed.** The gate judges comment metadata -
  who wrote it, and whether it left unticked boxes - so it cannot tell a review from any other
  `claude[bot]` answer on the PR. Replying to an `@claude` question satisfies it, and now does so
  for the life of the PR rather than only until the next push. That is why fork heads are refused
  by the gate outright rather than left to the comment rules. It dissolves if the reviewer ever
  stamps the reviewed SHA into a check run; see the entry above.
- **CARRIED, but never exercised: the OSS Index audit grants.** The carry-across this entry used to
  demand has happened. astubbs/parallel-consumer#279 merged the on-demand split in, found the
  reviewer no longer lived in `claude-code-review.yml`, and moved `bin/check-ossindex-audit.sh`,
  `bin/test-check-ossindex-audit.sh`, `bin/check-cve-exclusions.sh` and
  `bin/test-check-cve-exclusions.sh` - bare and `./` forms - into BOTH allowlists deliberately
  rather than by luck. That was the first live instance of the drift the entry above warns about,
  and it took a judgement no merge resolution would have made on its own, which is the argument for
  `bin/check-review-tool-grants.sh` existing. What stays **open** is that no reviewer has run any of
  the four yet: until a review invokes one and says so here, the grants are asserted capability, not
  demonstrated. `actionlint` was the fifth of that set and was already granted, since it ships with
  the runner and can never be inert.
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
- **Fork PRs have no green path and no runnable reviewer; a secretless reviewer is the only real
  fix.** Refusing fork heads means the gate - which accepts only a `claude[bot]` comment - can
  never go green on a fork PR, so those merge with `claude-review` red unless the commits are moved
  to a branch in this repo. The comment route also withholds its tool allowlist on a fork head, so
  a fork gets a reader rather than a runner: granting `./mvnw` and the `bin/` scripts there would
  put fork-controlled executables beside `CLAUDE_CODE_OAUTH_TOKEN`. Both are the same missing
  capability - a review job that runs an untrusted checkout WITHOUT the credential. Letting a
  maintainer assert the PR was reviewed was rejected, being the same self-asserted escape the gate
  refuses everywhere else. Raised on astubbs/parallel-consumer#284; the limit is in `docs/ci.md`.
- **`bin/ci-integration-test.sh` is granted but unproven** against the 30-minute cap -
  Testcontainers on a 2-core hosted runner is slow, and an overrun looks like a timeout rather than
  a misconfiguration. Also unverified whether Docker works inside the action's sandbox at all.
