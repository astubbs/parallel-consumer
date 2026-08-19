# Automated PR reviewer - gaps that affect what you can trust

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->


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
- **The DISPATCHED reviewer cannot open inline review comments; the comment route can** - read off
  the action's entity-event handling below, not off an observed thread; see the measured note linked
  further down for what has and has not actually been seen. The
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
- **OBSERVED 2026-08-14, and it failed: the dispatched reviewer ran a full review and posted
  nothing.** This is the end-to-end check the previous revision of this entry asked for, now done
  and read rather than assumed. `--ref master`, `-f pr=297`, `-f focus=<a three-point steer>`, run
  [`31770555653`](https://github.com/astubbs/parallel-consumer/actions/runs/31770555653). The
  `Run Claude Code Review` step ran **7m12s** and reported `conclusion=success`. On the PR
  afterwards: no `claude[bot]` issue comment, no PR review, `No buffered inline comments` in the
  log, and `claude-review` still red. Every step of both jobs was green, including the guard and
  the `refresh-gate`. So the reviewer is not merely unproven - one full billed review has now been
  spent producing nothing, and **nothing in the system noticed**. Treat a green reviewer run as
  saying only that the action exited, until the post-condition below exists.

  Not yet established: whether it reviewed and declined to post, or never got that far.
  `Bash(gh pr comment:*)` **is** granted, and the appended system prompt makes posting mandatory
  and non-optional ("your LAST action"), so the instruction and the capability were both present.
  The next dispatch should be read against `claude-execution-output.json` to tell those apart.

- **THE GUARD CANNOT CATCH THAT, because it tests the wrong thing.** The reviewer job's "Refuse to
  report success for a review that did not run" step branches only on
  `steps.claude-review.outputs.conclusion`. That detects an *abort* - the workflow-validation skip
  it was written for - and is structurally incapable of detecting a run that reviews, concludes
  `success`, and posts nothing. That is the same class as the `--comment` regression this repo
  already ate ("ran green for months while never posting a single review"), so the guard added
  afterwards does not cover the case that motivated it. **The missing piece already exists**:
  `bin/check-review-posted.sh` answers exactly "is there a finished `claude[bot]` comment on this
  PR", and the gate uses it - the reviewer job simply never asks it about its own output. Running
  it as a post-condition on the reviewer job turns "billed, ran, said nothing" from green into red
  with the reason attached. Highest-value fix in this file after the check-run entry below.

- **Nothing announces a dispatched review at its start, so an in-flight billed review is
  invisible - and so is the `-f focus` steer.** Until it posts, the only record that a review was
  requested, by whom, and what it was steered toward, is the requester's shell history. This is
  not an oversight but an unreplaced casualty: `track_progress: true` used to post the sticky
  `claude[bot]` comment at the start, and it is now hard-set `false` because the action refuses
  `track_progress` on any non-entity event and aborts the run outright (which killed the first
  live dispatch, run 31464598166). What replaced it - the FINISH WITH A SUMMARY COMMENT
  instruction - covers only the end, and the entry above is the case where the end never comes.
  The fix does not depend on the action at all: a plain `gh pr comment` step before it, posting
  the PR number, the run URL and the verbatim `focus`. The action can refuse its own sticky
  comment; it cannot refuse a step that does not involve it.

  **The steer is the part worth posting.** It is appended to the packaged procedure and materially
  shapes where the review looks hardest, so a reader who cannot see it cannot tell a narrow review
  from a broad one - and cannot tell whether a "no findings" result was steered away from the area
  they care about.

- **What the dispatch route buys over `@claude review this`, since the comment route otherwise
  looks strictly better.** Exactly one thing: the **packaged procedure**. `plugins:
  'code-review@claude-code-plugins'` plus `/code-review:code-review <repo>/pull/<n> --comment`
  invokes a versioned multi-step review; a mention passes whatever was typed as the *entire*
  prompt, silently substituting an improvised review that still looks like a review. Three things
  commonly assumed to be advantages are **not** - both routes carry the same curated tool
  allowlist, both execute the default branch's workflow copy, and both satisfy the gate, which
  matches `.user.login == 'claude[bot]'` and nothing else. Against that one advantage the comment
  route announces itself by construction and can open inline review threads that mechanically
  block the merge. Worth re-deciding only alongside the trigger question in the inline-comment
  entry above; recorded here so the next reader does not re-derive it.

  The constraint that made all of this unverifiable before merge still holds: `claude-code-action`
  refuses to run unless the invoking workflow file is byte-identical to the default branch's copy,
  so `--ref <your branch>` is refused and `--ref master` runs the old copy. Whoever edits the
  reviewer next lands their change unverified in exactly the same way. Reviewing a PR that edits
  this workflow is a separate thing and does work; the mechanism is in [`docs/ci.md`](../ci.md).

  **The comment route is no better off, for a different reason.** It looks testable - just comment
  on a PR - but `issue_comment` workflows always run the **default branch's** copy, so a comment
  exercises master's `claude.yml`, never the one on your branch. The tool grants and the
  `refresh-gate` added there are unverified in exactly the same way.

  **Both routes have since been exercised and measured**, and the result is closed, so it lives in
  [`docs/solutions/workflow-issues/the-two-review-routes-measured-2026-08-17.md`](../solutions/workflow-issues/the-two-review-routes-measured-2026-08-17.md)
  rather than here. The short version: the comment route posts, at both ends; the dispatch route can
  run for nine minutes, conclude success, and post nothing, leaving `claude-review` green on an older
  comment. **What is still open is only the inline thread** - neither run had a blocking finding, so
  neither had occasion to open one, and deciding it needs a PR that does.

  **Reproduced again 2026-08-19 on astubbs/parallel-consumer#320**, and this sighting narrows it.
  Run `32218074377`: dispatched with a long, specific `-f focus` naming four areas, ran 5m47s,
  concluded **success**, and posted neither an issue comment nor a review. The two comment-route
  reviews on the same PR that morning both posted normally, so the difference is the route, not the
  reviewer or the diff. `claude-review` stayed green throughout on a comment from 04:22 - which is
  the part that makes this worse than a plain failure: a run that produces nothing is
  indistinguishable, from the gate's side, from one that found nothing.

  **So the practical rule until this is fixed: ask for a review by comment.** `-f focus` is the only
  thing the dispatch route uniquely buys, and a steer that reliably produces no review is worth less
  than an unsteered one that posts. Say the focus in the `@claude review this` comment instead.
- **A contract restated in nine files still has no mechanical guard.** Follow-up (a) - point a clone
  engine at `docs/`, `.github/` and `bin/` - **landed** in astubbs/parallel-consumer#320: both jobs
  now scan `.` rather than a whitelist of Java module directories. Do not read that as the problem
  being solved. It catches **verbatim copy-paste**, and the failure recorded in the write-up was
  **paraphrase**: nine sentences saying one thing, sharing almost no token runs, which no clone
  engine can see. astubbs/parallel-consumer#320 hit the same wall from the other side - a genuine
  22-line clone that both engines are structurally unable to compare, because it was JavaScript
  embedded in a YAML string on one side and a shell heredoc on the other.
  What remains open is follow-up (b), which is the one that would actually have caught astubbs#287: for a
  contract specifically, a narrow guard asserting the canonical phrasing appears in its one home and
  nowhere else. More reliable than any similarity metric, because it tests the thing you care about
  instead of a proxy for it. Full write-up:
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
- **SETTLED, and there is no second approval layer: the OSS Index audit grants were simply not in
  the copy that was running.** astubbs/parallel-consumer#279 carried `bin/check-ossindex-audit.sh`,
  `bin/test-check-ossindex-audit.sh`, `bin/check-cve-exclusions.sh` and
  `bin/test-check-cve-exclusions.sh` - bare and `./` forms - into BOTH allowlists, and the reviewer
  on run [`31547154463`](https://github.com/astubbs/parallel-consumer/actions/runs/31547154463)
  still got `this command requires approval` for every spelling of both self-tests. An earlier
  revision of this entry recorded that as an unexplained approval layer *beyond* `--allowedTools`.
  There is no such layer. That run was an `issue_comment`, which GitHub runs from the **default
  branch**, so the list in force was master's `REVIEW_TOOL_ALLOWLIST` - which does not contain those
  four grants. The action prints the resolved list twice (the step's `claude_args`, and the SDK's
  `allowedTools` array) and they are absent from both. The reviewer's contrary self-check cited the
  two allowlists - `REVIEW_TOOL_ALLOWLIST:` in `.github/workflows/claude.yml`, and
  `--allowedTools "Bash(gh issue view` in `.github/workflows/claude-code-review-dispatch.yml` - **by
  line number**, and the line numbers it quoted exist in both copies, so nothing in the citation said
  which copy it had read. It had read the **PR's** copy over `gh pr diff`, not the copy underneath
  it. (That ambiguity is exactly why `AGENTS.md` -> "Cite by anchor" forbids `file:line`.) The rule it ran
  into is stated once, canonically, in [`docs/ci.md`](../ci.md) -> "Editing the reviewer"; this
  entry does not restate it.
  <!-- file-refs: N/A - names the script this note PROPOSES writing; it does not exist yet -->

  **What is left open is only that the grants remain unexercised**, which is the general condition
  of the bullet above rather than anything specific to these four: a grant a PR adds can never apply
  to that PR, on any route. The enumeration has since been replaced by `Bash(bin/check-*.sh:*)` and
  `Bash(bin/test-check-*.sh:*)` so that the *convention* is granted once instead of each script
  separately - but the patterns land unverified for the same reason, and the confirming step is the
  `--ref master` dispatch already named above, prompted to run a `bin/check-*.sh` and quote its
  output.
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
- **The `@claude` trigger fires on prose about it**, so a comment merely discussing the mechanism
  starts a billed job. Own note, since it is a distinct open defect:
  [`ci-claude-trigger-fires-on-prose.md`](ci-claude-trigger-fires-on-prose.md).
