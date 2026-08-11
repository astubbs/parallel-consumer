# `bin/` - how these scripts relate to CI and to the reviewer

Repo scripts. Two conventions live here because nothing else enforces them.

## Adding a verification script? Consider granting it to the reviewer

`.github/workflows/claude-code-review-dispatch.yml` gives the PR reviewer an **enumerated
allowlist** of commands (`--allowedTools`). It is not a glob, and there is no automated check that
it stays in step with this directory - so a new script is invisible to the reviewer until someone
adds it by hand. (It lived in `claude-code-review.yml` until the reviewer moved to on demand; that
file is now just the gate.)

That gap is not theoretical. `bin/test-check-review-posted.sh` went ungranted while its sibling
`bin/test-check-copyright-headers.sh` was granted, so when PR astubbs#210 changed the review gate, the
reviewer could not run the gate's own self-test and had to reason about the fix statically instead -
on a PR whose whole subject was that gate misreporting.

**Grant a script when it is read-only and lets the reviewer check a claim rather than infer it** -
`check-*.sh`, `test-check-*.sh`, the `ci-*-test.sh` wrappers. A reviewer that can re-run what a PR
asserts catches a false claim; one that cannot is guessing.

**Do not grant** anything that writes, publishes, or reaches the network beyond `gh` reads. Two
different boundaries meet at that allowlist, and mistaking one for the other is how it gets widened:

- **Whose code the job runs** is settled by the reviewer's own fork guard - its "Validate inputs and
  refuse fork heads" step rejects any PR whose head is not in this repo, because
  `workflow_dispatch` holds `CLAUDE_CODE_OAUTH_TOKEN` while checking out `refs/pull/<pr>/head`. The
  old `pull_request` trigger got that for free, since GitHub withholds secrets from fork PRs; on a
  dispatch it is explicit. The allowlist has nothing to do with this one.
- **What that code can talk the reviewer into running** is what the allowlist is for, and the fork
  guard does nothing for it. An in-repo head does not make the diff, the PR body and the comments
  trustworthy - they are still attacker-influencable text being fed to a model that can call Bash.
  Enumerating the commands rather than granting `Bash(*)` is the margin against
  injection-into-execution.

Widening it to `bin/*` hands away the second margin for the convenience of not editing one line, and
the first guard will not notice.

**Grant BOTH spellings.** These are prefix matches, not globs: `Bash(bin/foo.sh:*)` does **not**
match `./bin/foo.sh`. Every entry is listed twice for that reason, and a half-added grant is worse
than none - the reviewer's invocation fails in a way that reads like the script is broken.

**Editing the reviewer does not cost you the review on that PR, but your new grant does not apply
to it either.** The action's workflow-validation guard skips itself (exiting 0) whenever the
workflow file invoking it differs from the default branch - but the reviewer is dispatched
`--ref master`, so it runs master's copy of the workflow and reviews normally, without the new
grant. Expect the reviewer to say it lacks it. The grant goes live on merge.

## Scripts that guard other scripts

`test-check-*.sh` files are self-tests for the corresponding `check-*.sh`, and CI runs them **before**
the gate they protect (see the "Self-test the review gate" step). When you fix a bug in a checker,
add the case to its self-test and verify it goes red against the old code - a regression test that has
never failed proves nothing.

Two structural guards exist and are worth copying into any new checker's self-test:

- **No `printf | grep -q` or `| awk` under `set -o pipefail`.** The early-exiting reader closes the
  pipe, the writer takes `EPIPE`, and `pipefail` promotes 141 to the pipeline's status - so *matching*
  becomes a failure. It needs more than one pipe buffer (64 KiB) of trailing input to bite, which is
  why it survives small fixtures. Use a herestring. **`bin/check-shell-sigpipe.sh` enforces this
  across every script in this directory** and runs in CI, so a new violation fails the build rather
  than waiting to be noticed - `bin/check-review-posted.sh` shipped with one and misreported four
  PRs first. `shellcheck` does **not** catch this pattern (verified against the known-bad line,
  which it passed clean), which is why the guard is a bespoke grep rather than a linter. It
  matches every flag spelling - `-q`, `-qE`, `-Eq`, split flags (`grep -v -q`), `--quiet`,
  `--silent` - and skips exactly two files, itself and its self-test, because both must carry
  the anti-pattern as data. Anything else it skipped would be a violation in hiding.
- **Fixtures big enough to reach the failure.** The review gate's self-test has cases for a match
  buried mid-body and a match at the very end, and neither can trigger the bug - the first is small,
  the second has nothing following it. A case that reaches it is added in astubbs#210.

## Workflows

**One version per action.** `bin/check-action-versions.sh` fails if any action appears at two
versions across `.github/workflows/`, and runs in the Repo Hygiene workflow.

The rule is deliberately not "`actions/checkout` must be `v6`" - that rots on the next bump. "Pick a
version and use it everywhere" survives upgrades, because Dependabot raises every use of an action
in a single grouped PR, so a consistent repo stays consistent.

It exists because **Dependabot cannot catch this class**: it bumps versions that are already there
and has nothing to say about a *new* workflow authored at an old one. That is exactly how
`repo-hygiene.yml` shipped at `actions/checkout@v4` while 21 other uses were on `@v6` - a human
caught it in review, and the same mistake had been seen before.

The github-actions ecosystem was re-enabled in `.github/dependabot.yml` at the same time; it had
been off since 2022, when the repo genuinely had no workflows.

**SHA pins are exempt**, and are not drift. The `astubbs/*` forks are pinned to a commit on a
*branch* on purpose - a moving branch ref would be the unsafe choice - so each use site tracks a
different ref by design. Dependabot ignores them for the same reason.
