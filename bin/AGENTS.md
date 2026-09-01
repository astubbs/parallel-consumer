# `bin/` - how these scripts relate to CI and to the reviewer

Repo scripts. This doc owns the conventions for writing a script in `bin/`; the root AGENTS.md
routes here and keeps only what binds every session. Two conventions live here because nothing else
enforces them.

## Naming a script here can grant it to the PR reviewer

`bin/check-*.sh` and `bin/test-check-*.sh` are granted to the review agent **by pattern**, so a
script matching either prefix becomes runnable by the reviewer the moment it is on the default
branch. Nobody approves it; the name is the grant.

**So do not give that prefix to a script that writes, publishes, deploys, or reaches the network
beyond `gh` reads.** The two prefixes were chosen to keep `deploy.sh`, `chaos-test.sh`,
`soak-test.sh` and friends outside the grant, and a misnamed script defeats that silently.

Everything else about the allowlist - the two boundaries it sits between, what still needs a manual
grant, and why a grant must land before the pull request that needs it - is in
[`docs/ci.md`](../docs/ci.md) -> "Editing the reviewer".

## Run them all with `bin/check-all.sh`, not from memory

**Before you push: `bin/check-all.sh`.** By default it globs `bin/check-*.sh` and runs them concurrently - seconds, not minutes, because a sweep slow enough to skip protects nothing. `--with-tests` adds `bin/test-*.sh`, the self-tests, which answer a different question ("do the gates still work") and are CI's job. Either way it globs, so a gate
added tomorrow is swept with no edit anywhere - nobody has to remember to register it, and nobody
has to remember it exists.

That property is the whole design, and it is why the discovery loop must stay a glob. The script
exists because astubbs#356 pushed a branch that failed `check-branch-self-reference.sh` in CI after
a local sweep of seven gates chosen by hand. The gate was not new, subtle, or broken; it was not on
somebody's list.

- **A skip is never a pass.** Exit 2 (cannot run) and exit 3 (nothing in scope) get their own
  columns and are excluded from the pass count, because a gate that measured nothing must not read
  like one that measured and found nothing.
- **Five scripts are not tree gates** - four report the state of a *pull request* and one needs a
  maven-log argument - so they are skipped by default and run under `--pr`. Since a hand-maintained
  list is exactly what this script abolishes, every name in it is **asserted to exist**: rename one
  and the runner exits 2 rather than quietly sweeping one gate fewer than it claims.

## A script that answered its question is finished - `exp-` says so up front

`bin/` grows and never shrinks. The prefixes carry most of the grouping already - `check-`, `test-`,
`ci-`, `build-` account for the large majority of what is here - and the residue is the problem: the
one-off drivers that answered one empirical question and then stayed forever, because nothing ever
says a script is done.

**An experiment driver takes the `exp-` prefix.** The test is what the script is *for*, not what it
runs: if its header states a question with a stopping condition - "does the failure rate move with
scale?", "does this stall always drain?" - it is an instrument, and the prefix says so to everyone
who lists this directory later. A script that measures something you would re-measure after any
change is a tool and keeps an ordinary name.

The prefix is deliberately not a subdirectory. Script paths here are cited from `pom.xml`, workflow
YAML, javadoc and docs, nothing checks those citations, and `AGENTS.md` already records a move that
left six stale pointers behind. A prefix buys the same legibility for none of that risk.

**When the question is answered, the method moves to [`docs/solutions/`](../docs/solutions/) and the
script goes.** The durable value of an experiment is how it was settled and what it found - the
control arm, the trap that voided the first attempt, the number. That is a write-up, and this repo
already keeps them. An executable nobody will run again is not a record; it is a file everyone has
to scroll past and no reader can tell from a live tool.

Two things this rule is NOT. It does not license deleting a driver whose question is still open -
"answered" means answered, and a note in `docs/inflight/` usually says which. And it does not apply
to a tool that happens to have been written for one investigation: if you would run it again after
changing the code it exercises, it is a tool, whatever it was written for.

**Nothing enforces any of this** - no gate can tell an answered question from an open one - so it is
a judgement made at merge, by whoever knows what the experiment found.

**And the half that keeps it findable: an `exp-` script needs a row in
[`docs/testing.md`](../docs/testing.md) -> "Experiment runners" BEFORE it merges** - the question it
answers, and whether that question is still open. Without it the script is discoverable only by
`ls bin/`, which is how six runners arrived referenced by no doc, no workflow and no other script,
while a seventh would have been written rather than found. That table is also where a question is
marked answered, so the same row that finds a live instrument is what retires a dead one.

**A row is owed only where no mechanism sweeps the file.** `check-*` and `test-*` are globbed by
`bin/check-all.sh`, so they are found by construction and naming them anywhere else is a copy that
can go stale. The scripts that need an index are exactly the ones nothing globs: the experiment
runners, and the handful of build helpers beside them. That is the test to apply before adding a
name to any list - **does something already find this?**

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

**Temporary CVE exclusions expire.** `bin/check-cve-exclusions.sh` parses the root pom's
`excludeVulnerabilityIds` and fails once an entry marked `TEMPORARY-SINCE: YYYY-MM-DD` is more than
90 days old - also on an undated, unparseable or future-dated marker, and on an id with no rationale
comment above it. It runs in Repo Hygiene rather than in the audit job that reads the same list,
because that job is skipped for fork PRs and dies early on a token expiry, which is exactly when an
unwatched list rots. It exits **3**, leaving 1 and 2 to keep the meanings
`bin/check-ossindex-audit.sh` gives them. Same class as the rule above - **Dependabot cannot catch
it**: the ids it was written for are in no advisory database, so no alert exists to fire, and a
blanket `ignore` had silenced the patch bump that would have retired them.

**Scope every `ignore` in `.github/dependabot.yml` to the update you actually fear.** A bare
`dependency-name` with no `update-types` silences the dependency completely, including the patch
release you are waiting for - and the failure is invisible, because nothing reports a PR that was
never opened. Blanket is right only for a genuine freeze (`net.bytebuddy`, held until wiremock 3.x).
Anything you expect to move again gets `update-types`.

**SHA pins are exempt**, and are not drift. The `astubbs/*` forks are pinned to a commit on a
*branch* on purpose - a moving branch ref would be the unsafe choice - so each use site tracks a
different ref by design. Dependabot ignores them for the same reason.
