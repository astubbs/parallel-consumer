# `bin/` - how these scripts relate to CI and to the reviewer

Repo scripts. This doc owns the conventions for writing a script in `bin/`; the root AGENTS.md
routes here and keeps only what binds every session. These conventions live here because nothing
else enforces them. The portability rule below is the exception to the directory in the title: it
binds **every shell script in the repo**, including `.githooks/` and `.claude/hooks/`.

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

## Every script must be cross-platform, and degrade rather than guess

Scripts here run on macOS dev machines, Linux CI runners, and self-hosted runners, so **a script that
only works on the author's machine is a broken script**. Two rules, and the second is the one that
actually bites:

- **Never let a missing tool become a confident wrong answer.** Probe for what you need and skip what
  is absent, saying so. `docker`, `gdate`, `realpath`, `timeout` and `sha256sum` are all absent
  somewhere this repo runs. A check that cannot run must report that it did not run - the failure
  mode to design against is the one where "I could not measure it" is indistinguishable from "it is
  fine".

- **`stat -f` is NOT a portable idiom, and must never be used as a try-then-fallback.** On macOS
  `stat -f %b` is a *file's* allocated blocks; on GNU/Linux `-f` switches stat into *filesystem*
  mode, where `%b` is the total blocks in the filesystem. It exits **0** and returns a number three
  orders of magnitude too large, so `stat -f ... || stat -c ...` silently produces garbage on Linux
  rather than falling through. Resolve the platform once from `uname -s` and branch. The same
  divergence applies to `date -r` vs `date -d`, `sed -i ''` vs `sed -i`, and `readlink -f`.

  `.claude/hooks/warn-low-disk.sh` is the worked example, and it also shows the seam that makes such
  a branch testable: which `stat` **syntax** to speak is read from the real `uname` and is never
  injectable, while which platform **layout** to look for is injectable - so a self-test can exercise
  the macOS branch on a Linux runner without pretending Linux has BSD stat.

- **`df` needs `-P`.** Without POSIX output a long device name wraps onto its own line on Linux and a
  column-indexing `awk` silently reads the wrong field.

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

**One toolchain declaration.** `bin/check-toolchain-versions.sh` fails when `mise.toml` and the
`.github/workflows/clients.yml` matrix disagree about a language's version, and runs in Repo Hygiene
behind its own self-test. The two *install* differently on purpose - mise locally, `setup-*` actions
on the runner, which carry the client matrix's caching and a SHA-pinned `ruby/setup-ruby` - but they
must never *declare* differently. Same class as the rule above: **nothing else could catch it**, and
they had already drifted by whole major versions (dotnet 8 against 9, node 22 against 25) before the
gate existed. The four languages with no host toolchain (swift and cpp build in containers, kotlin
and scala on the Maven reactor) are asserted absent from both, so "nobody declared it" cannot be
mistaken for "it deliberately has none".

`bin/build-client.sh` asserts the *installed* version separately, at build time, on major.minor
only - the declarations are exact, but a developer one patch along from the pin is not a problem
worth blocking, and every failure the assertion exists for was a major or minor gap.

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
