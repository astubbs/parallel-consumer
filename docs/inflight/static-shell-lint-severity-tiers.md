# ShellCheck severities below `error` are not gated, and why

<!-- inflight-type: register -->
<!-- inflight-impact: ci -->

`bin/check-shell-lint.sh`, run as part of the single `repo: hygiene` job in `repo-hygiene.yml` (there
is no separate `shell: lint` job any more - see [`docs/ci.md`](../ci.md)), gates on **errors only**.
This note says what the other severities contain and what would turn each on. Same contract as
[`docs/inflight/static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md): a severity that
is off carries a reason and a trigger, and the off-set only shrinks.

**A note on what the profile split means here.** ShellCheck's floor is a severity, not a rule list,
so "full rules on new code" means running new or changed scripts at `warning` (or lower) while the
corpus stays at `error`. `SC2016` is the one code that would still need silencing at any floor,
because single-quoted `$` is deliberate wherever these scripts build awk programs and grep patterns.

**There is deliberately no per-code suppression list.** `bin/check-shell-lint.sh` has one knob, the
severity floor, overridable per-run with `SHELL_LINT_SEVERITY`. The moment a code-level allowlist
exists it grows, and this repo has just spent a PR removing exactly that shape from SpotBugs.

## Measured 2026-08-25, on the tree that introduced the lane

Errors: 3, all fixed in the same change - two real (`SC1072`/`SC1073` from a prose comment parsed as
a directive, which aborted analysis of an entire file, and `SC1087` ambiguous array expansion) and
their knock-on. Now 0.

**The warning count went UP after fixing them, from 10 to 14, and that is the directive bug's real
size.** ShellCheck had been aborting on `bin/lib/source-patterns.mjs` (rule `sigpipe-into-grep-q`) before reaching its body, so
that file's four findings were never reported. A count taken before the fix understated the corpus,
and nothing said so - which is the same shape as the rest of this PR.

Not gated:

**`profile:` splits these the same way the SpotBugs registry is split** - see
[`static-analysis-rule-profiles.md`](static-analysis-rule-profiles.md). `new` means the severity is
right and only the existing corpus blocks it; `old` means it stays off everywhere.

| Severity | Count | Profile | Why not gated | Turns on when |
|---|--:|---|---|---|
| `warning` | 14 | `new` | `SC2034` (unused variable, 6) is the largest group and includes shared-library exports the linter cannot see used across a `source` boundary. Then `SC2164` (`cd` without `\|\|`, 4) and `SC2155` (declare-and-assign masking a return value, 3). **"None currently causing a defect" stopped being true - see below.** | Somebody works the remainder off. This is the next floor to raise and the cheapest, and it now has an incident behind it rather than only tidiness. |
| `info` | 40 | `new` except `SC2016`, which is `old` | Dominated by `SC2016` - single-quoted `$` in strings, overwhelmingly deliberate here because these scripts build awk programs and grep patterns - and `SC2001`, `sed` where parameter expansion would do. | Only after `warning` is clean, and probably never for `SC2016`. |
| `style` | 18 | `old` | Preference. | Not planned. |

## The warning floor has now missed a real defect: SC2215

**`SC2215` is a `warning`, so the gate at `error` passed a script that had been silently broken for
weeks.** `bin/ci-mutation-test.sh` carried a comment *inside* the backslash continuation of its
`./mvnw` invocation. The continuation splices the `#` onto the command line, so the shell truncated
the command there and every following line became a separate command. The lane therefore ran without
`-pl parallel-consumer-core -am` (mutating every module instead of the PR's), without the Lincheck
`-DexcludedTestClasses` the deleted comment was explaining, and without its output formats, timeouts
and thread count - then exited 127 from the orphaned argument, whose `tee` also overwrote the PIT log
the script parses for its verdict.

ShellCheck names this exactly - *"This flag is used as a command name. Bad line break or missing
[ .. ]?"* - and `shellcheck --severity=error` does not report it. Reproduce the gap:

```bash
printf '#!/usr/bin/env bash\nfoo --a \\\n  --b \\\n  # comment\n  --c\n' > /tmp/probe.sh
shellcheck /tmp/probe.sh                    # SC2215, exit 1
shellcheck --severity=error /tmp/probe.sh   # silent, exit 0
```

**The shebang in that probe is load-bearing, not boilerplate.** Without it the file also trips
`SC2148` (*"Tips depend on target shell and yours is unknown"*), which IS an error - so the second
command reports a finding and exits 1, and the snippet appears to disprove the gap it is
demonstrating.

**This did not become a per-code promotion, deliberately.** The one-knob rule above is the reason:
the moment `--include=SC2215` is added, the next incident adds another code, and the list is the
shape this repo has already removed from SpotBugs once. The floor is the knob. What this incident
changes is the *argument* for raising it - the warning tier is no longer a tidiness backlog, it is a
tier with a shipped defect in it.

**A whole-tree sweep for the class found no other instance** - `SC2215` matched the one site and
nothing else. Re-run it over every script in `bin/`, `.claude/hooks/` and `.github/`:

```bash
find bin .claude/hooks .github -type f -name '*.sh' -print0 \
  | xargs -0 shellcheck -f gcc --severity=warning 2>/dev/null | grep SC2215
```

The gap is covered behaviourally in the meantime: `bin/test-ci-mutation-test.sh` gained argv arms
that run the real invocation branch against a stub `mvnw` and assert the flags arrive. They are
**proven red against the pre-fix script** - restore master's `bin/ci-mutation-test.sh` over the
fixed one and re-run the self-test: every argv arm flips and every other arm stays green either
way, which is what shows the pre-existing arms could never have caught it.

## Top 5 to turn back on whole-tree, ranked

ShellCheck's floor is a **severity**, not a rule list, so raising the floor to `warning` turns on all
of these at once. That makes the ranking a work order rather than five separate switches: clear them
in this order and the floor moves on its own once the list is empty. Fourteen findings total, so this
is genuinely finishable.

Ranked by how close each sits to a failure this repo has actually paid for.

| # | Code | Sites | Why this one | Effort |
|---|---|--:|---|---|
| 1 | `SC2155` | 3 | **Declare-and-assign masks the return value** - `local x=$(cmd)` swallows `cmd`'s exit status. That is the exit-code-swallowing mechanism behind several of this month's silent false greens, in a repo whose gates are shell scripts. | Mechanical, split each line |
| 2 | `SC2164` | 4 | `cd` without `\|\|` - the run continues in the wrong directory. Same failure shape as the BSD class: accepted, and means something else. | Mechanical |
| 3 | `SC2010` | 1 | `ls \| grep` instead of a glob. One site, and it breaks on filenames this repo will eventually have. | Mechanical |
| 4 | `SC2034` (shared-lib exports) | 4 | `INFLIGHT_*` in `bin/lib/inflight-tags.sh`. **Not a defect** - the linter cannot see them used across a `source` boundary. Needs a directive at the definitions, which is legitimate use of a suppression rather than silencing a finding. | One directive |
| 5 | `SC2034` (`rc`, `out`) | 2 | The remaining two are genuinely unused locals, so unlike the four above these are real. | Read 2 sites |

Clearing all five raises the floor from `error` to `warning`, which is the single biggest coverage
gain available to this lane. **After that the next floor is `info`, and it should probably never
move** - 34 of its findings are `SC2016`, and single-quoted `$` is deliberate everywhere these
scripts build awk programs and grep patterns.

## What this lane cannot do

**ShellCheck has no bash-version awareness.** `--shell=bash` means bash of any version, so a bash-4
builtin on a bash 3.2 platform - `mapfile`, the construct that cost this repo an exit-127 gate on
macOS - passes clean. It is reported only under `--shell=sh` or `dash`, as POSIX portability, and
these are bash scripts.

That class belongs to the `shell: macos` lane, which runs the scripts under the real 3.2.
`bin/test-check-shell-lint.sh` asserts this gap as a **deliberately green** case, so if a future
ShellCheck gains version awareness the self-test goes red and tells somebody to widen this lane.
