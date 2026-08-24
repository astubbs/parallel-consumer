# astubbs#341 - GNU/BSD portability: what the review left open

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

Findings from the simplify-and-review pass on this branch that astubbs#341 does **not** close.
Delete this note when they are resolved, not when the PR merges.

## A seventh site, and it does not go silent - it lies

`.claude/hooks/inject-recorded-knowledge.sh` uses `xargs -r` in three places. `-r` is a GNU
extension; BSD/macOS `xargs` rejects it. Measured under a BSD-shaped `xargs`: the **Registers**
section drops from 13 entries to 4, and the "Not shown above" section vanishes while its notes are
relabelled *"Their `inflight-type` or `inflight-impact` is missing or misspelt"* - which is false.

Worse than the `paste` defect this PR fixes: not absence but an active misstatement, inside the hook
whose whole purpose is that prior art must not be invisible. The PR's sweep construct list did not
include `xargs`, so its "no others found" is incomplete.

Unfixed because the usual `/dev/null` operand workaround does not work for `grep -L` - the fix is a
restructure, and the premise cannot be confirmed on Linux.

## The PR body states the rationale wrongly, and the truth is stronger

The body says a blind `stat -c %Y || stat -f %m` fallback is wrong because on Linux `stat -f` is
`--file-system` and *succeeds*, "returning a number about the filesystem". Measured: GNU
`stat -f %m FILE` **exits 1 while printing six lines of prose to stdout**. That prose reaches
`$(( now - mtime ))`, `set -u` aborts the hook, and `PreToolUse` reads a non-zero exit with no
verdict as a non-blocking error - so the guard **allowed the merge**. Probing rather than falling
back is right for a worse reason than the one written down. Correct the body before merge; it is a
GitHub artefact and was not edited here.

## Two fixes have no executing coverage

- `bin/check-branch-self-reference.sh` - the pre-fix `mapfile` version was swapped back in and
  `bin/test-check-branch-self-reference.sh` still passed 31/31. That suite cannot catch a
  reintroduced `mapfile` bug on any bash 4+ host.
- `bin/check-pr-ready.sh` - `bin/test-check-pr-ready.sh` holds no `stat` or `mtime` reference at
  all; it greps the script's source text.

## Needs a real BSD userland - unprovable on Linux

1. `PATH=/usr/bin bin/test-check-agent-hooks.sh` - settles the PR's "10 failures to 0" claim.
2. `xargs -r true </dev/null; echo $?` - settles the finding above.
3. `.claude/hooks/inject-recorded-knowledge.sh | grep -A20 '^# Registers'` - shows the degradation.
4. `bash --version && bin/check-branch-self-reference.sh` - bash 3.2 treats an empty array under
   `set -u` as an unbound-variable error, so `candidates=()` paired with `${#candidates[@]}` would
   turn the friendly "no documents to check" exit 0 into exit 1. Unreachable today because
   `docs/inflight/AGENTS.md` always matches the pathspec.
5. `stat -f %m <file>; echo $?` - confirms the BSD arm returns a bare integer, as the new shape
   guard requires.

## The class has bitten 10+ times and has no `docs/solutions/` entry

`sed -i`, `date -d`, `awk -v`, `\b` in grep, plus this PR's six sites and the `xargs` one above. The
identical "probe, never fall back" reasoning is now repeated near-verbatim in three files.
`bin/AGENTS.md` has the precedent: the SIGPIPE class got a named write-up and a CI guard.

## The node gates' `source` line reintroduces the class it fixes

`bin/lib/node-gate.sh` is sourced as:

    source "${BASH_SOURCE[0]%/*}/lib/node-gate.sh" 2>/dev/null || source bin/lib/node-gate.sh

Under `set -e`, if both attempts fail the script exits with `source`'s status, **1** - which both
gates reserve for "violations found". Measured by moving the lib aside: `bin/check-issue-refs.sh`
and `bin/check-file-refs.sh` both exit 1, reporting a policy violation because a helper was
missing. That is the same defect one level up, and it is the objection astubbs#341's body raises
against `bin/lib/` in the first place.

Low reachability - the lib is tracked, and both callers `cd` to the repo root before sourcing, so
the fallback resolves in any normal checkout. The fix is one line, `|| { echo ...; exit 2; }`, and
it wants a self-test case in the style of the existing `check_cannot_run` ones.
