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

## An eighth site - bare `mktemp` - and one of its victims is a gate that stops gating

GNU `mktemp` invents a template when given none; BSD/macOS `mktemp` **requires** a template operand
(or `-t prefix`) and exits 1 on none. Every hook in `.claude/hooks/` writes its payload to a temp
file as its first act - deliberately, because the payload arrives on stdin and a dispatch prompt
clears Linux's `MAX_ARG_STRLEN` - so on macOS that first line fails and the hook is over before it
starts. `bin/` has roughly twenty more sites, all in fixtures.

Measured under a BSD-shaped `mktemp` stub, on a `gh pr merge --subject` that the gate denies on
Linux:

| Hook | On Linux | Under a BSD `mktemp` |
|---|---|---|
| `check-squash-subject.sh` | emits the `deny` verdict | prints `usage:` to stderr, **exits 0, no verdict** |
| `pre-commit-gate.sh` | same shape | same shape - `set -euo pipefail` aborts before the verdict |
| `inject-merge-checklist.sh` | injects the checklist | silent |

For the two gates this is the failure mode already recorded above for `stat`: `PreToolUse` reads a
non-zero exit with no verdict as a **non-blocking** error, so the guard allows what it exists to
refuse - and it does so on every invocation, not just an unlucky one. It is not a degraded read; the
gate is simply absent on macOS while still being registered, still appearing in
`docs/agent-harness.md`, and still passing every self-test on Linux CI.

`.claude/hooks/inject-branch-context.sh` no longer has the site - it is templated, and carries a
modelled-BSD-`mktemp` case with a negative control - because a hook whose correct output is silence
cannot afford a defect whose symptom is silence. The remaining sites are still open, and are listed
here rather than fixed alongside it because they belong to this note's sweep: the construct list that
produced "six sites" did not include `mktemp`, exactly as it did not include `xargs`.

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

`sed -i`, `date -d`, `awk -v`, `\b` in grep, plus this PR's six sites, the `xargs` one and the
`mktemp` one above. The
identical "probe, never fall back" reasoning is now repeated near-verbatim in three files.
`bin/AGENTS.md` has the precedent: the SIGPIPE class got a named write-up and a CI guard.
