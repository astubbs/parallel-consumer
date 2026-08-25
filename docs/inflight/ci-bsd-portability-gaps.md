# BSD portability in the agent harness: what is still open after the Mac run

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

<!-- post-merge: checked-begin -->
The hooks in `.claude/hooks/` and the gates in `bin/` were swept for GNU-only constructs, and the
sweep was then **executed on a Mac** rather than reasoned about
(astubbs/parallel-consumer#341). The whole `bin/` suite passes there now. What the class *is*, the
four defects it produced and how to avoid the next one are written up in
[`docs/solutions/workflow-issues/gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md`](../solutions/workflow-issues/gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md),
which **owns that knowledge**. This note keeps only what is still open. Delete it when these are
resolved.
<!-- post-merge: checked-end -->

## Nothing runs any of this on macOS in CI

**This is the item that matters.** Every lane is ubuntu, so the entire class is caught only when
somebody happens to be working on a Mac - which is how four live defects survived a deliberate
sweep, two automated reviews and two human lgtms. The next one will survive the same way.

A `macos-latest` lane running `bin/test-check-*.sh`, `bin/test-rename-packages.sh` and a
`/bin/bash -n` parse sweep over every tracked script would cost a couple of minutes and close it.
Not done here because adding a required check is a repo-wide decision, not a side effect of a
portability fix.

## A latent instance of the bash 3.2 `source` defect

`bin/check-quarantine-registry.sh`, `bin/quarantine-lane-report.sh` and
`bin/check-quarantine-owners.sh` all run

    source "${BASH_SOURCE[0]%/*}/lib/quarantine-common.sh" 2>/dev/null || source bin/lib/quarantine-common.sh

under `set -e`. On bash 3.2 a failed `source` is fatal, so the `||` fallback is unreachable - the
same defect fixed in the two node gates, which now test `[ -r ]` before sourcing.

It is **latent, not live**: `${BASH_SOURCE[0]%/*}` resolves for every ordinary invocation, so the
first `source` succeeds and the dead fallback is never reached. `bin/test-check-quarantine-registry.sh`
passes on macOS. It becomes real the moment that path stops resolving - and then it fails silently,
with an exit code that means something else. Left alone here because these three are master-state
and nothing on this branch touches them.

## Two fixes still have no executing coverage

- `bin/check-branch-self-reference.sh` - the pre-fix `mapfile` version was swapped back in and
  `bin/test-check-branch-self-reference.sh` still passed 31/31. That suite cannot catch a
  reintroduced `mapfile` bug on any bash 4+ host. It does at least now *run* on bash 3.2, where it
  previously failed to parse.
- `bin/check-pr-ready.sh` - `bin/test-check-pr-ready.sh` holds no `stat` or `mtime` reference at
  all; it greps the script's source text.

## The "probe, never fall back" reasoning is stated three times

The identical argument is repeated near-verbatim in three files. `bin/AGENTS.md` has the precedent
for collapsing that - the SIGPIPE class got a named write-up and a CI guard - and the write-up half
now exists (linked above). The guard half does not.
