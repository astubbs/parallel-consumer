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

## A latent instance of the bash 3.2 `source` defect

`bin/check-quarantine-registry.sh`, `bin/quarantine-lane-report.sh` and
`bin/check-quarantine-owners.sh` all run

    source "${BASH_SOURCE[0]%/*}/lib/quarantine-common.sh" 2>/dev/null || source bin/lib/quarantine-common.sh

under `set -e`. On bash 3.2 a failed `source` is fatal, so the `||` fallback is unreachable - the
same defect fixed in the two node gates, which now test `[ -r ]` before sourcing.

It is **latent, not live**: `${BASH_SOURCE[0]%/*}` resolves for every ordinary invocation, so the
first `source` succeeds and the dead fallback is never reached. `bin/test-check-quarantine-registry.sh`
passes on macOS. It becomes real the moment that path stops resolving - and then it fails silently,
with an exit code that means something else. Nothing has claimed these three yet, and the
`shell: macos` lane cannot surface it either: the fallback stays unreached on every platform until
that path breaks.

## One fix still has no executing coverage

`bin/check-pr-ready.sh` was fixed for BSD `stat`, but `bin/test-check-pr-ready.sh` holds no `stat`
or `mtime` reference at all - it greps the script's source text. **The `shell: macos` lane does not
help here**, unlike the other fixes: a source-text grep passes identically on both platforms, so
running it on macOS asserts nothing new. This one needs a case that actually dates a file and reads
the result back.

## The "probe, never fall back" reasoning is stated three times

The identical argument is repeated near-verbatim in three files. `bin/AGENTS.md` has the precedent
for collapsing that - the SIGPIPE class got a named write-up and a CI guard - and the write-up half
now exists (linked above). The guard half does not.
