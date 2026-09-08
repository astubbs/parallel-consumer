# BSD portability in the agent harness: what is still open after the Mac run

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-vetted: 2026-09-08 - applied: shrunk, the third item removed because its guard half shipped as `gnu-bsd` rows in `bin/check-shell-hazards.sh`; items 1 and 2 left as they were; checked: all three quarantine gates still carry the unreachable `|| source bin/lib/quarantine-common.sh` fallback under `set -e`, `bin/test-check-pr-ready.sh` still holds no executing `stat`/mtime case (its only match is a prose comment), and the hazards gate's `gnu-bsd` rows cover both `stat -c` and `stat -f` -->

<!-- post-merge: checked-begin -->
The hooks in `.claude/hooks/` and the gates in `bin/` were swept for GNU-only constructs, and the
sweep was then **executed on a Mac** rather than reasoned about
(astubbs/parallel-consumer#341). The whole `bin/` suite passes there now. What the class *is*, the
four defects it produced and how to avoid the next one are written up in
[`docs/solutions/workflow-issues/gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md`](../solutions/workflow-issues/gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md),
which **owns that knowledge**. The guard half of that class is now in place too:
`bin/check-shell-hazards.sh` carries `gnu-bsd` rows for `stat -c` and `stat -f` among others, so the
argument no longer has to be remembered - which retired this note's third item, the near-verbatim
repetition of the "probe, never fall back" reasoning across several script headers. What that leaves
is a comment at the point of use rather than an open item.

This note keeps only what is still open. Delete it when these are resolved.
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
