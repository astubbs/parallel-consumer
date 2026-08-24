#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE single home of "did the node gate actually RUN, or did node die before it got there?" -
# shared by bin/check-issue-refs.sh and bin/check-file-refs.sh. Source this; do not copy from it.
#
# WHY IT EXISTS. Both scripts guarded with `command -v node`, which proves node is PRESENT and says
# nothing about whether it can RUN. When NODE_OPTIONS carries a `--require` naming a file that no
# longer exists - an agent harness writes a preload into a temp directory, the temp directory is
# later cleaned up - every node process dies during preload, before one line of the gate executes:
#
#     Error: Cannot find module '.../restore-node-options.cjs'
#     requireStack: [ 'internal/preload' ]
#
# node's exit status for that is **1**, and both scripts reserve 1 for "violations found". So a gate
# that checked nothing reported a policy violation. That is not hypothetical - it happened on a real
# machine and cost a real debugging detour, because `bin/check-issue-refs.sh` exiting 1 reads as
# "you wrote a bare #NN", not as "your environment is broken". Both scripts already document 2 for
# "cannot run"; the guard simply never reached it. Failing closed where a script guards is the whole
# point of the branch this landed on (astubbs/parallel-consumer#341).
#
# WHY THE PROBE RUNS ON THE FAILURE PATH RATHER THAN UPFRONT. An unconditional `node -e ''` before
# each gate would cost ~90ms of interpreter start apiece, and .githooks/pre-commit budgets ~1.5s for
# its whole run - which these two gates already very nearly spend between them. Probing only after a
# non-zero result costs nothing on the clean path, and the one case it slows down is a case whose
# output you are about to read anyway. It is also the stronger check: an upfront probe only proves
# node worked a moment BEFORE the run.
#
# THE PROBE LOADS THE GATE MODULE, it does not merely start node, so "node cannot start" and "the
# module cannot be loaded or parsed" both land on 2 - "cannot run" - instead of the second
# masquerading as a finding. A module that loads and then throws at runtime is still reported as a
# finding; that residue is covered by the modules' own unit tests in the `PR Checklist` workflow.

# node_gate_verdict <node-exit-status> <gate-module-path-from-repo-root>
#
# Returns the CALLING SCRIPT's exit code, per the contract both callers state in their headers:
#   0 - node ran and reported clean
#   1 - node ran and reported a real finding
#   2 - node never reached a verdict, so nothing was checked
#
# Call it as `node_gate_verdict "$status" <module> || exit $?` - on the left of `||` so the caller's
# `set -e` does not turn the 1 and 2 answers into an exit before the message is used.
node_gate_verdict() {
    local status=$1 module=$2

    if [ "$status" -eq 0 ]; then
        return 0
    fi

    # `require` from `-e` resolves relative to the current directory, and both callers cd to the
    # repo root first. Output is discarded: if node itself is broken, the run above has already
    # printed the same stack trace, and a second copy buries the explanation below.
    if node -e 'require(process.argv[1])' "./$module" >/dev/null 2>&1; then
        return "$status"
    fi

    echo "ERROR: the gate in ${module} did not run - node exited ${status} without reaching a verdict." >&2
    echo "       This is NOT a finding. Nothing was checked; see node's own error above." >&2
    if [ -n "${NODE_OPTIONS:-}" ]; then
        echo "       NODE_OPTIONS is set: ${NODE_OPTIONS}" >&2
        echo "       A stale --require in there - one naming a file that has since been deleted - kills" >&2
        echo "       node during preload, before any script runs. Clear it and run this again." >&2
    fi
    echo "       The authoritative gate is the 'PR Checklist' workflow; this is the local mirror of it." >&2
    return 2
}
