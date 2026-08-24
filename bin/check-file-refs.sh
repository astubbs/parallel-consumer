#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Flags a cited repo file path that does not exist - the same rule the `PR Checklist` workflow
# enforces (AGENTS.md -> "Cite by anchor, never by line number").
#
# WHY THIS EXISTS
#
# The convention says to cite a path plus a greppable anchor, and to "run the grep before you commit
# the citation". docs/citations.md then said the quiet part out loud: "nothing in CI checks any of
# this, so the only thing standing between a reader and a confidently wrong pointer is the author
# having run it". It cost what you would expect - docs/ci.md told readers to run
# bin/check-review-gate-contract.sh, a script that has never existed, and the citation survived every
# review since astubbs#287 because a path that looks right reads as right.
#
# NO SECOND COPY OF THE RULE. This calls .github/scripts/file-ref-gate.js - the exact module the
# workflow requires - rather than reimplementing the matching in bash. Everything subtle (what counts
# as a citation rather than prose, the three ways a path may resolve, history pointers, the paragraph
# marker, the exempt documents) lives there and is unit tested by file-ref-gate.test.js.
#
# WHOLE TREE, NOT THE DIFF, for the reason the module's header gives: deleting a file does not change
# a line in the documents that cite it, so a diff-scoped gate cannot see the commonest way a citation
# breaks. There is nothing to configure and no base ref to get wrong.
#
# WORKING TREE INCLUDED, deliberately, matching bin/check-issue-refs.sh and
# bin/check-copyright-headers.sh: uncommitted and not-yet-added files are judged too, so you find out
# before `git commit`, not after `git push`.
#
# THE ORACLE IS `git ls-files` PLUS UNTRACKED-BUT-NOT-IGNORED, AND NOTHING ELSE - not `test -e`.
# An ignored path (anything under target/, a scratch directory) exists on your machine and not in CI,
# so honouring it would make this script pass what the gate then fails. A local check that disagrees
# with CI is worse than no local check: it teaches you to trust the wrong answer.
#
# Usage: bin/check-file-refs.sh [--help]
# Exit codes: 0 = clean, 1 = dangling references found, 2 = cannot run (node missing, bad argument).

set -euo pipefail

cd "$(dirname "$0")/.."

# shellcheck source=bin/lib/node-gate.sh
source "${BASH_SOURCE[0]%/*}/lib/node-gate.sh" 2>/dev/null \
    || source bin/lib/node-gate.sh 2>/dev/null \
    || { echo "ERROR: cannot load bin/lib/node-gate.sh - the helper that classifies node's exit." >&2
         echo "       This is NOT a finding. Nothing was checked." >&2
         exit 2; }

# NECESSARY BUT NOT SUFFICIENT, which is the whole reason bin/lib/node-gate.sh exists: this answers
# "is node installed", and a node that is installed can still die at startup. That case is
# classified after the run, below.
if ! command -v node >/dev/null 2>&1; then
    echo "ERROR: node not found - needed to reuse .github/scripts/file-ref-gate.js." >&2
    echo "The authoritative gate is the 'PR Checklist' workflow; this is the local mirror of it." >&2
    exit 2
fi

case "${1:-}" in
    "") ;;
    -h|--help)
        # Print this header rather than a hand-maintained usage string, and stop at the first
        # non-comment line rather than at a line NUMBER - an earlier `sed -n '6,40p'` had already
        # rotted past its range and swallowed the exit codes, which is exactly the habit this gate
        # exists to discourage.
        while IFS= read -r line; do
            case "$line" in
                '#!'*) ;;
                '#'|'') printf '\n' ;;
                '#'*) printf '%s\n' "${line#\# }" ;;
                *) break ;;
            esac
        done < "$0"
        exit 0
        ;;
    *)
        echo "ERROR: unexpected argument '${1}'. Usage: bin/check-file-refs.sh [--help]" >&2
        exit 2
        ;;
esac

# `set +e` so a non-zero node status reaches node_gate_verdict instead of exiting here with it -
# which is precisely how a node that never started got reported as "dangling references found".
set +e
node <<'NODE'
const fs = require("fs");
const { execFileSync } = require("child_process");
const gate = require("./.github/scripts/file-ref-gate.js");

const git = (args) => execFileSync("git", args, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
const lines = (s) => s.split("\n").filter(Boolean);

// Untracked-but-not-ignored counts: a brand-new document full of dangling citations should fail
// here rather than waiting for CI. Ignored files deliberately do not - see the header.
const tracked = [
  ...lines(git(["ls-files"])),
  ...lines(git(["ls-files", "--others", "--exclude-standard"])),
];
const tree = gate.treeFrom(tracked);

const docs = tracked
  .filter((f) => gate.CITING_FILE.test(f))
  .map((filename) => {
    try {
      return { filename, lines: fs.readFileSync(filename, "utf8").split("\n") };
    } catch {
      return { filename, lines: [] };   // deleted in the working tree
    }
  });

const baseRev = process.env.FILE_REFS_BASE
  || (() => { try { git(["rev-parse", "--verify", "origin/master"]); return "origin/master"; }
              catch { return "master"; } })();

let dangling = gate.danglingRefs(docs, tree);
let scope = `${docs.length} citing file(s), whole tree`;

// The base tree comes from the gate module, the same reader CI uses - the byte-exact slicing it
// does is not something to keep two copies of.
const baseTree = gate.readTreeDocs(baseRev, (args, opts = {}) =>
  execFileSync("git", args, {
    encoding: opts.encoding || "utf8", input: opts.input, maxBuffer: 512 * 1024 * 1024,
  }));

if (baseTree) {
  const inherited = gate.danglingRefs(baseTree.docs, gate.treeFrom(baseTree.names));
  const before = dangling.length;
  dangling = gate.newFindings(dangling, inherited);
  scope += `, ${before - dangling.length} inherited from ${baseRev} ignored`;
} else {
  scope += `, base ${baseRev} unavailable - reporting every finding`;
}

if (dangling.length === 0) {
  console.log(`No new dangling file references (${scope}).`);
  process.exit(0);
}

console.error(gate.formatFailure(dangling));
process.exit(1);
NODE
node_status=$?
set -e

node_gate_verdict "$node_status" ".github/scripts/file-ref-gate.js" || exit $?
exit 0
