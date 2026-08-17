#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Flags unqualified `#NN` references below #1000 on lines this branch ADDS - the same rule the
# `PR Checklist` workflow enforces (AGENTS.md -> Issue references).
#
# WHY THIS EXISTS
#
# The rule was previously only reachable from CI, so the first time you learned you had written a
# bare `#118` was a red check several minutes after pushing. That is a slow loop for a
# one-character fix, and it cost two CI round-trips on astubbs#217 alone.
#
# NO SECOND COPY OF THE RULE. This calls `.github/scripts/issue-ref-gate.js` - the exact module the
# workflow requires - rather than reimplementing the regex in bash. A bash re-implementation would
# drift from the gate, and a local check that disagrees with CI is worse than no local check: it
# teaches you to trust the wrong answer. Everything subtle (exempt paths, javadoc `{@link #close()}`
# false positives, markdown/anchor/URL forms that already qualify a number) lives there and is unit
# tested by issue-ref-gate.test.js.
#
# WORKING TREE INCLUDED, deliberately, matching bin/check-copyright-headers.sh: uncommitted edits are
# judged too, so you find out before `git commit`, not after `git push`. CI necessarily sees only
# what you pushed.
#
# WHERE IT CAN DISAGREE WITH CI. The rule is shared, but the input is not, in two ways:
#
#   * CI reads patches from GitHub's `pulls.listFiles`, which omits `patch` on a very large diff, and
#     the gate skips a file it cannot see. This script builds its own patch with `git diff`, so it
#     still checks that file - it flags something CI would silently pass.
#   * CI also scans the PR BODY (`gate.prBodyEntry`) and honours the `issue-refs: N/A` opt-out
#     written there. When `gh` can resolve the current branch to a PR, this script does the same -
#     without that, the opt-out was one-sided: CI accepted it while every local run stayed red on
#     the same file forever, teaching people the local check lies. When there is no PR yet (or no
#     gh, or no network) there is no body to read, so a bare `#NN` destined for the description is
#     still CI's to catch, and an opt-out you are only planning to write cannot be honoured.
#
# So a red result here is always real, but a green one promises neither that CI examined every file
# nor that a description you have not written yet will pass.
#
# Usage: bin/check-issue-refs.sh [base-ref]      (default base: origin/master, else master)
# Exit codes: 0 = clean, 1 = unqualified refs found,
#             2 = cannot run (node missing, or no merge base with the base ref - e.g. a shallow clone).

set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v node >/dev/null 2>&1; then
    echo "ERROR: node not found - needed to reuse .github/scripts/issue-ref-gate.js." >&2
    echo "The authoritative gate is the 'PR Checklist' workflow; this is the local mirror of it." >&2
    exit 2
fi

BASE_REF="${1:-}"
if [ -z "$BASE_REF" ]; then
    if git rev-parse --verify -q origin/master >/dev/null; then
        BASE_REF=origin/master
    else
        BASE_REF=master
    fi
fi

if ! MERGE_BASE=$(git merge-base "$BASE_REF" HEAD 2>/dev/null); then
    echo "ERROR: cannot find a merge base with ${BASE_REF}." >&2
    exit 2
fi

node - "$MERGE_BASE" <<'NODE'
const { execFileSync } = require("child_process");
const gate = require("./.github/scripts/issue-ref-gate.js");

const base = process.argv[2];
const git = (args) => execFileSync("git", args, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });

// owner/name from origin, so the gh call and the mirror-lookup hint name the repo being checked
// rather than assuming the fork (AGENTS.md: qualify gh in anything written down).
let repo = null;
try {
  const m = git(["remote", "get-url", "origin"]).trim().match(/[:/]([^/:]+\/[^/:]+?)(?:\.git)?$/);
  if (m) repo = m[1];
} catch { /* no origin - the gate's default stands */ }

// Mirror CI's body handling whenever a body is reachable: honour the opt-out, scan the body.
// `gh pr view` resolves the PR from the current branch; ANY failure (no PR yet, gh missing,
// offline) degrades to the body-less behaviour, and the failure message says which run this was.
let prBody = null, prNumber = null;
try {
  const pr = JSON.parse(execFileSync("gh",
    ["pr", "view", ...(repo ? ["-R", repo] : []), "--json", "number,body"],
    { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }));
  prBody = pr.body || "";
  prNumber = pr.number;
} catch { /* body unreadable this run */ }

if (prBody !== null && gate.findOptOut(prBody)) {
  console.log(`issue-refs: N/A opt-out found in PR ${prNumber}'s body - skipping, exactly as CI will.`);
  process.exit(0);
}

// Working tree vs merge-base, so uncommitted edits are judged too.
const names = git(["diff", "--name-only", base, "--"]).split("\n").filter(Boolean);

const files = names.map((filename) => ({
  filename,
  patch: git(["diff", "--unified=0", base, "--", filename]),
}));
if (prBody !== null) files.push(gate.prBodyEntry(prBody));

let hits = gate.suspectRefs(files);

// File-scoped opt-out (`issue-refs: exempt-file`) is judged against file CONTENT, not the patch,
// so it holds however small the diff. Only files that actually have hits are read.
const fs = require("fs");
for (const path of [...new Set(hits.map((h) => h.file))]) {
  if (path === gate.PR_BODY_LABEL) continue;
  try {
    if (gate.hasFileOptOut(fs.readFileSync(path, "utf8"))) {
      hits = hits.filter((h) => h.file !== path);
    }
  } catch { /* deleted or unreadable - nothing to exempt */ }
}

if (hits.length === 0) {
  const bodyNote = prBody !== null ? ` and PR ${prNumber}'s body` : "";
  console.log(
    `No unqualified references below #${gate.QUALIFY_BELOW} on added lines ` +
    `(${names.length} changed file(s) vs ${base.slice(0, 12)}${bodyNote}).`
  );
  process.exit(0);
}

const opts = repo ? { repo } : {};
if (prBody === null) opts.readsPrBody = false;
console.error(gate.formatFailure(hits, opts));
process.exit(1);
NODE
