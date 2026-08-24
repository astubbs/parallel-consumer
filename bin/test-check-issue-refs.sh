#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-issue-refs.sh - specifically its PR-body handling, which the gate module's
# own unit tests (issue-ref-gate.test.js) cannot reach: they test the rule, this tests the plumbing
# that feeds the rule a body fetched via `gh`.
#
# WHY THE BODY PLUMBING NEEDS A TEST AT ALL. The `issue-refs: N/A` opt-out lives in the PR body, and
# for as long as the local script never read bodies, an opt-out CI accepted left every local run red
# on the same file forever - a "local mirror of CI" that permanently disagreed with CI. The script
# now fetches the body through `gh pr view` and honours the opt-out exactly as the workflow does;
# these cases pin that, plus the graceful fall-back when there is no PR to read.
#
# `gh` is stubbed on PATH: the stub prints $GH_STUB_JSON when set and fails like a PR-less branch
# when unset. No network, no real gh, deterministic either way.

set -uo pipefail

REPO_ROOT="$(cd "${BASH_SOURCE[0]%/*}/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# --- fixture repo, with the script under test in its real layout ---------------------------------
mkdir -p "$TMP/repo/bin/lib" "$TMP/repo/.github/scripts" "$TMP/stub"
cp "$REPO_ROOT/bin/check-issue-refs.sh" "$TMP/repo/bin/"
cp "$REPO_ROOT/bin/lib/node-gate.sh" "$TMP/repo/bin/lib/"
cp "$REPO_ROOT/.github/scripts/issue-ref-gate.js" "$TMP/repo/.github/scripts/"

cat > "$TMP/stub/gh" <<'STUB'
#!/usr/bin/env bash
# Test stub: behave like `gh pr view` on a branch with no PR unless the case supplies a PR JSON.
[ -n "${GH_STUB_JSON:-}" ] || exit 1
printf '%s' "$GH_STUB_JSON"
STUB
chmod +x "$TMP/stub/gh"
export PATH="$TMP/stub:$PATH"

cd "$TMP/repo"
git init -q -b master
git config user.email test@example.invalid
git config user.name test
git add -A
git commit -qm base
git switch -qc feature
mkdir -p docs

fails=0
check() { # <name> <expected-exit> <must-contain> [must-not-contain]
    local name=$1 want=$2 must=$3 mustnot=${4:-} out got
    out="$(bash bin/check-issue-refs.sh master 2>&1)"
    got=$?
    if [ "$got" != "$want" ]; then
        echo "FAIL: $name (expected exit $want, got $got)"
        echo "$out" | sed 's/^/      /'
        fails=$((fails + 1))
        return
    fi
    case "$out" in
        *"$must"*) ;;
        *) echo "FAIL: $name (output lacks: $must)"; echo "$out" | sed 's/^/      /'; fails=$((fails + 1)); return ;;
    esac
    if [ -n "$mustnot" ]; then
        case "$out" in
            *"$mustnot"*) echo "FAIL: $name (output must not contain: $mustnot)"; fails=$((fails + 1)); return ;;
        esac
    fi
    echo "ok:   $name"
}

# --- no PR to read (gh fails) ---------------------------------------------------------------------
unset GH_STUB_JSON || true
echo "See #857 for details" > docs/note.md # issue-refs: exempt - fixture
git add docs/note.md
check "no PR: a bare ref still fails, and the message says the body was unreadable" \
      1 "could not read the PR body"

echo "See astubbs#857 for details" > docs/note.md
git add docs/note.md
check "no PR: a qualified ref passes, without claiming a body was checked" \
      0 "changed file(s)" "body"

echo 'He wrote "see #857" <!-- issue-refs: exempt -->' > docs/note.md
git add docs/note.md
check "no PR: a line carrying issue-refs: exempt passes end-to-end" \
      0 "changed file(s)"

printf '%s\n%s\n' '<!-- issue-refs: exempt-file - fixture -->' 'A mess of quotes: #857 #858 #859' > docs/note.md
git add docs/note.md
check "no PR: issue-refs: exempt-file exempts the whole file, judged from disk content" \
      0 "changed file(s)"

# --- PR exists, body carries the opt-out ----------------------------------------------------------
export GH_STUB_JSON='{"number":42,"body":"Explanation.\nissue-refs: N/A - refs occur inside quoted source material\n"}'
echo "See #857 for details" > docs/note.md # issue-refs: exempt - fixture
git add docs/note.md
check "opt-out in the PR body is honoured locally, exactly as CI honours it" \
      0 "opt-out"

# --- PR exists, body itself carries a bare ref ----------------------------------------------------
export GH_STUB_JSON='{"number":42,"body":"This fixes #858 for good."}' # issue-refs: exempt - fixture
echo "See astubbs#857 for details" > docs/note.md
git add docs/note.md
check "a bare ref in the PR BODY is flagged locally, pre-empting the CI round-trip" \
      1 "<PR body>"

# --- PR exists, everything clean ------------------------------------------------------------------
export GH_STUB_JSON='{"number":42,"body":"All refs qualified, honest."}'
check "clean diff and clean body pass, and the success line says the body was checked" \
      0 "PR 42's body"

# --- the guidance survives truncation to the tail -------------------------------------------------
# The trailer formatFailure emits is four lines, led by "Fix: qualify each ref" - so the tail width
# must cover all four, and the assertion must include the LEAD line: tail -3 passed for months
# while provably cutting the very sentence the trailer exists to deliver.
export GH_STUB_JSON='{"number":42,"body":"This fixes #858 for good."}' # issue-refs: exempt - fixture
out="$(bash bin/check-issue-refs.sh master 2>&1 | tail -4)"
tail_ok=1
case "$out" in *"Fix: qualify each ref"*) ;; *) tail_ok=0 ;; esac
case "$out" in *"issue-refs: N/A"*) ;; *) tail_ok=0 ;; esac
if [ "$tail_ok" = 1 ]; then
    echo "ok:   the last four lines of a failure carry the whole fix/opt-out reminder"
else
    echo "FAIL: the fix/opt-out reminder did not survive | tail -4 intact"
    echo "$out" | sed 's/^/      /'
    fails=$((fails + 1))
fi

# --- node is PRESENT but cannot RUN ---------------------------------------------------------------
# The guard was `command -v node`, which proves node is installed and nothing more. A stale
# `--require` in NODE_OPTIONS - an agent harness writes a preload into a temp directory, the temp
# directory is later cleaned up - kills node during preload, before one line of the gate executes,
# and node's status for that is 1: this script's code for "unqualified refs found". A gate that had
# checked nothing therefore reported a policy violation, and it cost a real debugging detour.
#
# BOTH arms must be 2, "cannot run". The clean tree because the accusation is pure fiction; the
# dirty one because a gate that did not run may not be believed even when it would have been right.
broken_preload="$TMP/nodepreflight-deleted-by-the-harness.cjs"   # deliberately never created

check_cannot_run() { # <name>
    local name=$1 out got
    out="$(NODE_OPTIONS="--require=$broken_preload" bash bin/check-issue-refs.sh master 2>&1)"
    got=$?
    if [ "$got" != 2 ]; then
        echo "FAIL: $name (expected exit 2, got $got)"
        echo "$out" | sed 's/^/      /'
        fails=$((fails + 1))
        return
    fi
    case "$out" in
        *"This is NOT a finding"*) ;;
        *) echo "FAIL: $name (output does not say it is not a finding)"
           echo "$out" | sed 's/^/      /'; fails=$((fails + 1)); return ;;
    esac
    case "$out" in
        *"NODE_OPTIONS is set"*) ;;
        *) echo "FAIL: $name (output does not name NODE_OPTIONS, the likely cause)"
           echo "$out" | sed 's/^/      /'; fails=$((fails + 1)); return ;;
    esac
    echo "ok:   $name"
}

export GH_STUB_JSON='{"number":42,"body":"All refs qualified, honest."}'
echo "See astubbs#857 for details" > docs/note.md
git add docs/note.md
check_cannot_run "clean tree: a node that cannot start is 'cannot run', never a violation"

echo "See #857 for details" > docs/note.md # issue-refs: exempt - fixture
git add docs/note.md
check_cannot_run "dirty tree: a gate that could not run is still 'cannot run', not a finding"

if [ "$fails" -gt 0 ]; then
    echo "$fails case(s) failed"
    exit 1
fi
echo "all cases passed"
