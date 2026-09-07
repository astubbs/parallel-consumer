#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for bin/check-pr-ready.sh. Every case must go RED against the broken version; a
# regression test that has never failed proves nothing (bin/AGENTS.md).
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
SCRIPT_UNDER_TEST="$PWD/bin/check-pr-ready.sh"
pass=0; fail=0
assert() { # <desc> <expected> <actual>
    if [ "$2" = "$3" ]; then echo "ok:   $1"; pass=$((pass+1));
    else echo "FAIL: $1 (expected '$2', got '$3')" >&2; fail=$((fail+1)); fi
}

# THE SCRIPT MUST NEVER CONCLUDE READY. Its whole reason for existing is that an agent turned a git
# fact into a verdict; a version that prints "ready" would reproduce the defect it guards.
body="$(cat "$SCRIPT_UNDER_TEST")"
case "$body" in *'is ready'*|*'READY TO MERGE'*) got=claims_ready ;; *) got=never_claims ;; esac
assert "the script never prints a readiness verdict" never_claims "$got"
case "$body" in *"THAT IS NOT READINESS"*) got=says_so ;; *) got=silent ;; esac
assert "a clean run says explicitly that clean is not ready" says_so "$got"
case "$body" in *"NOT a readiness verdict"*) got=labelled ;; *) got=unlabelled ;; esac
assert "git mergeability is labelled as a git fact" labelled "$got"

# EVERY matching note, not the first. `pr-322-*` matches more than one file, and reading only the
# first skipped the note that actually recorded what was open.
# CODE ONLY, not comments. The first version of this case matched the word anywhere in the file and
# so flagged the comment that EXPLAINS the fix - a test asserting on prose rather than behaviour.
code="$(grep -v '^[[:space:]]*#' "$SCRIPT_UNDER_TEST")"
case "$code" in *'-name "pr-${pr}-*.md"'*'head -1'*) got=takes_first ;; *) got=reads_all ;; esac
assert "it does not read only the first matching note" reads_all "$got"
case "$body" in *'while IFS= read -r note'*) got=loops ;; *) got=no_loop ;; esac
assert "it loops over the notes it finds" loops "$got"

# A human LGTM is required; automated review is not approval and neither is green CI.
case "$body" in *'no human approval'*) got=checks_human ;; *) got=missing ;; esac
assert "absence of human approval is a blocker" checks_human "$got"
case "$body" in *'background task'*) got=checks_inflight ;; *) got=missing ;; esac
assert "live background work is a blocker" checks_inflight "$got"

# Usage, with no PR resolvable, must not exit 0 - a silent success would read as "nothing outstanding".
out=$( cd "$(mktemp -d)" && bash "$SCRIPT_UNDER_TEST" 2>&1 ); rc=$?
[ "$rc" -ne 0 ] && got=nonzero || got=zero
assert "an unresolvable PR exits non-zero" nonzero "$got"

# THE LOOKUP, AND WHETHER ITS ANSWER IS AN ANSWER. Everything above this line greps the script's own
# source, which is why the lookup fix shipped with no coverage at all: prose assertions pass
# identically before and after a behavioural change. These cases RUN the script against a stubbed
# `gh` instead, and each one is red against the pre-fix version - `gh pr list --head "$branch"
# 2>/dev/null || true`, which discarded the exit status and the stderr together and named no
# repository, so a failed lookup and a branch with no PR printed the same sentence.
#
# Each case builds its own throwaway repository, because the answer under test depends on the branch
# the script is run from, and puts its own `gh` first on PATH so the answer is chosen rather than
# ambient.
pr_stub="$(mktemp -d)"
pr_argv="$pr_stub/argv"
pr_gh() { cat > "$pr_stub/gh"; chmod +x "$pr_stub/gh"; }
pr_repo() { # -> path to a fresh repo on a named branch
    local d; d="$(mktemp -d)"
    git -C "$d" init -q -b feats/some-branch
    git -C "$d" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init
    printf '%s' "$d"
}
pr_run() { # <repo-dir> [env assignments...] -> combined output of the script
    local d="$1"; shift
    rm -f "$pr_argv"
    ( cd "$d" && env "$@" PATH="$pr_stub:$PATH" bash "$SCRIPT_UNDER_TEST" 2>&1 )
}

# 1. THE LOOKUP FAILED. The pre-fix script printed "no open PR ... has this branch as its head
# branch" here - an assertion of absence built on an answer nobody received.
pr_gh <<'GH'
#!/usr/bin/env bash
printf '%s ' "$@" >> "$PR_ARGV"
echo "gh: To get started with GitHub CLI, please run: gh auth login" >&2
exit 4
GH
repo="$(pr_repo)"
out="$(pr_run "$repo" PR_ARGV="$pr_argv")"
case "$out" in *FAILED*) got=names_the_failure ;; *) got=silent_about_it ;; esac
assert "a failed lookup is reported as a failure" names_the_failure "$got"
case "$out" in *"no open PR in"*) got=claims_absence ;; *) got=claims_nothing ;; esac
assert "a failed lookup never claims the branch has no PR" claims_nothing "$got"
case "$out" in *"auth login"*) got=quotes_gh ;; *) got=discards_the_reason ;; esac
assert "the refusal repeats what gh actually said" quotes_gh "$got"
case "$out" in *"Nothing was measured"*) got=says_so ;; *) got=silent ;; esac
assert "a failed lookup says nothing was measured" says_so "$got"

# 2. A MEASURED ABSENCE. gh exits 0 printing nothing, which is what a real "no PR" looks like for
# `gh pr list --head`, so this must read differently from the failure above.
pr_gh <<'GH'
#!/usr/bin/env bash
printf '%s ' "$@" >> "$PR_ARGV"
exit 0
GH
out="$(pr_run "$repo" PR_ARGV="$pr_argv")"
case "$out" in *"no open PR in"*) got=measured_absence ;; *) got=undifferentiated ;; esac
assert "an empty answer is reported as a measured absence" measured_absence "$got"
case "$out" in *FAILED*) got=claims_failure ;; *) got=claims_nothing ;; esac
assert "an empty answer is not reported as a failure" claims_nothing "$got"

# 3. WHICH REPOSITORY IT ASKED. Unqualified, gh prefers `upstream` in this fork and answers for
# confluentinc/parallel-consumer - the damaging case is the lookup that SUCCEEDS against the wrong
# repository. The pre-fix script passed no `-R` at all, so this is red against it.
case "$(cat "$pr_argv" 2>/dev/null)" in *"-R astubbs/parallel-consumer"*) got=qualified ;; *) got=unqualified ;; esac
assert "the lookup names the repository it asks" qualified "$got"
case "$out" in *"astubbs/parallel-consumer"*) got=names_the_repo ;; *) got=unattributed ;; esac
assert "the absence message names the repository it asked" names_the_repo "$got"

# 4. THE OVERRIDE IS REAL, not just a default written twice. `bin/` gates name the repo with an
# env-overridable constant; a constant nothing can override is a hardcoded string with a comment.
out="$(pr_run "$repo" PR_ARGV="$pr_argv" PR_READY_REPO=someone/elses-fork)"
case "$(cat "$pr_argv" 2>/dev/null)" in *"-R someone/elses-fork"*) got=honoured ;; *) got=ignored ;; esac
assert "PR_READY_REPO overrides the repository actually queried" honoured "$got"
case "$out" in *"someone/elses-fork"*) got=names_it ;; *) got=names_the_default ;; esac
assert "the message names the overridden repository, not the default" names_it "$got"

# 5. DETACHED HEAD has no branch to look a PR up by, and must say so rather than asking gh about a
# branch literally named "HEAD".
rm -f "$pr_argv"
git -C "$repo" -c user.email=t@t -c user.name=t commit -q --allow-empty -m second
git -C "$repo" checkout -q --detach HEAD
out="$( cd "$repo" && env PR_ARGV="$pr_argv" PATH="$pr_stub:$PATH" bash "$SCRIPT_UNDER_TEST" 2>&1 )"
case "$out" in *detached*) got=says_so ;; *) got=silent ;; esac
assert "a detached HEAD is reported as having no branch" says_so "$got"
[ -s "$pr_argv" ] && got=asked_anyway || got=asked_nothing
assert "gh is not asked about a branch named HEAD" asked_nothing "$got"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
(( fail == 0 ))
