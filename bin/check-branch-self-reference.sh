#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Fails when a note in docs/inflight/ mentions THIS branch or THIS PR, unless the mention has been
# written in post-merge terms and says so.
#
# WHY IT EXISTS, and why only here. An inflight note is a claim about NOW - the directory's contract
# is transient cross-branch state, and a note is deleted when its work lands. So a sentence in one
# describing your own open PR is a claim your merge falsifies: accurate when written, accurate at
# review, wrong seconds after the merge button - by which time the person who could cheaply fix it has
# moved on, and the fix is too small to justify its own PR, so it rots.
#
# It happened in exactly this directory, and then it happened AGAIN while this gate was being written.
# `branch-package-rename-sweep.md` is a live 38-branch worklist. It told readers astubbs#1 was excluded
# from the sweep "until its branch was reset onto renamed master and repurposed as documentation" -
# written while astubbs#1 was open. astubbs#1 merged and its branch was deleted, leaving a live
# worklist asserting an arrangement that had ended.
#
# The recurrence is the better evidence. That sentence was repaired - and then astubbs#1's own merge
# commit put an equivalent one back, in the present tense, in the same paragraph of the same file, on
# the day this gate was in review. Grep it: `grep -n "reset onto renamed master"
# docs/inflight/branch-package-rename-sweep.md` resolves TODAY, and the claim is once again about a
# branch that no longer exists. A rule stated in prose did not survive one merge cycle in the file
# that motivated it, which is the whole argument for making it a gate.
#
# This gate would have caught it, on the branch where it was cheap: the sentence names astubbs#1, and
# it was added by astubbs#1. It deliberately does NOT fire now - it only ever looks at YOUR branch and
# YOUR PR, because only yours is about to change state. Somebody else's stale tense is not your merge.
#
# **The PR did not stop existing - PRs are permanent, and citing one is fine forever.** What rotted is
# the BRANCH (deleted) and the TENSE (a present-state claim about an arrangement that ended). That is
# also why this looks nowhere else: in docs/solutions/, dated docs/plans/ or CHANGELOG.adoc, a branch
# or PR reference records what already happened and stays correct forever. Demanding a marker there
# would be a tax on writing history correctly.
#
# **After the merge is too late, so this fires before it.** The rule it enforces is not "do not
# mention your own PR" - cross-referencing is often exactly right - but "write the mention as it will
# read AFTER this lands." A sweep table should say a branch is excluded because it carries no Java,
# not because a PR that will be closed by then is currently repurposing it.
#
# HOW TO SATISFY IT. Rewrite the sentence in post-merge terms, then mark the line so this gate knows
# a human made that judgement:
#
#     <!-- post-merge: checked -->        markdown / HTML, on or above the line
#     # post-merge: checked               shell, yaml
#     // post-merge: checked              java, js
#
# A rewritten sentence usually spans several lines, so a paragraph takes the block form - the same
# shape `check-issue-refs.sh` already uses, so there is one convention to remember rather than two:
#
#     <!-- post-merge: checked-begin -->
#     ...the rewritten paragraph...
#     <!-- post-merge: checked-end -->
#
# A whole file that legitimately discusses in-flight state - a PR note in docs/inflight/ named for
# this very PR - takes `post-merge: exempt-file` anywhere in it. The marker travels with the file, so
# it keeps holding on later branches and local runs.
#
# Two verbs, deliberately, and they are not interchangeable: `checked` is an ATTESTATION (a human read
# this line and judged it post-merge-correct), `exempt-file` is a SCOPE OPT-OUT (do not look here at
# all). `check-issue-refs.sh` spells all four of its markers `exempt`, so the habit transfers wrongly;
# a line marked `post-merge: exempt` would otherwise match nothing and be silently ignored, which is
# the failure this gate exists to prevent. It is therefore a hard error - see UNKNOWN MARKER below.
#
# WHAT IT DOES NOT DO. It cannot tell whether your rewritten sentence is actually true after the
# merge; no grep can. It forces a human to look at every self-reference at the one moment fixing it is
# cheap, and records that they did. It also does not care about OTHER branches or PRs - only your own,
# because only your own is about to change state.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# CI checks out a detached HEAD, so the branch name must come from the event, not from Git.
branch="${GITHUB_HEAD_REF:-$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '')}"
if [ -z "$branch" ] || [ "$branch" = "HEAD" ] || [ "$branch" = "master" ]; then
    echo "check-branch-self-reference: no feature branch to check (branch='${branch:-none}')"
    exit 0
fi

# The PR number is optional: this runs locally before a PR exists, where the branch name alone is
# still worth checking. Never `gh pr view` without -R here; a bare gh resolves to confluentinc.
pr="${PR_NUMBER:-}"
if [ -z "$pr" ] && command -v gh >/dev/null 2>&1; then
    pr="$(gh pr list -R astubbs/parallel-consumer --head "$branch" --json number --jq '.[0].number' 2>/dev/null || true)"
fi

# Escaped for ERE use below. A branch name may legally contain `.` and `+`.
branch_re="$(sed 's/[][\\.^$*+?(){}|]/\\&/g' <<<"$branch")"

problems=0
note() { printf 'SELF-REF: %s\n' "$1" >&2; problems=$((problems + 1)); }

# MENTION IS NOT USE. Every marker test runs on this backtick-stripped view: a marker quoted in a code
# span is documentation, not an instruction. Without it, a note explaining the convention - the
# directory's own rules doc, a solution write-up, this gate's own PR description pasted into a note -
# silences itself just by naming the marker. `.github/scripts/issue-ref-gate.js` records that exact
# incident for its own markers: "this PR's own docs made five files - including the convention's
# defining doc - permanently self-exempt, and a doc line quoting the block markers opened a real,
# unclosed block." This gate copied that convention's SPELLING without its guard, and reproduced the
# bug. An UNQUOTED prose mention still matches, because nothing can tell it from use - write marker
# names in backticks when writing about them, as this comment does.
#
# Only MARKERS use the stripped view. A branch name or PR number inside backticks is still a real
# mention, and the search arms below read the original file.
strip_spans() { sed 's/`[^`]*`/ /g' "$1"; }

# docs/inflight/ ONLY, and the scope is the whole point. That directory's contract is "currently
# true" - AGENTS.md calls it transient cross-branch state, and a note is deleted when its work lands.
# Everywhere else (docs/solutions/, dated docs/plans/, CHANGELOG.adoc) records what ALREADY HAPPENED,
# where a branch reference stays correct forever and demanding a marker would be pure tax.
# The glob DOES recurse - a git pathspec `*` matches `/` too, so `docs/inflight/sub/note.md` is
# covered. (An earlier comment here claimed the opposite and called the flatness load-bearing; it was
# wrong in the safe direction, but wrong.) The directory is flat anyway - docs/inflight/AGENTS.md
# mandates one file per item and bin/check-inflight-tags.sh fails on any subdirectory - so the
# recursion is belt-and-braces rather than something the scope depends on.
#
# UNTRACKED FILES COUNT. `git ls-files` alone reads the index, so a note you just wrote about your own
# branch - the single commonest way to trip this gate - was invisible until you staged it, and the
# local run this header advertises reported green on the exact case it exists for.
# `mapfile` is bash 4; macOS ships bash 3.2, where this line died with "mapfile: command not found"
# and the script still exited 0 - a local run that reported success having checked nothing, which is
# the exact failure this gate exists to prevent, one level up. Read the lines portably instead.
candidates=()
while IFS= read -r _line; do
    [ -n "$_line" ] && candidates+=("$_line")
done < <(
    {
        git ls-files -- 'docs/inflight/*.md' || true
        git ls-files --others --exclude-standard -- 'docs/inflight/*.md' || true
    } | sort -u
)
[ "${#candidates[@]}" -gt 0 ] || { echo "check-branch-self-reference: no documents to check"; exit 0; }

for f in "${candidates[@]}"; do
    [ -f "$f" ] || continue
    # AGENTS.md and CLAUDE.md are this directory's RULES, not notes - the same exclusion
    # bin/check-inflight-tags.sh already makes. They cite PR numbers permanently and correctly (the
    # rules doc for the tag gate names astubbs#324 as settled history), so without this the gate
    # fires on the document that explains it.
    case "$(basename "$f")" in AGENTS.md|CLAUDE.md) continue ;; esac

    # Line-for-line with the original, so line numbers below still address the real file.
    markers="$(strip_spans "$f")"
    grep -q 'post-merge: exempt-file' <<<"$markers" && continue

    # UNKNOWN MARKER. `exempt` is the sibling gate's word for all four of its markers, so it arrives
    # here by muscle memory - and an unrecognised marker is not a no-op, it is a line the author
    # believes is handled. Fail loudly rather than ignore it. `-[^f]` catches `exempt-begin`/`-end`
    # while leaving the one real spelling, `exempt-file`, already handled above.
    if grep -qE 'post-merge: exempt([^-]|$|-[^f])' <<<"$markers"; then
        note "$f uses an unrecognised 'post-merge: exempt...' marker. This gate has exactly two: 'post-merge: checked' on or above a line (or checked-begin/checked-end around a paragraph), and 'post-merge: exempt-file' for a whole file."
    fi

    # Line ranges covered by a checked-begin/end block.
    blocks=""
    if grep -q 'post-merge: checked-begin' <<<"$markers"; then
        blocks="$(awk '/post-merge: checked-begin/{s=NR} /post-merge: checked-end/{if(s){print s":"NR; s=0}}' <<<"$markers")"
    fi

    while IFS=: read -r lineno text; do
        [ -n "$lineno" ] || continue
        # A marker on the line itself, or on the line above it, is the human's acknowledgement.
        prev=$((lineno - 1))
        # Herestrings, not `printf | grep -q`: the reader exits on first match, the writer takes
        # EPIPE, and pipefail makes that the pipeline status - so the check would FAIL when it FOUND
        # the marker. bin/AGENTS.md, "Scripts that guard other scripts".
        # `checked([^-]|$)` not a bare `checked`: `checked-end` CONTAINS `checked`, so a loose match
        # let a block's END marker clear the line beneath it - silencing exactly the mentions that
        # fall outside the block. Caught by the near-miss self-test, not by any of the red controls.
        if grep -qE 'post-merge: checked([^-]|$)' <<<"$(sed -n "${lineno}p" <<<"$markers")"; then continue; fi
        if [ "$prev" -ge 1 ] && grep -qE 'post-merge: checked([^-]|$)' <<<"$(sed -n "${prev}p" <<<"$markers")"; then continue; fi
        covered=""
        for range in $blocks; do
            start="${range%%:*}"; end="${range##*:}"
            if [ "$lineno" -ge "$start" ] && [ "$lineno" -le "$end" ]; then covered=1; break; fi
        done
        [ -n "$covered" ] && continue
        note "$f:$lineno mentions this branch or PR - rewrite it as it will read AFTER this merges, then add 'post-merge: checked'. Line: ${text:0:100}"
    done < <(
        {
            # BOUNDED, not a substring. `grep -F "$branch"` matched any line CONTAINING the name, so
            # on `fix/909` a note mentioning `builds/fix/9090/output.log` failed the gate, and on
            # `feat/857-fix` so did `feat/857-fix-followup`. This repo's branch convention is
            # `bugs/857-...`/`fix/909-...`, so short names nest inside longer ones by design and the
            # false positive blocks an unrelated PR. The PR-number arm below was given boundaries for
            # exactly this reason; the branch arm was not.
            #
            # The trailing class excludes `/` and `-` (a longer branch, or a deeper path, is a
            # DIFFERENT branch); the leading one does not, so a mention as a URL or a path
            # (`.../tree/feat/my-branch`) still counts. Metacharacters in the name are escaped rather
            # than relying on -F, which cannot carry boundaries.
            grep -nE "(^|[^0-9A-Za-z_])${branch_re}([^0-9A-Za-z_/-]|$)" "$f" || true
            # Bare `#NNN` counts too. Requiring the `astubbs` prefix left a silent hole, masked only
            # by accident: `.github/scripts/issue-ref-gate.js` sets QUALIFY_BELOW=1000, so today's
            # three-digit PR numbers must be qualified anyway and a bare one never survives to reach
            # here. Nothing ties the two gates together though - once fork PR numbers pass 1000, or
            # that constant drops, a bare self-reference becomes legal AND undetected. Matching it
            # here removes the coupling rather than documenting it.
            # The URL form counts as much as the `#NNN` one - it is what `gh` prints and what a
            # paste carries, and it is already live in this very directory (deps-cve-backlog.md
            # cites PRs as .../pull/NNN). The `#`-anchored regex above cannot see it.
            [ -n "$pr" ] && {
                grep -nE "(^|[^0-9A-Za-z_/])(astubbs(/parallel-consumer)?)?#${pr}([^0-9]|$)" "$f" || true
                grep -nE "/pull/${pr}([^0-9]|$)" "$f" || true
            }
        } | sort -t: -k1,1n -u
    )
done

if [ "$problems" -gt 0 ]; then
    echo "check-branch-self-reference: $problems self-reference(s) not confirmed post-merge-correct." >&2
    echo "After the merge is too late - this branch and its PR stop existing when it lands." >&2
    exit 1
fi
if [ -n "$pr" ]; then
    echo "check-branch-self-reference: no unconfirmed self-references (branch '$branch', PR #$pr)"
else
    # NOT the same statement as a clean full run, and it must not read like one. Before the PR exists
    # this is normal; in CI it means PR_NUMBER was not passed and `gh` could not answer, so the whole
    # PR-number arm - the gate's own motivating incident, a note citing astubbs#1 by NUMBER with no
    # branch name in it - was never searched. Three of the four red self-test cases cover that arm.
    echo "check-branch-self-reference: branch name only - NO PR NUMBER RESOLVED, so #NNN and /pull/NNN mentions were NOT checked (branch '$branch')"
fi
