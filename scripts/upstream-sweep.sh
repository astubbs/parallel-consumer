#!/usr/bin/env bash
#
# upstream-sweep.sh -- check the archived upstream (confluentinc/parallel-consumer)
# for NEW activity since we last looked, and flag drift on the issues/PRs we track.
#
# Upstream is effectively archived, so our tracked items rarely change state -- the
# real value is catching new issues/PRs/comments from users who don't know a
# maintained fork exists. This is READ-ONLY by default: it prints a report and
# posts nothing. `--publish` updates a SINGLE fork tracking issue (never spams).
#
# Usage:
#   scripts/upstream-sweep.sh                 # report new activity since manifest last_swept
#   scripts/upstream-sweep.sh --since 2026-06-01
#   scripts/upstream-sweep.sh --update-swept  # also print the last_swept bump to apply
#   scripts/upstream-sweep.sh --publish       # create/update ONE fork tracking issue (guarded)
#   scripts/upstream-sweep.sh --audit         # time-independent: find bulk closures + coverage gaps
#
# WHY --audit EXISTS (learned the hard way, 2026-08-07)
#   The default mode is a *window* search: `updated:>=$since`. That silently cannot
#   see an item whose last activity predates the window, no matter how important it
#   is -- and `last_swept` only ever moves forward, so the blind spot grows.
#   Upstream closed 28 issues on 2023-07-07 with the comment "Closing Issue", every
#   one marked COMPLETED rather than "not planned", and closed 35 unmerged PRs three
#   weeks earlier as "Closing - Stale.". None of it was triage. All of it was
#   invisible to this script for three years, and `completed` meant GitHub rendered
#   them as resolved, so no symptom search would surface them either.
#   --audit asks a different question, with no time window: "which upstream items are
#   closed but unaccounted for in our fork?", and flags days where a suspicious number
#   of things closed at once. It would catch the next sweep on the first run after it.
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAP="$HERE/upstream-map.py"
TRACKING_TITLE="Upstream activity sweep (confluentinc/parallel-consumer)"

since=""
update_swept=0
do_publish=0
do_audit=0
bulk_threshold=8
audit_limit=1000
limit=100

while [ $# -gt 0 ]; do
  case "$1" in
    --since)        since="$2"; shift 2;;
    --update-swept) update_swept=1; shift;;
    --publish)      do_publish=1; shift;;
    --audit)        do_audit=1; shift;;
    --bulk-threshold) bulk_threshold="$2"; shift 2;;
    --limit)        limit="$2"; shift 2;;
    -h|--help)      sed -n '2,32p' "${BASH_SOURCE[0]}"; exit 0;;
    *)              echo "unknown arg: $1" >&2; exit 2;;
  esac
done

command -v gh >/dev/null || { echo "error: gh CLI not found" >&2; exit 1; }
gh auth status >/dev/null 2>&1 || { echo "error: gh not authenticated (gh auth login)" >&2; exit 1; }

eval "$(python3 "$MAP" meta)"          # LAST_SWEPT, UPSTREAM_REPO, FORK_REPO
since="${since:-$LAST_SWEPT}"
today="$(date +%F)"

# ---- audit mode: no time window, looks for closures we never accounted for --
#
# Two questions, neither of which the window search can answer:
#   1. Did upstream close a suspicious number of things on one day? (a sweep)
#   2. Which closed upstream items are neither tracked in the manifest nor mirrored
#      in the fork tracker? (coverage gaps)
# Deliberately counts CLOSED items only: open ones are what the window search is for.
if [ "$do_audit" -eq 1 ]; then
  tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT

  echo "# Upstream closure audit"
  echo
  echo "- upstream: \`$UPSTREAM_REPO\`   fork: \`$FORK_REPO\`"
  echo "- run:      $today   (no time window - scans all closed items)"
  echo "- bulk-closure threshold: >= $bulk_threshold items closed on one day"
  echo

  gh issue list --repo "$UPSTREAM_REPO" --state closed --limit "$audit_limit" \
    --json number,title,closedAt,stateReason,author > "$tmp/issues.json"
  # Bots self-close superseded dependency bumps in batches, which look exactly like a
  # sweep and drown the signal - 2022-08-16, 2022-10-20, 2023-11-03 and 2024-01-25 are
  # all dependabot. Human-authored PRs only; a bot batch is not a lost contribution.
  gh pr list --repo "$UPSTREAM_REPO" --state closed --limit "$audit_limit" \
    --json number,title,closedAt,mergedAt,author \
    --jq '[.[] | select(.mergedAt == null)
                | select((.author.login // "") | test("\\[bot\\]$|^app/") | not)]' \
    > "$tmp/prs.json"

  # what we already know about: manifest refs + fork mirrors (titled "confluentinc#N: ...")
  python3 "$MAP" refs | awk '{print $2}' | sort -u > "$tmp/tracked.txt"
  gh issue list --repo "$FORK_REPO" --state all --limit 400 --json title \
    --jq '.[].title | capture("^confluentinc#(?<n>[0-9]+):").n' 2>/dev/null \
    | sort -u > "$tmp/mirrored.txt" || : > "$tmp/mirrored.txt"
  sort -u "$tmp/tracked.txt" "$tmp/mirrored.txt" > "$tmp/known.txt"

  echo "## Bulk closure days (the signature of an administrative sweep)"
  echo
  for src in issues prs; do
    out="$(jq -r --argjson t "$bulk_threshold" '
      group_by(.closedAt[0:10]) | map(select(length >= $t)) | sort_by(.[0].closedAt) | reverse
      | .[] | "\(.[0].closedAt[0:10])\t\(length)\t\([.[].number] | sort | map(tostring) | join(" "))"
    ' "$tmp/$src.json")"
    if [ -z "$out" ]; then
      echo "  $src: none"
    else
      printf '%s\n' "$out" | while IFS=$'\t' read -r date count nums; do
        unknown=0
        for n in $nums; do grep -qx "$n" "$tmp/known.txt" || unknown=$((unknown + 1)); done
        printf '  %s  %s: %s closed  (%s not tracked/mirrored)\n' "$date" "$src" "$count" "$unknown"
        printf '      %s\n' "$nums"
      done
    fi
  done
  echo
  echo "  A bulk day is not proof of anything on its own - a real release triage looks"
  echo "  the same from here. Read the closing comments before believing it was a sweep."
  echo

  echo "## Closed upstream issues that are neither tracked nor mirrored"
  echo
  gap="$(jq -r '.[] | "\(.number)\t\(.closedAt[0:10])\t\(.stateReason // "-")\t\(.title)"' "$tmp/issues.json" \
    | sort -n | while IFS=$'\t' read -r n date reason title; do
        grep -qx "$n" "$tmp/known.txt" || printf '  #%-5s %s  %-9s %s\n' "$n" "$date" "$reason" "${title:0:70}"
      done)"
  if [ -z "$gap" ]; then
    echo "  (none - every closed upstream issue is tracked in the manifest or mirrored in the fork)"
  else
    printf '%s\n' "$gap"
    echo
    echo "  total: $(printf '%s\n' "$gap" | wc -l | tr -d ' ')"
    echo "  NOTE: stateReason COMPLETED is not evidence of a fix. Upstream marked all 28"
    echo "  of the 2023-07-07 sweep COMPLETED, which is why they read as resolved."
  fi
  exit 0
fi

# ---- gather + format via gh --jq (no inline python) ------------------------
#
# OUR OWN COMMENTS DO NOT COUNT AS UPSTREAM ACTIVITY. Every mirrored issue carries a backlink comment
# from us, and posting it bumped that issue's `updated` timestamp - so a bare `updated:>=` search
# returns the entire tracker and the signal is gone. An item is only reported when someone who is not
# us has touched it: either the item was opened inside the window, or a non-fork account commented in
# it inside the window.
FORK_VOICES="$(gh api user --jq .login 2>/dev/null || echo astubbs)"

foreign_activity() {  # $1 = issue/PR number; 0 = someone other than us moved it
  local n="$1" created foreign
  created="$(gh api "repos/$UPSTREAM_REPO/issues/$n" --jq '.created_at[0:10]' 2>/dev/null || true)"
  # opened inside the window: real signal regardless of who has commented since
  [ -n "$created" ] && [[ ! "$created" < "$since" ]] && return 0
  foreign="$(gh api "repos/$UPSTREAM_REPO/issues/$n/comments?per_page=100" 2>/dev/null \
    | jq --arg since "$since" --arg me "$FORK_VOICES" \
        '[.[] | select(.created_at[0:10] >= $since and .user.login != $me)] | length' 2>/dev/null || echo 0)"
  [ "${foreign:-0}" -gt 0 ]
}

list_fmt() {  # $1 = issue|pr
  gh "$1" list --repo "$UPSTREAM_REPO" --state all --limit "$limit" \
    --search "updated:>=$since sort:updated-desc" \
    --json number,title,state,updatedAt \
    --jq '.[] | "\(.number)\t  #\(.number)  \(.state|ascii_upcase[0:6])  \(.updatedAt[0:10])  \(.title)"' \
    2>/dev/null | while IFS=$'\t' read -r num line; do
      foreign_activity "$num" && printf '%s\n' "$line"
    done || true
}
issues_out="$(list_fmt issue)"; [ -n "$issues_out" ] || issues_out="  (none - nothing but our own comments)"
prs_out="$(list_fmt pr)";       [ -n "$prs_out" ]    || prs_out="  (none - nothing but our own comments)"

# ---- drift on tracked refs (recorded 'open' but now closed/merged) ---------
drift="$(python3 "$MAP" tracked | while read -r kind num recorded eid; do
  cur="$(gh api "repos/$UPSTREAM_REPO/issues/$num" --jq '.state' 2>/dev/null || echo '?')"
  if [ "$recorded" = "open" ] && [ "$cur" != "open" ] && [ "$cur" != "?" ]; then
    echo "  #$num ($eid): recorded 'open' but upstream is now '$cur'"
  fi
done)"
[ -n "$drift" ] || drift="  (none)"

# ---- report ----------------------------------------------------------------
report="$(cat <<EOF
# Upstream activity sweep

- upstream: \`$UPSTREAM_REPO\` (archived)
- window:   activity updated since **$since** (manifest last_swept: $LAST_SWEPT)
- run:      $today

## New / updated issues
$issues_out

## New / updated PRs
$prs_out

## Drift on tracked items (recorded 'open' but now closed/merged upstream)
$drift
EOF
)"
echo "$report"

if [ "$update_swept" -eq 1 ]; then
  echo
  echo "To advance the sweep window, set this in src/docs/development/upstream-map.yaml:"
  echo "    last_swept: $today"
fi

# ---- optional: publish to a SINGLE fork tracking issue (guarded, no spam) ---
if [ "$do_publish" -eq 1 ]; then
  echo
  existing="$(gh issue list --repo "$FORK_REPO" --state open --search "\"$TRACKING_TITLE\" in:title" \
    --json number --jq '.[0].number' 2>/dev/null || true)"
  if [ -n "$existing" ]; then
    printf 'Update fork tracking issue #%s with this report? [y/N] ' "$existing"; read -r a
    [ "$a" = "y" ] && gh issue comment "$existing" --repo "$FORK_REPO" --body "$report" && echo "commented on #$existing"
  else
    printf 'No tracking issue found. Create one on %s? [y/N] ' "$FORK_REPO"; read -r a
    [ "$a" = "y" ] && gh issue create --repo "$FORK_REPO" --title "$TRACKING_TITLE" --body "$report"
  fi
fi
