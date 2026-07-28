#!/usr/bin/env bash
#
# upstream-backlink.sh -- post a standard "fixed in the maintained fork" / "this
# is maintained in a fork" comment to an upstream issue/PR, driven by an entry in
# src/docs/development/upstream-map.yaml.
#
# SAFE BY DEFAULT: dry-run unless you pass --post. Dry-run prints the exact
# target(s) and rendered comment body and posts NOTHING.
#
# ANTI-SPAM (never nag upstream):
#   - Idempotent: skips any target already recorded in the manifest's forwarded
#     urls (so re-running never double-posts).
#   - Per-run cap (--max, default 3) limits how many comments one run can post.
#   - Delay between posts (--delay, default 3s).
#   - fix-backlink only fires for a fix that is in a PR or landed
#     (pr-open|merged|released); refuses none|in-progress|ready (use --force or
#     the fork-awareness template) so we don't announce a fix with no PR to link.
#   - Confirmation prompt before any posting.
# See src/docs/development/upstream-backlink-plan.md.
#
# Usage:
#   scripts/upstream-backlink.sh <entry-id>                          # dry-run
#   scripts/upstream-backlink.sh --template fork-awareness <entry-id>
#   scripts/upstream-backlink.sh --target issues|prs|both <entry-id>
#   scripts/upstream-backlink.sh --post [--max N] [--delay S] [--force] <entry-id>
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAP="$HERE/upstream-map.py"
TPL_DIR="$HERE/backlink-templates"

template="fix-backlink"
target="both"          # issues | prs | both
do_post=0
force=0
max=3
delay=3
entry_id=""

while [ $# -gt 0 ]; do
  case "$1" in
    --template) template="$2"; shift 2;;
    --target)   target="$2"; shift 2;;
    --post)     do_post=1; shift;;
    --force)    force=1; shift;;
    --max)      max="$2"; shift 2;;
    --delay)    delay="$2"; shift 2;;
    -h|--help)  sed -n '2,29p' "${BASH_SOURCE[0]}"; exit 0;;
    -*)         echo "unknown flag: $1" >&2; exit 2;;
    *)          entry_id="$1"; shift;;
  esac
done

[ -n "$entry_id" ] || { echo "error: entry-id required (see --help)" >&2; exit 2; }
tpl_file="$TPL_DIR/$template.md"
[ -f "$tpl_file" ] || { echo "error: no template '$template' at $tpl_file" >&2; exit 2; }

# Pull entry fields (shell-quoted KEY=value lines): sets ID SUMMARY GROUP
# FORK_REPO FORK_STATUS FORK_PRS FORK_BRANCHES UPSTREAM_REPO ISSUES PRS RELATED FORWARDED
eval "$(python3 "$MAP" show "$entry_id")"

# Anti-spam guard: only announce a fix that exists in a PR or is landed. A
# fix-backlink points people at "the fix", so it needs a PR/release to link -
# pr-open | merged | released. Refuse none | in-progress | ready (a "ready" branch
# has no fork PR yet; see upstream-backlink-plan.md, which says wait until PR'd).
case "$FORK_STATUS" in
  pr-open|merged|released) ;;
  *)
    if [ "$template" = "fix-backlink" ] && [ "$force" -eq 0 ]; then
      echo "refusing: '$ID' fork status is '$FORK_STATUS' - fix-backlink needs pr-open|merged|released." >&2
      echo "  use --force to override, or --template fork-awareness (no fix claimed)." >&2
      exit 3
    fi
    ;;
esac

# Already-commented upstream numbers (idempotency), across the whole manifest.
posted_set=" $(python3 "$MAP" posted-refs | tr '\n' ' ') "
is_posted() { case "$posted_set" in *" $1 "*) return 0;; *) return 1;; esac; }

# Build the fork reference string (prefer a PR, else branches).
if [ -n "${FORK_PRS// }" ]; then
  FORK_REF="PR ${FORK_REPO}#${FORK_PRS%% *}"
elif [ -n "${FORK_BRANCHES// }" ]; then
  FORK_REF="branch \`${FORK_BRANCHES%% *}\` on ${FORK_REPO}"
else
  FORK_REF="the ${FORK_REPO} fork"
fi

# Source of the comment body: a per-entry `backlink` in the manifest (the source
# of truth) wins, so a tailored public explanation lives in ONE place and is
# rendered from there. Otherwise fall back to the generic template file. Both go
# through the same placeholder substitution.
if [ -n "${BACKLINK//[[:space:]]/}" ]; then
  body_src="manifest backlink field"
  raw="$BACKLINK"
else
  body_src="template: $template"
  raw="$(cat "$tpl_file")"
fi
# Escape sed replacement metacharacters (delimiter |, & and \) in interpolated
# manifest values so a summary/backlink containing them can't break or mangle the
# substitution.
esc() { printf '%s' "$1" | sed 's/[&|\]/\\&/g'; }
body="$(printf '%s' "$raw" | sed \
  -e "s|{{FORK_REPO}}|$(esc "$FORK_REPO")|g" \
  -e "s|{{FORK_REF}}|$(esc "$FORK_REF")|g" \
  -e "s|{{SUMMARY}}|$(esc "$SUMMARY")|g" \
  -e "s|{{ID}}|$(esc "$ID")|g")"

# Collect targets.
targets=()
case "$target" in
  issues) for n in $ISSUES; do targets+=("issue:$n"); done;;
  prs)    for n in $PRS;    do targets+=("pr:$n");    done;;
  both)   for n in $ISSUES; do targets+=("issue:$n"); done
          for n in $PRS;    do targets+=("pr:$n");    done;;
  *) echo "error: --target must be issues|prs|both" >&2; exit 2;;
esac
[ ${#targets[@]} -gt 0 ] || { echo "error: entry '$entry_id' has no upstream $target to comment on" >&2; exit 2; }

echo "entry:     $ID  ($GROUP, fork status: $FORK_STATUS)"
echo "body from: $body_src"
echo "fork ref:  $FORK_REF"
echo "targets    ($UPSTREAM_REPO):"
todo=()
for t in "${targets[@]}"; do
  num="${t#*:}"
  if is_posted "$num"; then
    echo "  - $t   [SKIP: already commented, in manifest forwarded]"
  else
    echo "  - $t   [would post]"
    todo+=("$t")
  fi
done
echo "-------------------- comment body --------------------"
echo "$body"
echo "------------------------------------------------------"

if [ ${#todo[@]} -eq 0 ]; then
  echo "Nothing to do: all targets already commented (idempotent)."
  exit 0
fi
if [ ${#todo[@]} -gt "$max" ]; then
  echo "note: ${#todo[@]} targets pending but --max=$max; only the first $max would post this run."
fi

if [ "$do_post" -eq 0 ]; then
  echo "DRY-RUN: nothing posted. Re-run with --post to comment (requires 'gh auth login')."
  exit 0
fi

# --- real post path (guarded) ---------------------------------------------
command -v gh >/dev/null || { echo "error: gh CLI not found" >&2; exit 1; }
gh auth status >/dev/null 2>&1 || { echo "error: not authenticated (gh auth login)" >&2; exit 1; }
printf 'POST to up to %s of: %s ? [y/N] ' "$max" "${todo[*]}"
read -r ans; [ "$ans" = "y" ] || { echo "aborted."; exit 0; }

today="$(date +%F)"
posted_count=0
for t in "${todo[@]}"; do
  if [ "$posted_count" -ge "$max" ]; then
    echo "reached --max=$max; stopping (re-run to continue)."
    break
  fi
  num="${t#*:}"
  [ "$posted_count" -gt 0 ] && sleep "$delay"
  url="$(gh issue comment "$num" --repo "$UPSTREAM_REPO" --body "$body" 2>/dev/null || \
         gh pr    comment "$num" --repo "$UPSTREAM_REPO" --body "$body")"
  posted_count=$((posted_count + 1))
  echo "posted -> $url"
  echo "  RECORD in src/docs/development/upstream-map.yaml under '$ID' -> forwarded:"
  echo "        - url: $url"
  echo "          posted: $today"
done
echo
echo "Posted $posted_count comment(s). Manifest write-back is manual (preserves the"
echo "file's comments); paste the snippet(s) above into the entry's 'forwarded:' list"
echo "so future runs skip these targets."
