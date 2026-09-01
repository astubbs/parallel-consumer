#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Assert the repository owner has left an LGTM review on this PR.
#
# WHAT COUNTS: a submitted review by the owner whose body contains "lgtm", in any case, anywhere.
# That is the whole rule. Reviews come from the reviews endpoint, so "a review, not a comment" is
# free rather than something to enforce.
#
# WHAT IT IS FOR: a memory aid, not a security control. The owner is both the person it asserts
# about and the person who wants the merge, so it stops nobody who wants to be stopped. What it buys
# is that "have I read this one myself yet?" stops being a thing to carry across a dozen open PRs.
# Treating it as a security control is what produced the 593-line version this replaces, which
# parsed code fences, blockquotes, negations and typographic apostrophes to defend against an
# attacker who is also the only person it protects.
#
# NOT HEAD-SENSITIVE. An LGTM on any commit counts for the whole PR, permanently - the owner only
# stamps a PR once it is near merge, so later commits must not invalidate it. This is why review
# STATE is not consulted: the ruleset sets dismiss_stale_reviews_on_push, so an approval this repo
# dismissed on push would otherwise silently un-stamp the PR.
#
# Reads the reviews JSON on stdin (so it is testable from fixtures); the caller does the API call.
# `gh api --paginate` emits one array per page, so the input is slurped and the pages concatenated.
# Exit: 0 = an owner LGTM exists, 1 = it does not, 2 = could not read the input.

set -euo pipefail

OWNER_LOGIN="${OWNER_LOGIN:-astubbs}"

reviews=$(cat)
[ -n "$reviews" ] || { echo "ERROR: no review JSON on stdin - the API call failed." >&2; exit 2; }

bodies=$(jq -rs --arg owner "$OWNER_LOGIN" \
    '(add // []) | [.[] | select((.user.login // "") == $owner) | .body // ""] | join("\n")' \
    <<<"$reviews") || { echo "ERROR: could not parse the review JSON." >&2; exit 2; }

if grep -qi 'lgtm' <<<"$bodies"; then
    echo "OK: $OWNER_LOGIN has left an LGTM review on this PR."
    exit 0
fi

cat >&2 <<EOF
NO HUMAN LGTM ON THIS PR.

$OWNER_LOGIN has not left a review whose body contains "lgtm". Review the PR and say so - any case,
anywhere in the body, on any commit. It stays valid however many commits land afterwards.

This half of the gate is a memory aid: it exists so an unreviewed PR is visible rather than
remembered. The other half (bin/check-review-posted.sh) asserts the automated review separately, so
a red check names which one is missing.
EOF
exit 1
