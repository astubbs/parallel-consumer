#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Fails if any GitHub Action is used at more than one version across .github/workflows/.
#
# WHY THIS EXISTS
#
# Dependabot bumps the versions that are already there. It cannot notice a NEW workflow authored at
# an old version - which is exactly how `shell-hygiene.yml` shipped with `actions/checkout@v4` while
# 21 other uses were on `@v6`. A human caught it in review; nothing else would have.
#
# The rule is deliberately NOT "checkout must be v6", which rots the moment you bump. It is "pick a
# version and use it everywhere", which stays true across bumps and needs no edit when Dependabot
# raises one - because Dependabot raises ALL uses of an action in a single PR, so a grouped bump
# lands consistent and this check stays green.
#
# WHAT IT DOES NOT CHECK
#
# SHA pins. The `astubbs/*` forks are pinned to a commit on a BRANCH on purpose (a moving branch ref
# would be the unsafe choice), so "same version everywhere" does not apply to them - each use site
# tracks a different ref by design. They are also ignored by Dependabot, for the same reason.
#
# Run: bin/check-action-versions.sh   (CI runs it in the Repo Hygiene workflow)

set -euo pipefail

cd "$(dirname "$0")/.."

WORKFLOWS=".github/workflows"

# action -> sorted, unique list of tag versions it is used at. SHA pins are excluded by requiring
# the ref to look like a version tag.
report="$(grep -rhoE --include='*.yml' --include='*.yaml' \
                 'uses:[[:space:]]+[A-Za-z0-9._-]+/[A-Za-z0-9._-]+@v[0-9]+(\.[0-9]+)*' "$WORKFLOWS" \
          | sed -E 's/uses:[[:space:]]+//' \
          | sort -u)"

violations=0
while IFS= read -r action; do
    [ -n "$action" ] || continue
    versions="$(printf '%s\n' "$report" | grep -E "^${action}@" | sed -E 's/.*@//' | sort -u)"
    count="$(printf '%s\n' "$versions" | grep -c . || true)"
    if [ "$count" -gt 1 ]; then
        echo "VIOLATION: $action is used at $count versions: $(printf '%s\n' "$versions" | tr '\n' ' ')"
        # Name the files, so the fix does not need a second search.
        for v in $versions; do
            printf '%s\n' "  $v:"
            grep -rln --include='*.yml' --include='*.yaml' "uses:.*${action}@${v}\b" "$WORKFLOWS" | sed 's/^/    /'
        done
        violations=$((violations + 1))
    fi
done < <(printf '%s\n' "$report" | sed -E 's/@.*//' | sort -u)

if [ "$violations" -gt 0 ]; then
    cat >&2 <<'EOF'

FAIL: an action is used at more than one version.

Pick one and use it everywhere. Dependabot bumps every use of an action in a single grouped PR, so
a consistent repo stays consistent through upgrades - but it cannot fix a NEW workflow authored at
an old version, which is the case this check exists for.

See bin/AGENTS.md -> "Workflows".
EOF
    exit 1
fi

echo "ok:   every action is used at a single version across $WORKFLOWS"
