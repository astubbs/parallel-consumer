#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Fails if any shell script in bin/ pipes into `grep -q` while `pipefail` is set.
#
# WHY THIS EXISTS
#
# `writer | grep -q PATTERN` under `set -o pipefail` inverts its own answer. `grep -q` exits the
# instant it matches; the writer's next write gets EPIPE and dies with 141; `pipefail` promotes that
# to the pipeline's exit status. So a MATCH reports failure - the check fails precisely when it
# found what it was looking for.
#
# It hides well. The writer only gets that far if it has more than one pipe buffer (64 KiB on Linux)
# still to write when the reader leaves, so small inputs pass forever and the bug surfaces only once
# real data grows past the threshold. bin/check-review-posted.sh shipped with it and reported "no
# review posted" on four PRs whose reviews had posted; the one that broke it had the citation 4.7 KB
# in with a 127 KB report behind it.
#
# shellcheck does NOT catch this - verified against the known-bad line, which it passed clean. Hence
# a bespoke grep rather than a linter.
#
# THE FIX is always the same: a herestring. `grep -q PATTERN <<<"$data"` has no pipeline, so no
# SIGPIPE and nothing for pipefail to promote.
#
# SCOPE: `grep -q` only - in every flag spelling (`-q`, `-qE`, `-Eq`, `grep -v -q`, `--quiet`,
# `--silent`). Piping into `head`/`awk` can misbehave the same way, but both are common, usually
# consume everything, and are often written with `|| true`. Flagging them would be noise; the
# guidance lives in bin/AGENTS.md instead.
#
# Run: bin/check-shell-sigpipe.sh   (CI runs it alongside the copyright self-test)

set -euo pipefail

# Scans bin/ by default; takes a directory so the self-test can point it at fixtures.
SCAN_DIR="${1:-}"
# SCANS .claude/hooks/ AS WELL AS bin/. The hooks are shell that runs on every agent session with no
# interactive user to notice breakage - exactly the population this guard exists for - and the gate
# used to be blind to them. A live instance was found there by review, not by this check: the session
# index piped into `grep -q` under pipefail, harmless only because its input was well under the 64KiB
# pipe buffer that triggers the inversion.
if [ -z "$SCAN_DIR" ]; then
    cd "$(dirname "$0")/.."
    # THE `lib/` DIRECTORIES COUNT TOO. `ls "$d"/*.sh` does not recurse, so bin/lib/ and
    # .claude/hooks/lib/ were unscanned - and a shared helper is the worst place for this bug,
    # because every caller inherits it. Named explicitly rather than made recursive: the scan is a
    # list of directories whose contents are shell, and a `find` would start reading fixtures.
    SCAN_DIRS="bin bin/lib .claude/hooks .claude/hooks/lib"
else
    SCAN_DIRS="$SCAN_DIR"
fi

self_name="$(basename "$0")"
violations=0

for f in $(for d in $SCAN_DIRS; do ls "$d"/*.sh 2>/dev/null; done); do
    [ -e "$f" ] || continue
    # Two files must contain the anti-pattern as DATA rather than as code that runs: this script,
    # whose failure message shows the "wrong" half of a worked example, and its self-test, whose
    # fixtures are literally the shapes being detected. Excluding anything else would be hiding a
    # real violation. Matched on BASENAME, not a hardcoded path, so a rename or a move cannot
    # silently turn the exclusion off.
    # `if`, not `[ A ] || [ B ] && continue`: under `set -e` that list returns non-zero when both
    # tests are false, which is the classic bash foot-gun this file exists to discourage.
    base="$(basename "$f")"
    if [ "$base" = "$self_name" ] || [ "$base" = "test-$self_name" ]; then
        continue
    fi
    grep -q 'set -.*pipefail' "$f" || continue

    # Strip comment-only lines so the explanations above (and in the scripts) do not self-trigger.
    while IFS= read -r hit; do
        echo "VIOLATION: $f:$hit"
        violations=$((violations + 1))
    # Matches `| grep -q`, `-qE`, `-Eq`, split flags (`grep -v -q`) and `--quiet`/`--silent`.
    # A bare word like `query` cannot match: a leading `-` is required on the q-bearing token.
    done < <(grep -nE '\|[[:space:]]*grep([[:space:]]+-[a-zA-Z-]+)*[[:space:]]+(-[a-zA-Z]*q|--quiet|--silent)' "$f" \
             | grep -vE '^[0-9]+:[[:space:]]*#' || true)
done

if [ "$violations" -gt 0 ]; then
    cat >&2 <<'EOF'

FAIL: pipe into `grep -q` under `set -o pipefail`.

grep -q exits on its first match, the writer takes EPIPE (141), and pipefail makes that the
pipeline's status - so the check FAILS when it FINDS the thing. Only bites once the writer has
more than 64 KiB left to write, so it passes small fixtures and breaks in production.

Rewrite as a herestring:

    if printf '%s\n' "$data" | grep -qE "$pattern"; then    # wrong
    if grep -qE "$pattern" <<<"$data"; then                 # right

See bin/AGENTS.md -> "Scripts that guard other scripts".
EOF
    exit 1
fi

echo "ok:   no shell in $SCAN_DIRS pipes into grep -q under pipefail"
