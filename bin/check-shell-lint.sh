#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# ShellCheck over bin/ and .claude/hooks/, gating on ERRORS only.
#
# WHY IT EXISTS. August 2026 produced a portability class this repo paid for repeatedly - `mapfile`
# on a bash 3.2 platform, `stat -c`, `awk -v` with an embedded newline, GNU `sed -i` - and the
# expensive ones were never the constructs that error out. They were the ones ACCEPTED with a
# different meaning, or failing where the script does not check, so the run continues and produces a
# confident wrong answer. ShellCheck is largely machine-detectable coverage for that class, and it
# also flags unquoted expansion and exit-code swallowing, which is the mechanism underneath several
# of this month's silent false greens. The lane's severity tiers and what stays off are in
# docs/inflight/static-shell-lint-severity-tiers.md.
#
# WHY ERRORS ONLY, AND NOT WARNINGS. Measured on the tree that introduced this script: 3 errors, 10
# warnings, 49 info, 18 style. Gating the whole 80 would mean either a red build on arrival or a
# suppression file nobody reads - and this repo has just spent a PR replacing exactly that shape for
# SpotBugs. The severities that are off, and what turns each back on, are in
# docs/inflight/static-shell-lint-severity-tiers.md. Raise the floor by lowering SEVERITY below;
# there is no per-code suppression list on purpose, because the moment there is one, it grows.
#
# Both errors it found on arrival were real:
#   - bin/check-quarantine-registry.sh read `"$method[[:space:]]"` as an array expansion (SC1087).
#   - bin/check-shell-sigpipe.sh opened a PROSE comment with the word `shellcheck`, so ShellCheck
#     parsed it as a directive, errored SC1072/SC1073, and stopped analysing the rest of that file.
#     A sentence about the linter had silently switched the linter off - in the script whose entire
#     job is guarding against silent failure.
#
# Exit codes: 0 clean, 1 findings at or above SEVERITY, 2 cannot run (shellcheck missing).
# The 2 matters: "the linter is not installed" must never read as "the code is clean", which is the
# same fail-closed contract bin/lib/node-gate.sh gives the node-backed gates.

set -euo pipefail

SEVERITY="${SHELL_LINT_SEVERITY:-error}"

if ! command -v shellcheck > /dev/null 2>&1; then
    echo "check-shell-lint: shellcheck is not installed - CANNOT RUN (this is not a pass)" >&2
    echo "  macOS: brew install shellcheck    Debian/Ubuntu: apt-get install -y shellcheck" >&2
    exit 2
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

targets=()
while IFS= read -r f; do
    targets+=("$f")
done < <(git ls-files 'bin/*.sh' '.claude/hooks/*.sh' | sort)

if [ "${#targets[@]}" -eq 0 ]; then
    echo "check-shell-lint: NO SCRIPTS MATCHED - refusing to report success over an empty set" >&2
    exit 2
fi

echo "check-shell-lint: ${#targets[@]} script(s), severity floor '${SEVERITY}'"

if shellcheck --severity="$SEVERITY" --shell=bash "${targets[@]}"; then
    echo "check-shell-lint: clean at severity '${SEVERITY}'"
    exit 0
fi

cat >&2 <<'GUIDANCE'

check-shell-lint FAILED.

Fix the finding rather than adding a suppression. If a finding is genuinely wrong for this codebase,
raise it - the severity tiers live in docs/inflight/static-shell-lint-severity-tiers.md and changing
them is a decision with a written reason, not an inline silence.
GUIDANCE
exit 1
