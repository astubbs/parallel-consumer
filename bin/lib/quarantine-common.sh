#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Shared definitions for the quarantine lane - THE single home of the @Quarantined detection pattern
# and the registry parsing (previously duplicated ~8x across scripts + workflows with drift;
# ce-review P1/P2 findings). Source this; do not copy from it.

# Matches real annotation USAGE: start of line, optionally preceded by other stacked annotations
# (e.g. `@Test @Quarantined(...)`), plain or fully-qualified form. Does NOT match string literals
# (those have a quote before the @ - e.g. the self-tests' check("@Quarantined(") - so anchoring to
# an annotation-only prefix is what keeps releases from being blocked by the tooling's own sources).
QUARANTINE_ANNOTATION_ERE='^[[:space:]]*(@[[:alnum:]_.]+(\([^)]*\))?[[:space:]]*)*@(bz\.stub\.parallelconsumer\.)?Quarantined\('

REGISTRY="${REGISTRY:-docs/quarantined-tests.md}"

# All java files containing real annotation usage (repo-relative paths).
#
# WORKTREE ROOTS ARE EXCLUDED, BOTH OF THEM. `.claude/worktrees/<name>` and `/.worktrees/` each hold
# a full checkout of some OTHER branch - .gitignore names both, calling the second "the other root in
# use". Without the exclusions the scan is green inside a worktree and red on a clean master in the
# primary checkout, reporting drift from ~60 sibling branches: annotations that are not on this
# branch at all, judged against a registry that is. It stayed hidden because the repo's own rule is
# never to work in the primary checkout, so nobody ran it there; the pre-commit hook added in this PR
# does. Excluding only one root was the same bug with a smaller blast radius - review caught it, and
# bin/test-check-quarantine-registry.sh now has a fixture for each. .git is excluded for the same
# shape of reason, cheaply.
quarantined_files() {
    grep -rlE --include='*.java' --exclude-dir=target --exclude-dir=.claude --exclude-dir=.worktrees \
        --exclude-dir=.git "$QUARANTINE_ANNOTATION_ERE" . 2>/dev/null || true
}

# Count of annotation usages in one file.
quarantined_occurrences() {
    grep -cE "$QUARANTINE_ANNOTATION_ERE" "$1" 2>/dev/null || echo 0
}

# The human-readable audit listing: every annotation usage with the following lines that carry its
# `fixedBy`/reason. Lives here rather than in the caller because it needs the SAME exclusions as
# quarantined_files() above - bin/quarantined-test.sh had its own copy of this grep without them,
# so the worktree-pollution bug fixed there still reproduced in the audit output, listing
# annotations from ~60 sibling branches. One pattern and one exclusion set, one place.
quarantined_audit() {
    grep -rnE --include='*.java' --exclude-dir=target --exclude-dir=.claude --exclude-dir=.worktrees \
        --exclude-dir=.git -A 4 "$QUARANTINE_ANNOTATION_ERE" . 2>/dev/null
}

# Registry entries, one per line: `Class.method` (or `Class` for class-level quarantines).
registry_entries() {
    grep -E '^- \[ \] `' "$REGISTRY" 2>/dev/null | sed -E 's/^- \[ \] `([^`]+)`.*/\1/' || true
}

# The full text block of one entry (its line + continuation lines until the next entry).
# Fixed-string matching via awk index() - registry text is never treated as a pattern.
registry_entry_block() {
    awk -v needle="- [ ] \`$1\`" '
        index($0, needle) == 1 {f=1}
        f && /^- \[ \]/ && index($0, needle) != 1 {exit}
        f {print}
    ' "$REGISTRY"
}
