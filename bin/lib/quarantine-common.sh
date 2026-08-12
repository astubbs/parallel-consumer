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

REGISTRY="${REGISTRY:-docs/QUARANTINED_TESTS.md}"

# All java files containing real annotation usage (repo-relative paths).
quarantined_files() {
    grep -rlE --include='*.java' --exclude-dir=target "$QUARANTINE_ANNOTATION_ERE" . 2>/dev/null || true
}

# Count of annotation usages in one file.
quarantined_occurrences() {
    grep -cE "$QUARANTINE_ANNOTATION_ERE" "$1" 2>/dev/null || echo 0
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
