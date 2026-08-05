#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Regenerate docs/TODO_INDEX.md - an index of every TODO/FIXME/XXX marker in the tree.
#
# Usage:
#   bin/todo-index.sh            # rewrite docs/TODO_INDEX.md
#   bin/todo-index.sh --check    # exit 1 if the committed index is stale (for CI)
#
# Why generated and not hand-maintained: a hand-written list of TODOs is wrong the day after it is
# written. This is cheap to regenerate, so the index is always a true reflection of the tree - and
# `--check` makes staleness visible instead of silent.
#
# Scope note: markers inside THIS script and inside the generated index are excluded, otherwise the
# tool indexes itself.

set -euo pipefail

cd "$(dirname "$0")/.."

OUT="docs/TODO_INDEX.md"
SELF="bin/todo-index.sh"

CHECK_MODE=false
[[ "${1:-}" == "--check" ]] && CHECK_MODE=true

# Files worth scanning: source, scripts, build and workflow config. Deliberately excludes build
# output, the git dir, and agent scratch dirs.
#
# DOCS (.adoc/.md) ARE DELIBERATELY EXCLUDED, and it is worth saying why, because "surely docs could
# contain a TODO too" is a reasonable first reaction. It was tried: scanning them added 4 hits and
# not one was a marker. Three were docs/refactoring.md QUOTING markers that the index already lists
# from their .java source - so the same work appeared twice and the count inflated - and the fourth
# was prose ("seeded from a code scan (TODO/FIXME + ...)"). That is structural rather than bad luck:
# docs/refactoring.md is where marker triage lives, and AGENTS.md / docs/inflight/ / CHANGELOG.adoc
# describe this tool, so scanning them means indexing the index-of-work. The generated index itself
# is the reductio - it alone accounts for ~95 self-referential hits.
#
# If that ever changes (a genuine marker left in prose), prefer moving it into the code it concerns,
# where it belongs and where this scan will find it.
#
# $OUT is excluded belt-and-braces, so a future widening cannot make the index index itself.
list_files() {
    git ls-files \
        '*.java' '*.sh' '*.xml' '*.yml' '*.yaml' \
        | grep -v "^${SELF}$" \
        | grep -v "^${OUT}$" \
        | sort
}

# A marker is TODO / FIXME / XXX as a WORD, case-insensitive. Captures the rest of the line as the
# note. Deliberately not anchored to comment syntax so it works across .java/.sh/.xml/.yml alike.
MARKER_RE='\b([Tt][Oo][Dd][Oo]|[Ff][Ii][Xx][Mm][Ee]|XXX)\b'
# ...but not when it is an IDENTIFIER rather than a marker, e.g. a shell variable `todo=()` or a
# field named `fixmeCount`. Cheap heuristic: drop lines where the word is immediately assigned to.
# Not every occurrence of the word is a marker. Skip the cases seen in this tree:
#   shell variables   todo=()   todo+=("$t")   ${todo[*]}   ${#todo[@]}
#   YAML/schema keys  todo:            (upstream-map.yaml uses `todo` as a field name)
#   string literals   Mono.just("something todo")
# NOTE: this filter is applied to `grep -n` output, so line-anchored patterns must allow the
# leading "<lineno>:" prefix - anchoring on ^[[:space:]] alone silently never matches.
#   references TO a marker elsewhere, e.g. "the run-length optimisation TODO on {@link X}"
#   compound names    todo-index.sh, TODO_INDEX.md   (a filename, not a marker - this one is live:
#                                                     .github/workflows/claude-code-review.yml names
#                                                     the script in a comment)
NOT_A_MARKER_RE='(\b([Tt][Oo][Dd][Oo]|[Ff][Ii][Xx][Mm][Ee]|XXX)[A-Za-z0-9_]*[[:space:]]*(=|\+=)|\$\{#?[Tt][Oo][Dd][Oo]|^[0-9]+:[[:space:]]*[A-Za-z_]*[Tt][Oo][Dd][Oo][A-Za-z_]*:|"[^"]*[Tt][Oo][Dd][Oo][^"]*"|(optimisation|optimization) TODO|[Tt][Oo][Dd][Oo][-_][A-Za-z])'

emit_body() {
    local current_group=""
    while IFS= read -r file; do
        # group by module (path up to /src/), else by top-level dir
        local group
        case "$file" in
            */src/*) group="${file%%/src/*}" ;;
            bin/*)   group="bin (scripts)" ;;
            .github/*) group=".github (CI)" ;;
            *)       group="$(dirname "$file")" ;;
        esac
        [[ "$group" == "." ]] && group="(repo root)"

        # collect this file's markers as 'line<TAB>text'
        local hits
        hits=$(grep -nE "$MARKER_RE" "$file" 2>/dev/null | grep -vE "$NOT_A_MARKER_RE" || true)
        [[ -z "$hits" ]] && continue

        if [[ "$group" != "$current_group" ]]; then
            printf '\n### %s\n\n' "$group"
            current_group="$group"
        fi

        printf '**`%s`**\n\n' "$file"
        while IFS= read -r hit; do
            local lineno text
            lineno="${hit%%:*}"
            text="${hit#*:}"
            # tidy: strip leading comment punctuation and whitespace, collapse runs of spaces
            text=$(printf '%s' "$text" | sed -E 's/^[[:space:]]*(\/\/|\*|#|<!--)?[[:space:]]*//; s/[[:space:]]+/ /g; s/[[:space:]]*(-->)?[[:space:]]*$//')
            printf -- '- L%s - %s\n' "$lineno" "$text"
        done <<< "$hits"
        printf '\n'
    done < <(list_files)
}

count_markers() {
    local n=0
    while IFS= read -r file; do
        local c
        c=$(grep -nE "$MARKER_RE" "$file" 2>/dev/null | grep -vcE "$NOT_A_MARKER_RE" || true)
        n=$((n + ${c:-0}))
    done < <(list_files)
    printf '%s' "$n"
}

generate() {
    local count
    count=$(count_markers)

    cat <<EOF
# TODO index

**Generated file - do not edit by hand.** Regenerate with \`bin/todo-index.sh\`
(\`bin/todo-index.sh --check\` fails if this file is stale).

Every \`TODO\` / \`FIXME\` / \`XXX\` marker in the tracked tree, grouped by module. **$count marker(s)** at
the time of generation.

## Prioritised? See the refactoring backlog

This file is a raw, generated inventory - deliberately **not** prioritised, because it is rewritten
wholesale on every run. Triage lives in [\`docs/refactoring.md\`](refactoring.md), which is the
repo's existing backlog: markers that turn out to be real deferred work get written up there (grouped
by file, with breaking changes in their own release-gated section). Do **not** start a parallel
priority list - that was tried and duplicated the backlog.

Most markers are notes-to-self and should stay exactly where they are; this index makes them
discoverable in aggregate without promoting them to tasks.

## How to use this

Markers here are *not* a backlog - most are notes-to-self left next to the code they concern, and
that is the right place for them. This index exists so they are **discoverable in aggregate**: to
spot clusters (several markers around one class usually means a design that wants revisiting), and
so that the backlog can point at the code that motivates an item instead of restating it.

The durable rule of thumb: if a marker describes work someone should actually schedule, write it up
in \`docs/refactoring.md\` (with a link back to the code). If it is context for whoever next edits
that line, leave it in the code - it will show up here.
EOF

    emit_body
}

if $CHECK_MODE; then
    tmp=$(mktemp)
    trap 'rm -f "$tmp"' EXIT
    generate > "$tmp"
    if ! diff -q "$OUT" "$tmp" > /dev/null 2>&1; then
        echo "STALE: $OUT does not match the tree. Regenerate with: bin/todo-index.sh" >&2
        diff "$OUT" "$tmp" | head -40 >&2 || true
        exit 1
    fi
    echo "$OUT is up to date."
else
    generate > "$OUT"
    echo "Wrote $OUT ($(count_markers) markers)."
fi
