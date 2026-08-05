#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/release-notes.py.
#
# Two halves:
#
#   A. Synthetic changelogs, asserting the renderer's contract:
#        1.  the right section is picked, and neighbouring sections are not bled in
#        2.  `v0.5.2.2`-style headings and `0.5.2.2` arguments name the same release
#        3.  a heading suffix such as `(unreleased)` still matches (and warns) - this is
#            the exact bug from astubbs#197: release.yml compared the heading to the literal
#            string `== 0.6.0.0` and matched nothing
#        4.  a version prefix does NOT match a longer version (0.6.0.1 vs 0.6.0.10)
#        5.  a missing section is exit 2, not empty output
#        6.  a present-but-empty section is exit 2
#        7.  AsciiDoc outside the convertible subset is exit 3, not mangled markup
#        8.  headings, bullets, ordered lists, admonitions, continuations and comments
#        9.  bold: AsciiDoc `*one*` and `**two**` both become Markdown `**two**`
#       10.  `*` inside a `monospace` span is not read as an emphasis marker
#       11.  link macros: absolute URLs, and relative targets absolutised at the ref
#
#   B. The REAL CHANGELOG.adoc: every version section in the file must render with exit 0.
#      Case 7 is worth its keep only if something keeps checking the actual file against
#      it - this is that something, and it is why a future hand-written section that
#      reaches for a table or a source block fails in CI rather than on the release page.
#
# Run: bin/test-release-notes.sh   (CI runs it before the release job renders anything)

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RENDERER="$HERE/release-notes.py"
REAL_CHANGELOG="$(dirname "$HERE")/CHANGELOG.adoc"

failures=0
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1"
        echo "        expected: $2"
        echo "        actual:   $3"
        failures=$((failures + 1))
    fi
}

# Writes $1 to a changelog file and echoes its path.
changelog() { # <text>
    local path="$tmp/changelog-$RANDOM.adoc"
    printf '%s\n' "$1" > "$path"
    echo "$path"
}

# Echoes the renderer's exit code, discarding its output.
render_status() { # <changelog-path> <version> [extra args...]
    local ec=0
    python3 "$RENDERER" "${@:2}" --changelog "$1" >/dev/null 2>&1 || ec=$?
    echo "$ec"
}

# Echoes the renderer's stdout.
render() { # <changelog-path> <version> [extra args...]
    python3 "$RENDERER" "${@:2}" --changelog "$1" 2>/dev/null
}

# --- A. contract ------------------------------------------------------------------------

THREE_SECTIONS=$(changelog '= Change Log

== 0.6.0.0

Newest.

== 0.5.3.3

Middle.

== v0.5.2.2

Oldest.')

assert "picks the requested section only" \
    "Newest." "$(render "$THREE_SECTIONS" 0.6.0.0)"
assert "a middle section stops at the next heading" \
    "Middle." "$(render "$THREE_SECTIONS" 0.5.3.3)"
assert "the last section runs to end of file" \
    "Oldest." "$(render "$THREE_SECTIONS" 0.5.2.2)"
assert "a v-prefixed argument finds the same section" \
    "Oldest." "$(render "$THREE_SECTIONS" v0.5.2.2)"

SUFFIXED=$(changelog '== 0.6.0.0 (unreleased)

Body.')
assert "a heading suffix still matches (astubbs#197 regression)" \
    "Body." "$(render "$SUFFIXED" 0.6.0.0)"
assert "...and warns about the suffix" \
    "1" "$(python3 "$RENDERER" 0.6.0.0 --changelog "$SUFFIXED" 2>&1 >/dev/null | grep -c "unreleased")"

PREFIXES=$(changelog '== 0.6.0.10

Ten.

== 0.6.0.1

One.')
assert "0.6.0.1 does not match the longer 0.6.0.10" \
    "One." "$(render "$PREFIXES" 0.6.0.1)"

assert "a missing section fails loudly" \
    2 "$(render_status "$THREE_SECTIONS" 0.4.0.0)"
assert "a missing section prints nothing to stdout" \
    "" "$(render "$THREE_SECTIONS" 0.4.0.0)"

EMPTY_SECTION=$(changelog '== 0.6.0.0

== 0.5.3.3

Body.')
assert "an empty section fails loudly" \
    2 "$(render_status "$EMPTY_SECTION" 0.6.0.0)"

for construct in '[source,java]' '----' '|===' '[[anchor]]' 'include::other.adoc[]' \
                 'ifdef::x[]' ':attribute: value' 'see <<other-section>>'; do
    UNSUPPORTED=$(changelog "== 0.6.0.0

$construct")
    assert "unsupported AsciiDoc is an error, not mangled markup: $construct" \
        3 "$(render_status "$UNSUPPORTED" 0.6.0.0)"
done

BLOCKS=$(changelog '== 0.6.0.0

Intro paragraph.

=== Fixes

* Top level.
** Nested.
+
Continuation.

==== Deeper heading

. First.
. Second.

NOTE:: An admonition.

// a comment that must not reach the release page')
assert "block conversions" \
    'Intro paragraph.

### Fixes

- Top level.
  - Nested.

Continuation.

#### Deeper heading

1. First.
1. Second.

> **Note:** An admonition.' "$(render "$BLOCKS" 0.6.0.0)"

BOLD=$(changelog '== 0.6.0.0

* *Constrained* and **unconstrained** bold.
* Package names (`bz.stub.parallelconsumer.*`) keep their asterisk.')
assert "bold and code spans" \
    '- **Constrained** and **unconstrained** bold.
- Package names (`bz.stub.parallelconsumer.*`) keep their asterisk.' \
    "$(render "$BOLD" 0.6.0.0)"

LINKS=$(changelog '== 0.6.0.0

* An https://github.com/astubbs/parallel-consumer/pull/55[#55] link.
* A bare https://example.com/x[] link.
* See link:docs/self-hosted-runner.md[the runner doc] and link:docs/inflight/[the directory].')
assert "link macros, with relative targets absolutised at the ref" \
    '- An [#55](https://github.com/astubbs/parallel-consumer/pull/55) link.
- A bare <https://example.com/x> link.
- See [the runner doc](https://github.com/astubbs/parallel-consumer/blob/v0.6.0.0/docs/self-hosted-runner.md) and [the directory](https://github.com/astubbs/parallel-consumer/tree/v0.6.0.0/docs/inflight/).' \
    "$(render "$LINKS" 0.6.0.0)"

# --- B. the real changelog --------------------------------------------------------------

real_failures=0
while IFS= read -r version; do
    status=$(render_status "$REAL_CHANGELOG" "$version")
    if [ "$status" != "0" ]; then
        echo "FAIL: CHANGELOG.adoc section '$version' does not render (exit $status):"
        python3 "$RENDERER" "$version" --changelog "$REAL_CHANGELOG" >/dev/null 2>&1
        python3 "$RENDERER" "$version" --changelog "$REAL_CHANGELOG" 2>&1 >/dev/null | sed 's/^/        /'
        real_failures=$((real_failures + 1))
    fi
done < <(grep -oE '^== v?[0-9][0-9.]*' "$REAL_CHANGELOG" | sed 's/^== //')
assert "every section of the real CHANGELOG.adoc renders" 0 "$real_failures"

echo
if [ "$failures" -eq 0 ]; then
    echo "All bin/release-notes.py self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
