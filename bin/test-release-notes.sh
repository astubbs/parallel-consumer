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
#            string `== 0.6.0.0` and matched nothing - and `--strict` makes it fatal, which
#            is what a real release passes and a rehearsal does not
#        4.  a version prefix does NOT match a longer version (0.6.0.1 vs 0.6.0.10)
#        5.  a missing section is exit 2, not empty output
#        6.  a present-but-empty section is exit 2 - INCLUDING one that is non-empty but
#            renders to nothing (only `//` comments, only a `+`), which is the same empty
#            release body astubbs#197 was about, reached by a different route
#        7.  AsciiDoc outside the convertible subset is exit 3, not mangled markup
#        8.  headings, bullets, ordered lists, admonitions, continuations and comments
#        9.  bold: AsciiDoc `*one*` and `**two**` both become Markdown `**two**`
#       10.  `*` inside a `monospace` span is not read as an emphasis marker
#       11.  link macros: absolute URLs, and relative targets absolutised at the ref
#       12.  a usage error is exit 1, not argparse's default 2 - which would be
#            indistinguishable from "no section for that version"
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

# No interpreter is CANNOT RUN (exit 2), never FAIL. bin/check-all.sh and repo-hygiene.yml's
# macOS lane both distinguish the two, and "a suite that never ran, reported as a suite that ran
# and found a defect" is the attribution failure they exist to prevent. Python 3 specifically:
# the renderer's `print(..., file=...)` is a syntax error under a `python` that is Python 2.
PY=""
for c in python3 python; do
  if command -v "$c" >/dev/null 2>&1 &&
     "$c" -c 'import sys; sys.exit(0 if sys.version_info[0] >= 3 else 1)' >/dev/null 2>&1; then
    PY="$c"
    break
  fi
done
if [ -z "$PY" ]; then
  echo "test-release-notes: no working Python 3 on PATH - CANNOT RUN" >&2
  exit 2
fi

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

# Writes $1 to a changelog file and echoes its path. mktemp, not $RANDOM: every call here is
# inside a `$(...)`, and bash before 5.1 does not reseed $RANDOM in a subshell - so on the
# macOS 3.2 lane every fixture would land on one path and silently overwrite the last, while
# fixtures written early are still referenced by asserts near the end of the file.
changelog() { # <text>
    local path
    path="$(mktemp "$tmp/changelog-XXXXXX.adoc")"
    printf '%s\n' "$1" > "$path"
    echo "$path"
}

# Echoes the renderer's exit code, discarding its output.
render_status() { # <changelog-path> <version> [extra args...]
    local ec=0
    "$PY" "$RENDERER" "${@:2}" --changelog "$1" >/dev/null 2>&1 || ec=$?
    echo "$ec"
}

# Echoes the renderer's stdout.
render() { # <changelog-path> <version> [extra args...]
    "$PY" "$RENDERER" "${@:2}" --changelog "$1" 2>/dev/null
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
    "1" "$("$PY" "$RENDERER" 0.6.0.0 --changelog "$SUFFIXED" 2>&1 >/dev/null | grep -c "unreleased")"
# A rehearsal tolerates the suffix; a real release must not tag a section still labelled
# unreleased, and release.yml passes --strict whenever dryRun is false.
assert "--strict makes an unfrozen heading fatal" \
    4 "$(render_status "$SUFFIXED" 0.6.0.0 --strict)"
assert "...and prints no notes when it does" \
    "" "$(render "$SUFFIXED" 0.6.0.0 --strict)"

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

# "The section has lines in it" and "the section renders to something" are DIFFERENT tests, and
# only the second one is the promise. A section holding nothing but dropped constructs passes the
# first and produces a one-byte body - the empty release page of astubbs#197, reached from the
# other side. Emptiness is therefore judged on the converted output.
RENDERS_TO_NOTHING=$(changelog '== 0.6.0.0

// a comment, which is dropped

+

== 0.5.3.3

Body.')
assert "a section that renders to nothing fails loudly" \
    2 "$(render_status "$RENDERS_TO_NOTHING" 0.6.0.0)"
# Counted in BYTES, not compared to "": `$(...)` strips the trailing newline, so the bug this
# case is about - a one-byte "\n" body, published as a blank release page - compares equal to
# empty and the assertion passes over it.
assert "...and prints nothing to stdout rather than an empty body" \
    0 "$(render "$RENDERS_TO_NOTHING" 0.6.0.0 | wc -c | tr -d ' ')"

# argparse's own exit code is 2, which is this script's "no section for that version". Sharing it
# would send a release operator looking for a missing changelog section over a mistyped flag.
assert "a usage error is exit 1, not the no-section code" \
    1 "$(render_status "$THREE_SECTIONS" --not-a-flag)"

for construct in '[source,java]' '----' '|===' '[[anchor]]' 'include::other.adoc[]' \
                 'ifdef::x[]' ':attribute: value' 'see <<other-section>>' \
                 "'''" '<<<' 'an unclosed `monospace span'; do
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

# AsciiDoc bold is *constrained*: bare asterisks surrounded by whitespace are literal text, not
# emphasis delimiters. A regex that ignores those boundaries silently rewrites `1 * cores` into
# broken emphasis - a mangled body, which is exactly what this renderer promises never to ship.
BARE_ASTERISK=$(changelog '== 0.6.0.0

* Budget roughly 3 * 4 * 5 records.
* Pass --forkCount 1 * cores, then *really* measure it.')
assert "bare asterisks in prose are not emphasis" \
    '- Budget roughly 3 * 4 * 5 records.
- Pass --forkCount 1 * cores, then **really** measure it.' \
    "$(render "$BARE_ASTERISK" 0.6.0.0)"

# AsciiDoc `**bold**` is UNCONSTRAINED - unlike `*bold*` it may sit against word characters. The
# converter applies constrained boundaries to both, so an intraword `**` span is not matched. That
# is deliberately harmless: `**x**` is already the Markdown spelling, so converting it is the
# identity, and CommonMark renders intraword `**` as strong anyway. Asserted so the pass-through
# is a locked-in guarantee rather than an accident of the regex.
UNCONSTRAINED=$(changelog '== 0.6.0.0

* Reads un**bel**ievable and re**start** correctly.
* Kafka **3.9.1** is the default.')
assert "unconstrained intraword bold passes through as valid Markdown" \
    '- Reads un**bel**ievable and re**start** correctly.
- Kafka **3.9.1** is the default.' \
    "$(render "$UNCONSTRAINED" 0.6.0.0)"

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

# One invocation per section, keeping its stderr rather than throwing it away and re-running to
# get it back. render_status/render cannot be reused here: the first discards the diagnostics this
# loop exists to print, the second discards them and the exit code.
real_failures=0
while IFS= read -r version; do
    status=0
    "$PY" "$RENDERER" "$version" --changelog "$REAL_CHANGELOG" \
        >/dev/null 2>"$tmp/real-section-stderr.txt" || status=$?
    if [ "$status" != "0" ]; then
        echo "FAIL: CHANGELOG.adoc section '$version' does not render (exit $status):"
        sed 's/^/        /' "$tmp/real-section-stderr.txt"
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
