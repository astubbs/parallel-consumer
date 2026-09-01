#!/usr/bin/env python3
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
"""Render one CHANGELOG.adoc version section as Markdown, for a GitHub Release body.

Why this exists: GitHub Release bodies are Markdown, our changelog is AsciiDoc, and
release.yml used to do the conversion with an inline awk+sed one-liner that silently fell
back to auto-generated notes when it matched nothing. It matched nothing (the heading is
`== 0.6.0.0 (unreleased)`, the awk compared against the exact string `== 0.6.0.0`), so the
curated notes never reached the release page and no error was raised. See astubbs#197.

Conversion policy - a bounded, checked subset, NOT a general AsciiDoc engine:
  - We convert only the constructs the changelog actually uses (headings, bullets, ordered
    lists, link macros, bold, admonitions, list continuations, comments). Everything else
    passes through unchanged, which is safe for the constructs Markdown and AsciiDoc share
    (paragraphs, `monospace`, _italics_).
  - Anything outside that subset that WOULD render as mangled markup (block delimiters,
    block attributes, anchors, tables, includes, conditionals, xrefs) is an ERROR, not a
    silent pass. Fix the changelog or teach this script - do not ship broken markup.
  - No asciidoctor/pandoc dependency: the release job must not be able to fail because a
    gem or a Python package would not install.

Relative `link:docs/FOO[...]` targets are rewritten to absolute URLs at the released ref,
because a relative link in a release body resolves against github.com and 404s.

Usage:
  bin/release-notes.py <version> [--changelog PATH] [--repo-url URL] [--ref REF] [--strict]

Exit codes:
  0  notes written to stdout
  1  usage / IO error  (argparse's own exit code is overridden to 1, so 2 keeps one meaning)
  2  no section for that version, or the section produces no notes  (the bug this script
     fixes: never let this be silent, and never substitute auto-generated notes for it)
  3  the section uses AsciiDoc this script does not convert
  4  --strict, and the heading is not frozen yet (still carries a suffix such as
     `(unreleased)`); without --strict that is a warning

Nothing here may exit 0 with an empty body. "The section exists" is not the same test as
"the section renders to something": a section holding only `//` comments passes the first
and fails the second, and an empty release body is the precise failure this script exists
to prevent - so the emptiness check runs on the CONVERTED output, not on the raw lines.

Tested by bin/test-release-notes.sh (CI runs it before the release job uses this script).
"""

import argparse
import os
import re
import sys

DEFAULT_REPO_URL = "https://github.com/astubbs/parallel-consumer"

EXIT_USAGE = 1
EXIT_NO_SECTION = 2
EXIT_UNSUPPORTED = 3
EXIT_NOT_FROZEN = 4

# A level-2 heading: the version sections. Historic sections are `== v0.5.2.2`, newer ones
# `== 0.6.0.0`, and an unreleased one may carry a suffix such as `== 0.6.0.0 (unreleased)`.
SECTION_HEADING = re.compile(r"^==\s+(\S+)\s*(.*?)\s*$")

# Constructs that would render as visible garbage in a Markdown release body. Each entry is
# (compiled pattern, explanation shown to the operator).
UNSUPPORTED = [
    (re.compile(r"^\s*(-{4,}|={4,}|\*{4,}|_{4,}|\.{4,}|\+{4,}|/{4,})\s*$"),
     "delimited block (listing/example/sidebar/literal/passthrough/comment)"),
    (re.compile(r"^\s*\|===\s*$"), "table"),
    (re.compile(r"^\s*\|"), "table cell"),
    (re.compile(r"^\s*\[[^\]]*\]\s*$"), "block attribute list, e.g. [source,java]"),
    (re.compile(r"\[\["), "anchor"),
    (re.compile(r"^\s*(image|video|audio|include)::"), "block macro"),
    (re.compile(r"^\s*(ifdef|ifndef|ifeval|endif)::"), "preprocessor conditional"),
    (re.compile(r"^\s*:[A-Za-z0-9_!-]+:"), "document attribute"),
    (re.compile(r"xref:|<<[^<>]+>>"), "internal cross-reference"),
    (re.compile(r"footnote:"), "footnote"),
    (re.compile(r"^\s*toc::"), "toc macro"),
    (re.compile(r"^\s*'''\s*$"), "thematic break"),
    (re.compile(r"^\s*<<<\s*$"), "page break"),
]

# Not an AsciiDoc construct - a typo. An odd number of backticks leaves one monospace span
# open, so convert_inline masks the rest of the line as code and every emphasis marker after
# the stray backtick silently stops being converted. Same contract as the table above: a line
# that would ship mangled markup is an error, not a pass.
UNBALANCED_MONOSPACE = "odd number of ` - one monospace span is left open"

ADMONITIONS = ("NOTE", "TIP", "IMPORTANT", "WARNING", "CAUTION")

RE_COMMENT = re.compile(r"^\s*//")
RE_HEADING = re.compile(r"^(=+)\s+(.*)$")
RE_ULIST = re.compile(r"^(\*+)\s+(.*)$")
RE_OLIST = re.compile(r"^(\.+)\s+(.*)$")
RE_ADMONITION = re.compile(r"^(%s):{1,2}\s+(.*)$" % "|".join(ADMONITIONS))
RE_URL_MACRO = re.compile(r"(https?://[^\s\[\]]+)\[([^\]]*)\]")
RE_LINK_MACRO = re.compile(r"link:([^\s\[\]]+)\[([^\]]*)\]")
# AsciiDoc *constrained* bold: the opening `*` must not follow a word character and must not be
# followed by whitespace; the closing `*` must not follow whitespace nor precede a word character.
# Honouring those boundaries is what stops ordinary prose containing two bare asterisks (`3 * 4 * 5`)
# from being read as an emphasis span - a naive `\*{1,2}(...)\*{1,2}` mangles it into `3 ** 4 ** 5`.
# The backreference keeps `*x*` and `**x**` symmetric instead of pairing one delimiter with two.
RE_BOLD = re.compile(r"(?<![\w*])(\*{1,2})(?![\s*])(.+?)(?<![\s*])\1(?![\w*])")


class NoSection(Exception):
    pass


class Unsupported(Exception):
    def __init__(self, problems):
        super().__init__("unsupported AsciiDoc")
        self.problems = problems


def normalise_version(token):
    """`v0.5.2.2` and `0.5.2.2` name the same release."""
    return token[1:] if token.startswith("v") else token


def find_section(lines, version):
    """Return (heading_suffix, body_lines) for `version`, or raise NoSection.

    The section runs from its own `== <version>` heading to the next level-2 heading.
    Emptiness is NOT judged here - see `render`.
    """
    wanted = normalise_version(version)
    body, suffix, grabbing = [], "", False
    for line in lines:
        heading = SECTION_HEADING.match(line)
        if heading:
            if grabbing:
                break
            if normalise_version(heading.group(1)) == wanted:
                grabbing, suffix = True, heading.group(2)
            continue
        if grabbing:
            body.append(line)
    if not grabbing:
        raise NoSection("no `== %s` section in the changelog" % version)
    return suffix, body


def first_problem(line):
    """The one reason this line cannot be converted, or None."""
    for pattern, why in UNSUPPORTED:
        if pattern.search(line):
            return why
    if line.count("`") % 2:
        return UNBALANCED_MONOSPACE
    return None


def check_supported(body):
    # First match wins, deliberately: the patterns are allowed to overlap (`|===` is both a
    # table fence and a table cell), and an operator reading the failure wants one reason per
    # offending line, not one per pattern that happened to fire.
    problems = []
    for i, line in enumerate(body, start=1):
        why = first_problem(line)
        if why:
            problems.append((i, line.rstrip("\n"), why))
    if problems:
        raise Unsupported(problems)


def resolve_link(target, repo_url, ref):
    """Absolutise a repo-relative link macro target; leave real URLs alone."""
    if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", target):
        return target
    kind = "tree" if target.endswith("/") else "blob"
    return "%s/%s/%s/%s" % (repo_url.rstrip("/"), kind, ref, target.lstrip("/"))


def convert_prose(text, repo_url, ref):
    """Inline conversions, applied outside `monospace` spans only."""
    def url_macro(m):
        url, label = m.group(1), m.group(2)
        return "<%s>" % url if not label else "[%s](%s)" % (label, url)

    def link_macro(m):
        url = resolve_link(m.group(1), repo_url, ref)
        label = m.group(2) or m.group(1)
        return "[%s](%s)" % (label, url)

    text = RE_URL_MACRO.sub(url_macro, text)
    text = RE_LINK_MACRO.sub(link_macro, text)
    # AsciiDoc bold is *one* asterisk (**two** unconstrained); Markdown bold is two. One
    # asterisk in Markdown is italics, so leaving it alone would quietly change emphasis.
    text = RE_BOLD.sub(lambda m: "**%s**" % m.group(2), text)
    return text


def convert_inline(text, repo_url, ref):
    """The inline entry point: mask the monospace spans, then `convert_prose` the rest."""
    # Backticks delimit monospace in both languages; never rewrite what is inside one
    # (`bz.stub.parallelconsumer.*` must not be read as an emphasis marker). check_supported
    # has already rejected a line with an odd number of them, so the spans pair up here.
    parts = text.split("`")
    for i in range(0, len(parts), 2):
        parts[i] = convert_prose(parts[i], repo_url, ref)
    return "`".join(parts)


def convert_line(line, repo_url, ref):
    """Convert one line, or return None to drop it."""
    line = line.rstrip("\n")
    if RE_COMMENT.match(line):
        return None
    if line.strip() == "+":
        return ""  # list continuation: a blank line keeps the block with its item

    m = RE_HEADING.match(line)
    if m:
        return "%s %s" % ("#" * len(m.group(1)), convert_inline(m.group(2), repo_url, ref))

    m = RE_ADMONITION.match(line)
    if m:
        return "> **%s:** %s" % (m.group(1).capitalize(),
                                 convert_inline(m.group(2), repo_url, ref))

    m = RE_ULIST.match(line)
    if m:
        return "%s- %s" % ("  " * (len(m.group(1)) - 1),
                           convert_inline(m.group(2), repo_url, ref))

    m = RE_OLIST.match(line)
    if m:
        return "%s1. %s" % ("  " * (len(m.group(1)) - 1),
                            convert_inline(m.group(2), repo_url, ref))

    return convert_inline(line, repo_url, ref)


def to_markdown(body, repo_url, ref):
    converted = (convert_line(line, repo_url, ref) for line in body)
    out = [line for line in converted if line is not None]
    # Trim leading/trailing blanks only - `.strip()` would eat a nested bullet's indent.
    while out and not out[0].strip():
        out.pop(0)
    while out and not out[-1].strip():
        out.pop()
    return "\n".join(out) + "\n"


def render(text, version, repo_url, ref):
    """Return (markdown, heading_suffix). Never returns an empty body - see the module docstring."""
    suffix, body = find_section(text.splitlines(), version)
    check_supported(body)
    markdown = to_markdown(body, repo_url, ref)
    if not markdown.strip():
        raise NoSection("the `== %s` section produces no notes - it is empty, or holds only "
                        "`//` comments and `+` continuations" % version)
    return markdown, suffix


def default_changelog():
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "CHANGELOG.adoc")


class ArgumentParser(argparse.ArgumentParser):
    """argparse exits 2 on a bad argument, which is this script's "no section" code.

    Two failures sharing an exit code is one failure the release operator will misdiagnose,
    so a usage error is reported as EXIT_USAGE like every other operator mistake here.
    """

    def error(self, message):
        self.print_usage(sys.stderr)
        print("error: %s" % message, file=sys.stderr)
        sys.exit(EXIT_USAGE)


def main(argv):
    parser = ArgumentParser(
        description="Render a CHANGELOG.adoc section as Markdown release notes.")
    parser.add_argument("version", help="release version, e.g. 0.6.0.0")
    parser.add_argument("--changelog", default=None, help="path to CHANGELOG.adoc")
    parser.add_argument("--repo-url", default=DEFAULT_REPO_URL,
                        help="repository URL used to absolutise relative links")
    parser.add_argument("--ref", default=None,
                        help="git ref relative links resolve against (default: v<version>)")
    parser.add_argument("--strict", action="store_true",
                        help="fail if the heading is not frozen (still carries a suffix such "
                             "as `(unreleased)`); a real release passes this, a rehearsal does not")
    args = parser.parse_args(argv)

    path = args.changelog or default_changelog()
    # normalise_version, because `v0.5.2.2` and `0.5.2.2` are both accepted for the SAME release
    # (find_section says so) - and "v" + "v0.5.2.2" is a tag that does not exist, so every relative
    # link in the body would 404 while the notes themselves rendered perfectly.
    ref = args.ref or "v%s" % normalise_version(args.version)
    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except OSError as err:
        print("error: cannot read %s: %s" % (path, err), file=sys.stderr)
        return EXIT_USAGE

    try:
        notes, suffix = render(text, args.version, args.repo_url, ref)
    except NoSection as err:
        print("error: %s (%s). A release must not ship with an empty or auto-generated "
              "body - add or fill in the section, then re-run the release." % (err, path),
              file=sys.stderr)
        return EXIT_NO_SECTION
    except Unsupported as err:
        print("error: the %s section uses AsciiDoc this renderer does not convert, so the "
              "release body would be mangled. Fix the changelog or extend %s:"
              % (args.version, os.path.basename(__file__)), file=sys.stderr)
        for lineno, line, why in err.problems:
            print("  line %d of the section (%s): %s" % (lineno, why, line), file=sys.stderr)
        return EXIT_UNSUPPORTED

    if suffix:
        # The suffix never reaches the body - the heading is not rendered - so this is about the
        # changelog that gets tagged, not the release page. A rehearsal must not be blocked by it;
        # a real release must not tag a section still labelled unreleased, and a warning in a
        # 30-minute job log is not a check.
        problem = ("the `== %s` heading still carries %r - the released section must name the "
                   "version alone" % (args.version, suffix))
        if args.strict:
            print("error: %s. Drop the suffix in %s, commit it to master, then re-run the "
                  "release." % (problem, os.path.basename(path)), file=sys.stderr)
            return EXIT_NOT_FROZEN
        print("warning: %s." % problem, file=sys.stderr)

    sys.stdout.write(notes)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
