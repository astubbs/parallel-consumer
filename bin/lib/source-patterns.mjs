// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE PATTERN TABLE. Adding a rule to this repo should be adding a row here, not adding a script.
//
// WHY THIS FILE EXISTS
//
// bin/ accumulated a gate per rule, and most of them are the same program: walk some files, match a
// regex, complain. check-shell-sigpipe.sh is one grep. check-branch-self-reference.sh is one grep with
// an opt-out marker. Each one re-implements file walking, exclusion, an opt-out, an exit-code
// contract and a failure message, in a language where each of those is a paragraph - and each one is
// a place for those five things to be subtly different from its neighbours.
//
// So: one runner, many rows. A new rule is a `{ id, why, files, forbid, allowIf, fix }` object. It
// inherits the walking, the marker handling, the exit codes and the reporting, and it cannot drift
// from its neighbours because there is only one implementation of all of that.
//
// WHAT DOES NOT BELONG HERE. A check that has to *think* - parse XML, call an API, compare numbers,
// know about git history beyond a diff - is a real program and gets its own file. This is for the
// "grep with a reason attached" family, which is most of them, and the honesty test is whether the
// rule fits in a regex without contortion. A pattern with three lookaheads and a negative class is a
// program wearing a regex as a disguise.
//
// EVERY ROW MUST CARRY `why`. A rule whose reason is not written down is one nobody can argue with,
// and it survives long after the thing it guarded stopped mattering - which is how a linter becomes
// noise people learn to skip.

/** @typedef {{id: string, why: string, files: RegExp, forbid: RegExp, allowIf?: RegExp, fix: string, scope?: 'added-files'}} Rule */

/** @type {Rule[]} */
export const RULES = [
  {
    id: 'new-shell-script',
    why: 'Node is the default for new scripts in bin/ (operator ruling, 2026-09-01). Shell here has '
       + 'produced silent wrong answers - a gate written with gawk ENDFILE parsed under mawk, matched '
       + 'nothing, and printed success over the defect it was written to catch - and two entire gates '
       + 'exist only to police shell traps. Existing scripts are grandfathered; this is about what is NEW.',
    scope: 'added-files',
    files: /^bin\/.*\.sh$/,
    forbid: /^/,                        // the file existing at all is the violation
    allowIf: /shell-justified:\s*\S/,
    fix: 'Write it as .mjs, or state why shell is right: # shell-justified: <reason>',
  },
  {
    id: 'awk-endfile',
    why: 'ENDFILE is a gawk extension. The default awk on Debian and Ubuntu is mawk, which parses the '
       + 'program, silently never runs the block, and exits 0 - so the check passes everything. This '
       + 'happened here, to a gate written to catch a specific defect, over a file containing it.',
    files: /\.(sh|bash)$/,
    forbid: /^\s*ENDFILE\s*\{/m,
    fix: 'One awk invocation per file with END, or write it in Node.',
  },
  {
    id: 'awk-reserved-exp',
    why: '`exp` is a built-in awk function, so using it as a variable is a syntax error - and one that '
       + 'only shows when the line is reached, which for a gate can be long after it was written.',
    files: /\.(sh|bash)$/,
    // `=(?!=)` and not `=`: `if (exp == 3)` is a comparison, not an assignment, and the first
    // draft flagged it. Caught by the must-NOT-match half of this rule's self-test, which is
    // the half that exists for exactly this.
    forbid: /awk[^\n]*\b(exp|log|int|split|index|length|substr)\s*=(?!=)/,
    fix: 'Rename the variable. Node has no reserved-word trap of this shape.',
  },
]
