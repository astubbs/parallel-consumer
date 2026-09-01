// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE PATTERN TABLE. Adding a rule to this repo should be adding a row here, not adding a script.
//
// WHY THIS FILE EXISTS
//
// bin/ accumulated a gate per rule, and most of them are the same program: walk some files, match a
// regex, complain. check-shell-sigpipe.sh WAS one grep, and is now the `sigpipe-into-grep-q` row below.
// check-branch-self-reference.sh is one grep with an opt-out marker. Each re-implements file walking,
// exclusion, an opt-out, an exit-code
// contract and a failure message, in a language where each of those is a paragraph - and each one is
// a place for those five things to be subtly different from its neighbours.
//
// So: one runner, many rows. A new rule is a `{ id, why, files, forbid?, requires?, allowIf?, fix }`
// object. It inherits the walking, the marker handling, the exit codes and the reporting, and it
// cannot drift from its neighbours because there is only one implementation of all of that.
//
// CHECK WHAT WE ALREADY RUN BEFORE ADDING A ROW. This repo already has ShellCheck, SpotBugs with
// fb-contrib/findsecbugs/findbugs-slf4j, Infer, forbiddenapis, ArchUnit and CodeQL. A rule that one of
// them covers does not belong here at any price - a second implementation of somebody else's check is
// a wheel, and it will disagree with theirs eventually.
//
// Two rules were written for this table and then deleted before it shipped, which is the standard: they
// flagged gawk's ENDFILE under mawk, and awk's reserved function names used as variables. Both were
// real - each had bitten this session - but each was generalised from a single incident, into policing
// a language now frozen for new work. A bespoke linter for the language you are leaving is the clearest
// possible case of reinventing a wheel nobody needs to roll.
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

/**
 * A rule. `forbid` is OPTIONAL: omit it when the file's mere existence is the violation, which is
 * what `new-shell-script` means. It was written as `forbid: /^/` at first - a regex that matches
 * everything, used as a sentinel - and that is the kind of cleverness the next person adding a row
 * has to decode before they can copy it.
 *
 * @typedef {{id: string, why: string, files: RegExp, forbid?: RegExp, requires?: RegExp,
 *            allowIf?: RegExp, fix: string, scope?: 'added-files'}} Rule
 */

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
    allowIf: /shell-justified:\s*\S/,
    fix: 'Write it as .mjs, or state why shell is right: # shell-justified: <reason>',
  },
  {
    id: 'sigpipe-into-grep-q',
    why: 'Under `set -o pipefail`, `writer | grep -q PATTERN` INVERTS its own answer. grep -q exits the '
       + 'instant it matches, the writer takes EPIPE and dies with 141, and pipefail promotes that to '
       + 'the pipeline status - so a MATCH reports failure. It hides well: the writer only gets that far '
       + 'with more than one pipe buffer still to write, so small inputs pass forever. It shipped in '
       + 'check-review-posted.sh, which reported "no review posted" on four PRs whose reviews had posted. '
       + 'ShellCheck does not catch it - verified against the known-bad line. Migrated from '
       + 'bin/check-shell-sigpipe.sh, whose own header said it was a hazard category rather than a gate.',
    files: /\.(sh|bash)$/,
    requires: /set -.*pipefail/,
    // LINE-ANCHORED, and the `(?![ \t]*#)` is load-bearing. A whole-file regex without it flagged
    // thirteen files the shell gate passes, because most of them only MENTION the hazard in a comment -
    // including the gate being replaced and its own self-test. The shell version got this by running
    // line-oriented and piping through a second `grep -v` for comments; a single pattern has to carry
    // both halves, and finding that out is why one real rule was migrated instead of shipping the
    // table with only invented ones.
    //
    // `[^|]\|` keeps `||` out: a logical-or is not a pipeline.
    forbid: /^(?![ \t]*#)[^\n]*[^|]\|[ \t]*grep(?:[ \t]+-[a-zA-Z-]+)*[ \t]+(?:-[a-zA-Z]*q|--quiet|--silent)/m,
    fix: 'Use a herestring: grep -q PATTERN <<<"$data". No pipeline, so no SIGPIPE for pipefail to promote.',
  },
]
