// Copyright (C) 2026 Antony Stubbs and contributors
//
// WHAT PLAYS THE PART OF A HEADING, in a corpus that holds prose and data records side by side.
//
// A DATA RECORD HAS KEYS WHERE A DOCUMENT HAS HEADINGS, and every reader that did not know it
// produced the SAME wrong answer, confidently: a record's only `# ` line is its copyright comment,
// so "Copyright (C) 2026 Antony Stubbs and contributors" came back as the title, or as the heading,
// of every version of every file under `docs/features/`.
//
// THAT WAS PATCHED THREE TIMES IN THREE PLACES before this file existed - the title chain in
// bin/lib/inflight-tags.mjs, the batched title read in bin/lib/notes.mjs, and the divergence
// preview beside it - and a fourth reader in bin/lib/terms.mjs was still classing a record's
// copyright comment as a HEADING. That one cost more than a wrong title: `heading` outranks `body`
// in the prompt query's tiers, so a word appearing in any record's comment surfaced a block of
// identically-titled records ABOVE the records that matched on substance.
//
// Three patches for one shape is the shape asking for an owner. This is it: one predicate, and the
// patterns that follow from it in both spellings the tool needs - JavaScript for the readers, ERE
// for `git grep -E`, which reads neither `\s` nor `\w`. A fifth reader asks here rather than
// re-deriving, and a reader that forgets is now a missing import rather than a silent wrong answer.
//
// NO GIT, NO FILESYSTEM, NO PRINTING, and no imports: the leaf every other library may take.

/** A path whose content is a data record rather than prose. Both spellings, because both are in use. */
export const RECORD_FILE_RE = /\.ya?ml$/

/**
 * Is the document at this path a data record? A null or absent path is NOT a record - a caller that
 * does not know what it is holding gets the prose rule, which is the corpus default everywhere else.
 */
export const isRecord = (path) => typeof path === 'string' && RECORD_FILE_RE.test(path)

/** A markdown heading line, at any level: what a prose document's table of contents is made of. */
export const PROSE_HEADING_RE = /^#{1,6}\s/

/**
 * A record's own table of contents: its TOP-LEVEL keys, column-0 anchored.
 *
 * Top-level only, and deliberately. A nested key is a field OF an entry - `  status:` inside
 * `availability:` - where a top-level key is a section of the record, which is what a heading is.
 * The same rule the divergence preview has always used, now in one place instead of two.
 */
export const RECORD_KEY_RE = /^[A-Za-z_][\w.-]*:/

/** A `#` line inside a record is a COMMENT - in this repository, always the copyright header. */
export const RECORD_COMMENT_RE = /^\s*#/

/** Which of the two rules answers "is this line a heading" for the document at `path`. */
export const headingRe = (path) => (isRecord(path) ? RECORD_KEY_RE : PROSE_HEADING_RE)

/**
 * The word for one of them, for a renderer: "no key added" about a record, "no heading added" about
 * a document. A sentence a reader has to decode before discarding is a sentence that cost something.
 */
export const headingWord = (path) => (isRecord(path) ? 'key' : 'heading')

// --- The same two rules as POSIX ERE, for `git grep -E`. ----------------------------------------
//
// SPELT OUT RATHER THAN DERIVED from the regexes above. `RegExp.source` would hand git `\s` and
// `\w`, which are GNU extensions rather than POSIX ERE: the regex backend git is built against here
// accepts them, and one built against a stricter library does not. So a derived pattern works on
// this box and matches NOTHING elsewhere - and in this tool "matched nothing" renders as a finding,
// which is the one failure mode the whole file guards. (Checked, rather than assumed: `git grep -E
// '^#{1,6}\s'` really does match here. That is what makes the trap silent.)
//
// One rule in two spellings has no compiler keeping them together, so the prose pair is pinned
// BEHAVIOURALLY: bin/test-inflight.mjs runs the ERE through `git grep -E` over a file of edge-case
// lines and asserts it selects exactly the lines the JavaScript twin selects. Two spellings in two
// files could not be pinned at all.

/** ERE: a markdown heading line. POSIX class, never `\s`. */
export const PROSE_HEADING_ERE = '^#{1,6}[[:space:]]'

/**
 * ERE: A LINE THE AUTHOR OF A RECORD DECLARED SOMETHING ON - anything whose first non-blank
 * character is not `#`. This is the record's answer to "what is this document ABOUT", and it is
 * nearly the whole file on purpose.
 *
 * WHY IT IS NOT `RECORD_KEY_ERE`, which is what "the record's own table of contents" would suggest,
 * and the distinction is the point of having one file own both. A prose document is mostly body, and
 * its headings are a short index OF that body - so keeping the headings and dropping the rest
 * removes noise. A record has no body: every line is a field the author declared or a value they
 * declared for one. Keeping only the top-level keys would therefore drop real claims rather than
 * noise: the prose of a record lives in folded block scalars (`summary: >`) and in list items, both
 * of which are indented under the key they belong to, so the key rule misses records that genuinely
 * carry the mechanism. Compare the two for yourself on any broad term -
 * `bin/inflight.mjs prior-art --headings <term>` against the same run without the flag; records
 * silently missing, under a heading that looks like a completed search, is the failure this mode was
 * fixed for, one size smaller.
 *
 * What it DOES drop is the only line in a record that is not a claim: the copyright comment - the
 * line every reader in this file's header mistook for the record's title. So both kinds keep exactly
 * the lines their author wrote as claims, which is the one rule stated twice in two shapes.
 */
export const RECORD_CLAIM_ERE = '^[[:space:]]*[^#[:space:]]'
