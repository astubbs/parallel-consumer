// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE FACTS THAT ARE ABOUT THIS REPOSITORY RATHER THAN ABOUT GIT.
//
// Everything else under bin/lib/ is generic - `git.mjs` wraps plumbing, `cache.mjs` stores network
// answers, `notes.mjs` reads a corpus. These constants are the only things that would have to
// change to point the tool at a different project, and `ci-inflight-next-commands.md` names
// gathering them as the one concrete thing the extract-as-FOSS direction already argues for.
//
// IT WAS WRITTEN THREE TIMES. `REPO` was declared identically in notes.mjs, branches.mjs and
// prior-art.mjs, while the note above said it was "a single constant today". Found by the
// same-defect sweep at merge prep, looking for the class behind a different bug: the known-cache
// list, also written twice, which drifted and made `cache` report the live file as an ORPHAN and
// the dead file as live. A copied constant is correct until exactly one copy changes, and nothing
// goes red at that moment.

import { RECORD_CLAIM_ERE, RECORD_FILE_RE } from './doc-kind.mjs'

/** Owner/name as `gh` wants it. NEVER omit it from a `gh` call - see the note in notes.mjs. */
export const REPO = 'astubbs/parallel-consumer'

/** Where in-flight notes live, relative to the repository root. */
export const NOTES_DIR = 'docs/inflight'

/** Where the feature records live - one YAML file per user-visible capability. */
export const FEATURES_DIR = 'docs/features'

/**
 * THE CORPUS AREAS, in the order `prior-art` has always numbered them.
 *
 * This was the section list hard-coded inside prior-art.mjs, and ci-inflight-next-commands.md
 * named it as the one fact to lift before it spread - which it was about to: the context query
 * indexes the same directories, the docs shape groups by them, and the session index renders them.
 * Three private copies of one table is the REPO defect again, a row wider - and each row this table
 * gains is a row those three would each have had to gain by hand.
 *
 * `dir` has no trailing slash - callers add one where git wants a pathspec. `name` is the display
 * half, and `prior-art`'s headings are built from it verbatim, so renaming one here changes output
 * a reader has learned to scan; the self-test pins the headings for that reason.
 *
 * `documents` is the area's own rule for which files in it are documents, for an area whose records
 * are not prose - `docs/features/` holds YAML, and the shared markdown rule would have skipped every
 * one of them while reporting the area as empty. Omitted means the corpus default, `DOCUMENT_RE` in
 * bin/lib/docs-shape.mjs.
 *
 * `headings` is the same idea for `prior-art --headings`: the ERE for a line that carries what this
 * area's documents are ABOUT, as opposed to a line a word merely appears on. The markdown-heading
 * rule matched no record on any ref, so this area reported `nothing, across 604 refs` - in the mode
 * AGENTS.md tells agents to reach for first, phrased as the sentence it tells them to read as a
 * completed check. Omitted means the prose default; `null` means the area has no such distinction to
 * draw, and `prior-art` then SAYS SO for that section rather than printing an emptiness it never
 * searched for. bin/lib/doc-kind.mjs owns both patterns and states why a record's is not its keys.
 *
 * FEATURES IS APPENDED, NEVER INSERTED. The numbering `prior-art` prints is this table's index, and
 * a reader has learned that 1 to 3 are plans, solutions and notes; adding a row in the middle would
 * renumber all of them to say nothing new. Appending also puts the area last in the shape and the
 * index, which is where it belongs by question: the other three record what was investigated, what
 * was settled and what is open, and this one records what the product DOES.
 */
export const DOC_AREAS = [
    { dir: 'docs/plans', name: 'Prior investigations' },
    { dir: 'docs/solutions', name: 'Solved problems' },
    { dir: NOTES_DIR, name: 'In-flight state' },
    { dir: FEATURES_DIR, name: 'Shipped and planned capability', documents: RECORD_FILE_RE, headings: RECORD_CLAIM_ERE },
]
