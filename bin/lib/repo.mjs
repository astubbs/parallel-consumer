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

/** Owner/name as `gh` wants it. NEVER omit it from a `gh` call - see the note in notes.mjs. */
export const REPO = 'astubbs/parallel-consumer'

/** Where in-flight notes live, relative to the repository root. */
export const NOTES_DIR = 'docs/inflight'

/**
 * THE THREE CORPUS AREAS, in the order `prior-art` has always numbered them.
 *
 * This was the section list hard-coded inside prior-art.mjs, and ci-inflight-next-commands.md
 * named it as the one fact to lift before it spread - which it was about to: the context query
 * indexes the same three directories, the docs shape groups by them, and the session index renders
 * them. Three private copies of a three-row table is the REPO defect again, one row wider.
 *
 * `dir` has no trailing slash - callers add one where git wants a pathspec. `name` is the display
 * half, and `prior-art`'s headings are built from it verbatim, so renaming one here changes output
 * a reader has learned to scan; the self-test pins the headings for that reason.
 */
export const DOC_AREAS = [
    { dir: 'docs/plans', name: 'Prior investigations' },
    { dir: 'docs/solutions', name: 'Solved problems' },
    { dir: NOTES_DIR, name: 'In-flight state' },
]
