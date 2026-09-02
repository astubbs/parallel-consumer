// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE FACTS THAT ARE ABOUT THIS REPOSITORY RATHER THAN ABOUT GIT.
//
// Everything else under bin/lib/ is generic - `git.mjs` wraps plumbing, `cache.mjs` stores network
// answers, `notes.mjs` reads a corpus. These two constants are the only things that would have to
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
