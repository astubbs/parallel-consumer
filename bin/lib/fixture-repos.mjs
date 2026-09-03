// Copyright (C) 2026 Antony Stubbs and contributors
//
// Throwaway git repositories for the self-tests - the shared half of what bin/test-inflight.mjs and
// bin/test-check-docs-hooks.mjs both need. Lifted out when the second suite arrived, so the corpus
// fixture the divergence header is specified against exists once: two copies of "what a divergent
// note looks like" would be two definitions of the thing the header reports.
//
// NOT USED BY THE TOOL. Nothing under bin/lib/ that answers a question imports this; it is here
// because bin/lib/ is where the two suites already share code, and because the library rule that
// nothing here exits the process applies to a fixture builder as much as to a query.

import { spawnSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'

/** A `git` runner pinned to one directory that throws on failure - a fixture that half-built is worse than none. */
export function windowGit(dir) {
    return (...args) => {
        const r = spawnSync('git', args, { cwd: dir, encoding: 'utf8' })
        if (r.status !== 0) throw new Error(`fixture: git ${args.join(' ')} failed: ${r.stderr}`)
        return r.stdout.trim()
    }
}

/** An empty repository on `master`, with a `commit(message)` that stages everything first. */
export function windowRepo() {
    const dir = mkdtempSync(join(tmpdir(), 'inflight-window-'))
    const git = windowGit(dir)
    git('init', '-q', '-b', 'master')
    return { dir, git, commit: (m) => { git('add', '-A'); git('-c', 'user.email=t@t', '-c', 'user.name=t', 'commit', '-q', '-m', m) } }
}

/**
 * A CORPUS THAT SPANS THE THREE DOCS AREAS, holding every state the divergence header reports.
 *
 *   master            docs/inflight/note.md, docs/solutions/ci/sol.md, docs/plans/2026-01-01-001-plan.md
 *   adds-heading      note.md plus a new `## ...` section - a divergent version that ADDED A HEADING
 *   adds-line         note.md plus one plain line - a divergent version that added NO heading
 *   only-here         docs/inflight/branch-only.md, which master has never had
 *   tag preserved/parked
 *                     note.md with content no live ref carries - its branch was deleted after
 *                     tagging, which is how this repository parks work before a re-cut
 *
 * ITS OWN REPOSITORY, NOT test-inflight's SHARED FIXTURE. The drift checks there assert exact counts
 * on shared.md (`divergent.length === 1`), and the mutant phase re-runs every check against
 * whatever the earlier ones left behind - so growing that note's divergent set here would turn an
 * unrelated check red one phase later, with nothing pointing back at the cause.
 *
 * Returns the repository with `master` checked out and the `only-here` note absent from the working
 * tree; a caller that needs that note on disk adds a worktree for its branch.
 */
export function buildDocsFixture() {
    const { dir, git, commit } = windowRepo()
    const write = (rel, body) => {
        mkdirSync(join(dir, dirname(rel)), { recursive: true })
        writeFileSync(join(dir, rel), body)
    }
    const NOTE = '# The note\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\nbody\n'
    write('docs/inflight/note.md', NOTE)
    write('docs/solutions/ci/sol.md', '# A solved problem\n\nfixed\n')
    write('docs/plans/2026-01-01-001-plan.md', '# A plan\n\nsteps\n')
    commit('the corpus')

    git('checkout', '-q', '-b', 'adds-heading')
    write('docs/inflight/note.md', `${NOTE}\n## What the branch learned\n\ndetail\n`)
    commit('add a heading')

    git('checkout', '-q', '-b', 'adds-line', 'master')
    write('docs/inflight/note.md', `${NOTE}one plain added line\n`)
    commit('add a line')

    git('checkout', '-q', '-b', 'only-here', 'master')
    write('docs/inflight/branch-only.md', '# Only here\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\nz\n')
    commit('a note master never had')

    git('checkout', '-q', '-b', 'to-tag', 'master')
    write('docs/inflight/note.md', `${NOTE}parked before a re-cut\n`)
    commit('parked')
    git('tag', 'preserved/parked')
    git('checkout', '-q', 'master')
    git('branch', '-q', '-D', 'to-tag')
    return { dir, git, commit, write, NOTE }
}
