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
 * A CORPUS THAT SPANS EVERY DOCS AREA, holding every state the divergence header reports.
 *
 *   master            docs/inflight/note.md, docs/solutions/ci/sol.md, docs/plans/2026-01-01-001-plan.md,
 *                     and the feature records below
 *   master            docs/features/batching.yaml and pause.yaml - published records in two
 *                     categories; docs/features/no-title.yaml - a record whose `title:` key is
 *                     MISSING, which the title chain must report rather than paper over with the
 *                     filename; docs/features/staging/planned-thing.yaml - a staged record which
 *                     DECLARES ITSELF PUBLISHED, so both readers of that fact - the group and the
 *                     line's tail - have to agree that the directory wins
 *   adds-heading      note.md plus a new `## ...` section - a divergent version that ADDED A HEADING
 *   adds-line         note.md plus one plain line - a divergent version that added NO heading
 *   only-here         docs/inflight/branch-only.md and docs/features/branch-only-capability.yaml,
 *                     which master has never had - THE CASE THE WORKING TREE CANNOT ANSWER: a
 *                     session on master grepping for that capability finds nothing and concludes
 *                     nobody has specified it
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
/**
 * One feature record in the shape `docs/features/README.md` specifies - the keys the corpus reads
 * (`title`, `category`, `availability.status`) around the copyright comment every record carries.
 * `title` omitted writes a record that HAS none, which is a state the tool must report rather than
 * hide, so it is spelt as an absent key here rather than an empty string.
 */
export function feature({ title = null, category, status }) {
    return ['# Copyright (C) 2026 Antony Stubbs and contributors', '', 'schema_version: 1', 'kind: feature',
        ...(title === null ? [] : [`title: ${title}`]), `category: ${category}`, 'module: parallel-consumer-core',
        'availability:', `  status: ${status}`, 'summary: what it does, in one line.', ''].join('\n')
}

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
    // EVERY RECORD OPENS WITH THE COPYRIGHT COMMENT the real ones carry, because that line is what
    // makes a markdown title rule answer confidently and wrongly for this whole area.
    write('docs/features/batching.yaml', feature({ title: 'Batch processing', category: 'processing', status: 'published' }))
    write('docs/features/pause.yaml', feature({ title: 'Pause and resume', category: 'operability', status: 'published' }))
    write('docs/features/no-title.yaml', feature({ category: 'processing', status: 'published' }))
    // DECLARES `published` ON PURPOSE, and that is the whole point of it: a staged record whose own
    // status contradicts the directory it sits in is the shape the index rendered as `_published_`
    // under a heading saying "not settled in the tree yet". A staged record that agreed with its
    // directory could not have caught it, and the live corpus holds one that does not agree.
    write('docs/features/staging/planned-thing.yaml', feature({ title: 'A capability not settled yet', category: 'integration', status: 'published' }))
    commit('the corpus')

    git('checkout', '-q', '-b', 'adds-heading')
    write('docs/inflight/note.md', `${NOTE}\n## What the branch learned\n\ndetail\n`)
    commit('add a heading')

    git('checkout', '-q', '-b', 'adds-line', 'master')
    write('docs/inflight/note.md', `${NOTE}one plain added line\n`)
    commit('add a line')

    git('checkout', '-q', '-b', 'only-here', 'master')
    write('docs/inflight/branch-only.md', '# Only here\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\nz\n')
    write('docs/features/branch-only-capability.yaml', feature({ title: 'A capability only this branch records', category: 'operability', status: 'planned' }))
    commit('a note and a feature record master never had')

    git('checkout', '-q', '-b', 'to-tag', 'master')
    write('docs/inflight/note.md', `${NOTE}parked before a re-cut\n`)
    commit('parked')
    git('tag', 'preserved/parked')
    git('checkout', '-q', 'master')
    git('branch', '-q', '-D', 'to-tag')
    return { dir, git, commit, write, NOTE }
}

/**
 * THE CORPUS FIXTURE PLUS THE SHAPES THE PROMPT-KEYWORD QUERY IS SPECIFIED AGAINST, one per tier
 * of the plan's R10 order, each under a name no other fixture document uses:
 *
 *   terms-only        docs/solutions/ci/retry-queue.md, whose frontmatter names `RetryQueueDrainer`
 *                     in `related_components:` and whose body never does - a FRONTMATTER hit that
 *                     master has never had
 *   master            docs/plans/2026-02-02-001-widget.md, naming `WidgetSpinner` in a `##` heading
 *                     only - a HEADING hit
 *   master            docs/inflight/flux-1.md .. flux-5.md, naming `flux_capacitor` in body prose
 *                     only - BODY hits, more of them than the per-term cap keeps
 *   master            docs/inflight/gadget-01.md .. gadget-14.md, each carrying an
 *                     `<!-- inflight-impact: GadgetFlange -->` marker - more frontmatter-tier hits
 *                     than the hook shows, so the `+N more` tail is reachable
 *   master            docs/inflight/issue-41.md naming `#41` and docs/inflight/issue-419.md naming
 *                     `#419` - one issue number a prefix of the other, so the fixed-string grep
 *                     finds both for `#41` and the attribution has to tell them apart
 *
 * Built on top of `buildDocsFixture` rather than inside it, for the reason that function gives:
 * the drift checks assert exact ref and version counts on the shared corpus, and a fifth live ref
 * would move them.
 */
export function buildTermsFixture() {
    const fx = buildDocsFixture()
    const { git, commit, write } = fx
    write('docs/plans/2026-02-02-001-widget.md', '# A rollout plan\n\n## The WidgetSpinner rollout\n\nsteps\n')
    for (let i = 1; i <= 5; i++) {
        write(`docs/inflight/flux-${i}.md`, `# Flux note ${i}\n\n<!-- inflight-type: task -->\n\nthe flux_capacitor stalls here\n`)
    }
    for (let i = 1; i <= 14; i++) {
        const nn = String(i).padStart(2, '0')
        write(`docs/inflight/gadget-${nn}.md`, `# Gadget ${nn}\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: GadgetFlange -->\nbody\n`)
    }
    // issue-refs: exempt-begin
    write('docs/inflight/issue-41.md', '# Issue forty-one\n\n<!-- inflight-type: task -->\n\ncloses #41 for good\n')
    write('docs/inflight/issue-419.md', '# Issue four-nineteen\n\n<!-- inflight-type: task -->\n\ntracked as #419\n')
    // issue-refs: exempt-end
    commit('documents for the prompt-keyword query')

    git('checkout', '-q', '-b', 'terms-only', 'master')
    write('docs/solutions/ci/retry-queue.md', [
        '---', 'title: The retry queue drained twice', 'related_components:', '  - RetryQueueDrainer', '---',
        '# The retry queue drained twice', '', 'the drainer ran once per shard and once per partition', '',
    ].join('\n'))
    commit('a solution master never had')
    git('checkout', '-q', 'master')
    return fx
}
