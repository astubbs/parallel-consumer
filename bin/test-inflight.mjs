#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Self-test for bin/inflight.mjs and bin/lib/prior-art.mjs.
//
// EVERY ASSERTION HERE IS NEGATIVE-CONTROLLED. bin/AGENTS.md: "a regression test that has never
// failed proves nothing", and the suite it says that about had shipped with a `fails` counter the
// exit check never read - so it printed FAIL and exited 0. Each check below is therefore run twice:
// once against the real tree, and once against a copy mutated to break exactly the thing it asserts.
// A check that stays green against its own mutant is reported as a FAILURE of the suite.
//
// IT ASSERTS ON THE RESULT, NOT ON STDOUT. The first cut of this suite grepped the formatted page,
// because priorArt() returned an exit code and printed as it went. A test that reads its own output
// cannot tell a missing hit from a reworded heading, and it cannot check anything the formatter does
// not happen to print. Returning findings is what makes `sections-do-not-overlap` and
// `ref-clusters-are-deduplicated` expressible at all - both are statements about data.
//
// THE FIVE CHECKS COVERING git.mjs AND notes.mjs WERE WRITTEN, REPORTED AS PASSING, AND WERE NOT IN
// THE FILE. The edit that added them anchored on text that did not match and said nothing; the suite
// then reported "All 22 self-test(s) passed" over a set that had never contained them, so three
// commands shipped with zero coverage while their author believed otherwise. It is the exact failure
// this file exists to prevent, committed inside it, and it surfaced only because a reviewer noticed
// two module loaders that nothing called. Any edit adding a check here must VERIFY the check is in
// the file afterwards - `grep -c "^        id: '"` is the cheap version - because an edit that
// reports success is not evidence that it landed.
//
// TWO OF THESE ARE REGRESSION TESTS FOR BUGS THIS FILE FOUND:
//   - sections-do-not-overlap: section 4's pathspec was `docs/*.md`, and `*` crosses `/` in git's
//     wildmatch, so "everything else under docs/" silently re-listed every plan, solution and note.
//   - ref-clusters-are-deduplicated: the consequence, one note counted as two paths in one cluster.
//
// Network: none. priorArt is called with `github: false` - the gh rate limit is shared with every
// parallel session here, and a self-test that can fail because someone else was busy is a flake.
//
// EXIT CODES: 0 all checks passed and every mutant went red; 1 otherwise.

import { spawnSync } from 'node:child_process'
// The fixture repositories are shared with bin/test-check-docs-hooks.mjs; bin/lib/fixture-repos.mjs owns them.
import { buildDocsFixture, buildTermsFixture, windowGit, windowRepo } from './lib/fixture-repos.mjs'
import { chdir, cwd } from 'node:process'
import {
    cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, symlinkSync, utimesSync,
    writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { mkdtempSync as mkTmp } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const BIN = dirname(fileURLToPath(import.meta.url))

/**
 * A PURPOSE-BUILT GIT REPOSITORY, because the corpus checks were testing the author's laptop.
 *
 * Every check that reads notes across refs used to run against whatever repository happened to be
 * the working directory. On a developer clone with 436 refs they passed; in CI, where
 * `actions/checkout` fetches ONE branch and there is no `origin/master`, nine of them failed - so
 * the suite that gates this tool was contributing no CI signal at all while appearing green locally.
 * Reproduced with `git clone --single-branch` before this was written.
 *
 * The fixture encodes exactly the situations the checks assert about, so each one now has a known
 * right answer instead of an ambient one:
 *
 *   master          shared.md v1 -> v2, plus closed.md added and then `git rm`d (baseline HISTORY)
 *   behind          shared.md still at v1 - a version master ITSELF once held, so: behind, not drift
 *   diverged        shared.md with content master has never held - the actual finding
 *   stranded-work   never-landed.md, which master has never had at all
 *   reuse           closed.md recreated with DIFFERENT content at a path master closed - the
 *                   filename-reuse false negative, which the path-only filter used to swallow
 */
function buildFixture() {
    const dir = mkdtempSync(join(tmpdir(), 'inflight-fixture-'))
    const git = (...args) => {
        const r = spawnSync('git', args, { cwd: dir, encoding: 'utf8' })
        if (r.status !== 0) throw new Error(`fixture: git ${args.join(' ')} failed: ${r.stderr}`)
        return r.stdout.trim()
    }
    const note = (name, body) => {
        mkdirSync(join(dir, 'docs', 'inflight'), { recursive: true })
        writeFileSync(join(dir, 'docs', 'inflight', name), body)
    }
    const commit = (msg) => { git('add', '-A'); git('-c', 'user.email=t@t', '-c', 'user.name=t', 'commit', '-q', '-m', msg) }

    git('init', '-q', '-b', 'master')
    note('shared.md', '# Shared note\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: stall -->\nv1\n')
    commit('v1')
    const v1 = git('rev-parse', 'HEAD')
    note('shared.md', '# Shared note\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: stall -->\nv2 on master\n')
    commit('v2')
    note('closed.md', '# A note that will close\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\ntopic A\n')
    // A workstream note on the baseline, so the remedy has an existing owner to point at rather
    // than a filename to invent - the case that sent the first real remedy to the wrong file.
    note('branch-feature-workstream.md',
        '# The feature workstream\n\n<!-- inflight-type: feature -->\n<!-- inflight-impact: coordination -->\nsignpost\n')
    commit('add closed.md and the workstream note')
    git('rm', '-q', 'docs/inflight/closed.md')
    commit('close it, per the directory contract')

    git('branch', 'behind', v1)

    git('checkout', '-q', '-b', 'diverged', v1)
    note('shared.md', '# Shared note\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: stall -->\nv1\ncontent master has never held\n')
    commit('diverge')

    // TWO stranded notes on ONE branch, so they share a ref-set and must collapse to one cluster.
    git('checkout', '-q', '-b', 'stranded-work', 'master')
    note('never-landed.md', '# Never landed\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: stall -->\nx\n')
    note('also-never-landed.md', '# Also never landed\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: stall -->\ny\n')
    commit('two notes that never reach master')

    // Carries the CLOSED note's original content at its original path - master had both, so this is
    // finished work, not stranded work. It is what the baseline-history filter exists to exclude,
    // and without it in the fixture that filter's negative control had nothing to catch.
    git('checkout', '-q', '-b', 'stale-closed', v1)
    note('closed.md', '# A note that will close\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\ntopic A\n')
    commit('still carrying the note master has since closed')

    git('checkout', '-q', '-b', 'reuse', 'master')
    note('closed.md', '# A DIFFERENT topic at a recycled filename\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: stall -->\ntopic B\n')
    commit('reuse the closed filename for unrelated work')

    // A REAL PARENT/CHILD PAIR, both off master. `behind` cannot serve: it points at a commit the
    // baseline already contains, so it has no work of its own and is not a meaningful parent - the
    // first version of these checks used it and failed, which is the fixture earning its keep.
    git('checkout', '-q', '-b', 'feature-a', 'master')
    note('feature-a.md', '# Feature A\n\n<!-- inflight-type: feature -->\n<!-- inflight-impact: ci -->\na\n')
    commit('feature a')
    git('checkout', '-q', '-b', 'feature-int', 'feature-a')
    note('feature-b.md', '# Feature B\n\n<!-- inflight-type: feature -->\n<!-- inflight-impact: ci -->\nb\n')
    commit('integrate a, add b')

    git('checkout', '-q', 'master')
    return dir
}

/**
 * A CLONE WITH A REAL REMOTE, because freshness is a statement about fetching and the fixture above
 * has nothing to fetch from. Returns the clone's path; what gets fetched is left to the caller,
 * because the width of that fetch is the variable under test.
 */
function buildFetchFixture() {
    const root = mkdtempSync(join(tmpdir(), 'inflight-fetch-'))
    const run = (where, ...args) => {
        const r = spawnSync('git', args, { cwd: where, encoding: 'utf8' })
        if (r.status !== 0) throw new Error(`fetch fixture: git ${args.join(' ')} failed: ${r.stderr}`)
        return r.stdout.trim()
    }
    const up = join(root, 'up')
    mkdirSync(up, { recursive: true })
    run(up, 'init', '-q', '-b', 'master')
    writeFileSync(join(up, 'f'), 'one\n')
    run(up, 'add', '-A')
    run(up, '-c', 'user.email=t@t', '-c', 'user.name=t', 'commit', '-q', '-m', 'one')
    // Enough refs that ONE of them is unambiguously a narrow fetch of the set.
    for (let i = 0; i < 12; i += 1) run(up, 'branch', `b${i}`)
    const down = join(root, 'down')
    run(root, 'clone', '-q', up, down)
    return down
}

let FIXTURE = null
const fixture = () => (FIXTURE ??= buildFixture())

/** Run a predicate with the fixture as the working directory, so the libraries read it. */
async function inFixture(fn) {
    const before = cwd()
    chdir(fixture())
    try { return await fn(fixture()) } finally { chdir(before) }
}

let failures = 0
const report = (ok, label) => {
    console.log(`${ok ? '  ok  ' : '  FAIL'} ${label}`)
    if (!ok) failures++
}

/** A command that RAN, whatever it found - exit 0. `could not run` is 2. */
const r0 = (r) => r.code === 0

/** Run a front door (real or mutant) as a subprocess - the CLI contract is a process-level fact. */
function invoke(binDir, args, opts = {}) {
    const r = spawnSync(process.execPath, [join(binDir, 'inflight.mjs'), ...args], { encoding: 'utf8', ...opts })
    return { code: r.status, out: `${r.stdout ?? ''}${r.stderr ?? ''}` }
}

const lib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'prior-art.mjs')).href)
const notes = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'notes.mjs')).href)
const gitlib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'git.mjs')).href)
const front = (binDir) => import(pathToFileURL(join(binDir, 'inflight.mjs')).href)
const branches = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'branches.mjs')).href)
const termsLib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'terms.mjs')).href)
const tagsLib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'inflight-tags.mjs')).href)
const docsShapeLib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'docs-shape.mjs')).href)
const cacheLib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'cache.mjs')).href)
const docsCommandsLib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'docs-commands.mjs')).href)
const rankLib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'rank.mjs')).href)
const repoLib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'repo.mjs')).href)

/** The notes-area corpus index, the way the `rank` row builds it - notes only, never all three areas. */
async function rankCorpus(binDir) {
    const { corpusIndex } = await notes(binDir)
    const { DOC_AREAS, NOTES_DIR } = await repoLib(binDir)
    return corpusIndex({ areas: DOC_AREAS.filter((a) => a.dir === NOTES_DIR) })
}

/**
 * `rank` over the fixture with no PR snapshot, exercising the real register fetch.
 *
 * The blob read goes through `registerBlob` rather than the working tree, because reading the
 * checked-out copy is exactly the working-tree answer this whole tool exists to stop giving.
 */
async function rankIndex(binDir, group = null) {
    const { rank, registerBlob } = await rankLib(binDir)
    const index = await rankCorpus(binDir)
    return rank(index, { prs: NO_PRS, register: registerBlob(index), group })
}

/**
 * Source with comments removed, so a check about CODE is not answered by prose. The first cut of
 * `library-never-exits-the-process` grepped raw text and failed on the comment that states the rule
 * it enforces - a check whose own documentation trips it teaches people to delete the documentation.
 * Crude by design: block comments then line comments, no string awareness, which can only produce a
 * false PASS on a `process.exit` hidden inside a string literal - not a false failure.
 */
const code = (file) => readFileSync(file, 'utf8').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '')

/** Patch a file, refusing to silently do nothing - a mutation that misses makes its control vacuous. */
function patch(file, from, to) {
    const before = readFileSync(file, 'utf8')
    if (!before.includes(from)) throw new Error(`mutation anchor not found in ${file}: ${from.slice(0, 60)}...`)
    writeFileSync(file, before.replace(from, to))
}
/**
 * Every command path the front door registers, including subcommands.
 *
 * Read from the registry itself. This was a regex over the source anchored to one indent level, so
 * it returned the three top-level names and neither subcommand - and the two checks that iterate it
 * are named "every registered command".
 */
const registeredNames = async (binDir) => (await front(binDir)).COMMAND_PATHS

/** A term that exists in docs/inflight/ on many refs - so overlap, if present, is visible. */
const TERM_IN_DOCS = 'inflight-impact'

/**
 * A note with both divergent and behind-only versions, CHOSEN FROM THE CORPUS rather than named.
 *
 * The drift checks hardcoded `docs/inflight/bug-857-family.md`. docs/inflight/AGENTS.md's contract is
 * that a note is `git rm`d when its work lands, so that path is guaranteed to disappear - and on the
 * day it does, these checks go red in the REAL tree for a reason unrelated to any code change, which
 * the suite reports identically to a regression. Picking the note by the property the check needs
 * makes it hermetic against the repository's own lifecycle.
 *
 * @returns {Promise<{path: string, drift: object} | null>} null when no note qualifies - the caller
 *   must treat that as "cannot test", never as a pass.
 */
async function aDriftedNote(binDir) {
    const n = await notes(binDir)
    const index = n.corpusIndex()
    const candidates = [...index.byPath.entries()]
        .filter(([path, versions]) => index.basePaths.has(path) && versions.size > 2)
        .map(([path]) => path)
        .sort()
    for (const path of candidates) {
        const d = n.drift(path, { prs: new Map() })
        if (d.found && d.divergent.length > 0 && d.behind.versions > 0) return { path, drift: d }
    }
    return null
}

/**
 * A check is an async predicate over a bin directory, so the same code runs against the real tree
 * and against a mutant. Returning a boolean rather than throwing is what makes that possible.
 */
// ---------------------------------------------------------------------------------------------
// REFACTOR-WINDOW FIXTURES
//
// The four checks that shipped first covered `loadCandidates` and one regex over the source, and
// NOTHING drove the measurement against an actual repository. Every defect the review found lived
// in the half no test could reach: a renamed path scored as a whole new file, a null baseline
// reported as four open windows, a ref with no merge-base dropped from both the maximum and the
// count meant to make absences visible. Each builder below is the smallest repository that makes
// one of those states reachable, and every check here was written to FAIL against the code as it
// was - that is what made them worth writing.
//
// Deliberately not the shared `buildFixture()`: that one is a corpus of in-flight NOTES, and these
// need source files moving between two paths, which is the fork's package rename in miniature.

const lines = (n, tag) => `${Array.from({ length: n }, (_, i) => `line ${i} ${tag}`).join('\n')}\n`

/**
 * THE PACKAGE RENAME IN MINIATURE, which is the shape three of the four shipped candidates hit.
 *
 * master holds the file at the OLD path. `renamed` moves it to the NEW path and adds five lines;
 * its real divergence is therefore five, not the twenty-five lines the file happens to contain.
 * `plain` keeps the old path and adds three. The correct maximum is 5.
 */
function buildRenameFixture() {
    const { dir, git, commit } = windowRepo()
    mkdirSync(join(dir, 'src', 'io'), { recursive: true })
    writeFileSync(join(dir, 'src', 'io', 'Big.java'), lines(20, 'base'))
    commit('base at the old path')

    git('checkout', '-q', '-b', 'renamed')
    mkdirSync(join(dir, 'src', 'bz'), { recursive: true })
    git('mv', 'src/io/Big.java', 'src/bz/Big.java')
    writeFileSync(join(dir, 'src', 'bz', 'Big.java'), lines(20, 'base') + lines(5, 'added by renamed'))
    commit('rename the package and add five lines')

    git('checkout', '-q', '-b', 'plain', 'master')
    writeFileSync(join(dir, 'src', 'io', 'Big.java'), lines(20, 'base') + lines(3, 'added by plain'))
    commit('add three lines at the old path')

    git('checkout', '-q', 'master')
    return dir
}

/** A ref with an unrelated history, so `git merge-base` cannot answer for it at all. */
function buildOrphanFixture() {
    const { dir, git, commit } = windowRepo()
    mkdirSync(join(dir, 'src', 'io'), { recursive: true })
    writeFileSync(join(dir, 'src', 'io', 'Big.java'), lines(10, 'base'))
    commit('base')
    git('checkout', '-q', '--orphan', 'unrelated')
    writeFileSync(join(dir, 'src', 'io', 'Big.java'), lines(400, 'unrelated history'))
    commit('a huge version on an unrelated history')
    git('checkout', '-q', 'master')
    return dir
}

/** Neither origin/master nor master resolves - the shallow / single-ref clone `baseline()` names. */
function buildNoBaselineFixture() {
    const dir = mkdtempSync(join(tmpdir(), 'inflight-nobase-'))
    const git = windowGit(dir)
    git('init', '-q', '-b', 'trunk')
    mkdirSync(join(dir, 'src', 'io'), { recursive: true })
    writeFileSync(join(dir, 'src', 'io', 'Big.java'), lines(10, 'base'))
    git('add', '-A'); git('-c', 'user.email=t@t', '-c', 'user.name=t', 'commit', '-q', '-m', 'only commit')
    return dir
}


/**
 * A file that grew, with the OLD version committed far enough back to sit outside the growth window
 * and under its pre-rename name - so this also pins that the historical lookup tries every
 * configured path rather than only today's.
 *
 * `rev-list --before` reads the COMMITTER date, so both dates are set; setting only the author date
 * leaves the commit looking present-day and the window finds nothing.
 */
function buildGrowthFixture() {
    const { dir, git } = windowRepo()
    const old = { GIT_AUTHOR_DATE: '2020-01-01T00:00:00Z', GIT_COMMITTER_DATE: '2020-01-01T00:00:00Z' }
    const commitAt = (msg, env) => {
        git('add', '-A')
        const r = spawnSync('git', ['-c', 'user.email=t@t', '-c', 'user.name=t', 'commit', '-q', '-m', msg],
            { cwd: dir, encoding: 'utf8', env: { ...process.env, ...env } })
        if (r.status !== 0) throw new Error(`fixture: commit failed: ${r.stderr}`)
    }
    mkdirSync(join(dir, 'src', 'io'), { recursive: true })
    writeFileSync(join(dir, 'src', 'io', 'Big.java'), lines(10, 'old'))
    commitAt('long ago, under the old name', old)
    mkdirSync(join(dir, 'src', 'bz'), { recursive: true })
    git('mv', 'src/io/Big.java', 'src/bz/Big.java')
    writeFileSync(join(dir, 'src', 'bz', 'Big.java'), lines(30, 'now'))
    commitAt('renamed and grown', {})
    return dir
}


/** A branch that only DELETES, so a signal counting additions alone scores it at zero. */
function buildDeletionFixture() {
    const { dir, git, commit } = windowRepo()
    mkdirSync(join(dir, 'src', 'io'), { recursive: true })
    writeFileSync(join(dir, 'src', 'io', 'Big.java'), lines(20, 'base'))
    commit('base')
    git('checkout', '-q', '-b', 'deleter')
    writeFileSync(join(dir, 'src', 'io', 'Big.java'), lines(12, 'base'))
    commit('delete eight lines')
    git('checkout', '-q', 'master')
    return dir
}

/** One MEASURABLE ref safely under threshold, plus one unrelated ref nobody can measure. */
function buildMixedFixture() {
    const { dir, git, commit } = windowRepo()
    mkdirSync(join(dir, 'src', 'io'), { recursive: true })
    writeFileSync(join(dir, 'src', 'io', 'Big.java'), lines(20, 'base'))
    commit('base')
    git('checkout', '-q', '-b', 'small')
    writeFileSync(join(dir, 'src', 'io', 'Big.java'), lines(20, 'base') + lines(1, 'one more'))
    commit('add one line')
    git('checkout', '-q', '--orphan', 'unrelated')
    writeFileSync(join(dir, 'src', 'io', 'Big.java'), lines(300, 'unrelated history'))
    commit('a huge version on an unrelated history')
    git('checkout', '-q', 'master')
    return dir
}

let DOCS = null
const docsFixture = () => (DOCS ??= buildDocsFixture().dir)

let TERMS = null
const termsFixture = () => (TERMS ??= buildTermsFixture().dir)

/**
 * The corpus fixture plus the two situations `docs show`'s ref selection is specified against and
 * the shared fixture does not hold: a path carried by SEVERAL live refs and an archival ref that
 * SORTS FIRST (`aa-parked` < `yy-live` < `zz-live`, so a selection that forgot the live filter
 * picks the tag), and a path held by a tag alone. Its own repository for the reason
 * bin/lib/fixture-repos.mjs gives: the drift checks assert exact counts on the shared one.
 */
let DOCS_SHOW = null
function buildDocsShowFixture() {
    const { dir, git, commit, write } = buildDocsFixture()
    const parkInTag = (branch, tag, rel, body) => {
        git('checkout', '-q', '-b', branch, 'master')
        write(rel, body)
        commit(`parked in ${tag}`)
        git('tag', tag)
        git('checkout', '-q', 'master')
        git('branch', '-q', '-D', branch)
    }
    git('checkout', '-q', '-b', 'yy-live', 'master')
    write('docs/inflight/parked.md', '# Parked\n\nfirst live copy\n')
    commit('first live copy')
    git('checkout', '-q', '-b', 'zz-live', 'master')
    write('docs/inflight/parked.md', '# Parked\n\nsecond live copy\n')
    commit('second live copy')
    git('checkout', '-q', 'master')
    parkInTag('to-tag-parked', 'aa-parked', 'docs/inflight/parked.md', '# Parked\n\ntagged copy\n')
    parkInTag('to-tag-only', 'aa-tagonly', 'docs/inflight/tag-only.md', '# Tag only\n\nx\n')
    return dir
}
const docsShowFixture = () => (DOCS_SHOW ??= buildDocsShowFixture())

/**
 * The corpus fixture plus the shapes the session index is specified against: a WORKSTREAM - one
 * branch carrying several notes, a solution and a plan the baseline has never had, which R18 says
 * must appear as one heading naming the branch - and two more single-note branches, so the
 * off-baseline in-flight groups number four (workstream, only-here, second, third) and a line cap
 * has a tail to collapse. Its own repository for the reason bin/lib/fixture-repos.mjs gives.
 */
let DOCS_INDEX = null
function buildDocsIndexFixture() {
    const { dir, git, commit, write } = buildDocsFixture()
    const task = (title) => `# ${title}\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\nbody\n`
    git('checkout', '-q', '-b', 'feats/workstream', 'master')
    write('docs/inflight/ws-a.md', task('Workstream note A'))
    write('docs/inflight/ws-b.md', task('Workstream note B'))
    write('docs/inflight/ws-c.md', task('Workstream note C'))
    write('docs/solutions/ci/ws-sol.md', '# A workstream solution\n\nfixed\n')
    write('docs/plans/2026-03-03-001-ws-plan.md', '# A workstream plan\n\nsteps\n')
    commit('a workstream master never had')
    git('checkout', '-q', '-b', 'feats/second', 'master')
    write('docs/inflight/second.md', task('Second branch note'))
    commit('one note')
    git('checkout', '-q', '-b', 'feats/third', 'master')
    write('docs/inflight/third.md', task('Third branch note'))
    commit('one note')
    git('checkout', '-q', 'master')
    return dir
}
const docsIndexFixture = () => (DOCS_INDEX ??= buildDocsIndexFixture())

/**
 * The corpus fixture plus the two shapes the shared-tree index build is specified against and the
 * shared fixture does not hold: refs that SHARE a `docs/` tree object (`src-only-a` and `src-only-b`
 * edit source only, so both name master's tree - the common case on the real repository, where
 * most tips never touch `docs/`), and a ref with NO `docs/` directory at all (`no-docs`, an orphan
 * history), which must index as empty rather than as an error. Its own repository for the reason
 * bin/lib/fixture-repos.mjs gives: the drift checks assert exact ref counts on the shared one.
 */
let SHARED_TREES = null
function buildSharedTreesFixture() {
    const { dir, git, commit, write } = buildDocsFixture()
    for (const name of ['src-only-a', 'src-only-b']) {
        git('checkout', '-q', '-b', name, 'master')
        write(`src/${name}.java`, `// ${name}\n`)
        commit(`${name}: source only, docs untouched`)
    }
    git('checkout', '-q', '--orphan', 'no-docs')
    git('rm', '-rq', '--cached', '.')
    git('clean', '-fdq')
    write('src/Orphan.java', '// no docs directory on this history\n')
    commit('an orphan history with no docs/')
    git('checkout', '-q', 'master')
    return dir
}
const sharedTreesFixture = () => (SHARED_TREES ??= buildSharedTreesFixture())

/** A corpus whose index is comfortably over a 64 KiB pipe buffer: enough long titles on master. */
let BIG_INDEX = null
function buildBigIndexFixture() {
    const { dir, commit } = windowRepo()
    mkdirSync(join(dir, 'docs', 'inflight'), { recursive: true })
    const filler = 'a title long enough that six hundred of them overflow the pipe buffer this fixture exists to overflow'
    for (let i = 0; i < 600; i++) {
        writeFileSync(join(dir, 'docs', 'inflight', `note-${String(i).padStart(3, '0')}.md`),
            `# Note ${i} ${filler}\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\nbody\n`)
    }
    commit('six hundred notes')
    return dir
}
const bigIndexFixture = () => (BIG_INDEX ??= buildBigIndexFixture())

/** The document half of a `docs show` page: everything after the separator that names the ref. */
const bodyShown = (out, path, ref) => out.split(`--- ${path} @ ${ref} ---`)[1] ?? null

/** One malformed invocation refused: exit 2, and the reason exactly as the code states it - never a paraphrase. */
const refuses = (binDir, args, reason, cwd) => {
    const r = invoke(binDir, args, { cwd })
    return r.code === 2 && r.out.includes(reason)
}

const views = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'views.mjs')).href)
const docsViews = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'docs-views.mjs')).href)
const perfOf = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'perf.mjs')).href)

/** How many times one subcommand ran since the last perfReset, read off the perf report. */
const callCount = (report, kind) => {
    const m = report.match(new RegExp(`${kind.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&')}\\s+(\\d+) call`))
    return m ? Number(m[1]) : 0
}

/** Run `fn` with the process inside `dir`, restoring the previous cwd whatever happens. */
async function inDir(dir, fn) {
    const before = cwd()
    chdir(dir)
    try { return await fn() } finally { chdir(before) }
}

/** A candidate config written beside the fixture, so the loader's default path is never involved. */
function windowConfig(dir, candidates, name = "candidates.json") {
    const p = join(dir, name)
    writeFileSync(p, JSON.stringify({ candidates }))
    return p
}

const NO_PRS = { ok: true, map: new Map() }

const CHECKS = [
    {
        id: 'library-never-exits-the-process',
        why: 'exit codes are a fact about a process; a library that exits cannot be called by anything else',
        run: async (binDir) => readdirSync(join(binDir, 'lib'))
            .filter((f) => f.endsWith('.mjs'))
            .every((f) => !code(join(binDir, 'lib', f)).includes('process.exit')),
        mutate: (binDir) => {
            const f = join(binDir, 'lib', 'prior-art.mjs')
            writeFileSync(f, `${readFileSync(f, 'utf8')}\nif (globalThis.__never) process.exit(1)\n`)
        }, // append, so there is no anchor to go stale
    },
    {
        id: 'a-renamed-path-is-one-file-not-two',
        why: 'the shipped config lists two paths per candidate BECAUSE the package rename is in flight, and reading them as two independent files scored a renamed branch at the whole file length - 3 of 4 candidates reported a fabricated divergence and named the wrong branch to land',
        run: async (binDir) => {
            const { refactorWindow } = await import(pathToFileURL(join(binDir, 'lib', 'refactor-window.mjs')).href)
            const dir = buildRenameFixture()
            return inDir(dir, () => {
                const cfg = windowConfig(dir, [{ id: 'big', paths: ['src/bz/Big.java', 'src/io/Big.java'], threshold: 2, hint: 'h' }])
                const r = refactorWindow({ configPath: cfg, prs: NO_PRS })
                if (!r.ok) return false
                const c = r.candidates[0]
                // `renamed` added five lines while moving the file; `plain` added three. The file is
                // twenty-five lines long on `renamed`, and that number must appear nowhere.
                return c.ok === true && c.largest !== null && c.largest.churn === 5 && c.open === false
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'refactor-window.mjs'),
            'const mbBlobsByPath = c.paths.map((p) => mergeBaseBlobs(mbByRef, p))',
            'const mbBlobsByPath = [mergeBaseBlobs(mbByRef, c.paths[0])]'),
    },
    {
        id: 'no-baseline-never-reports-a-window-open',
        why: 'baseline() returns null in a shallow or single-ref clone and documents that the caller must say so; unchecked, every candidate reported OPEN and both hooks told the operator to go and decompose an oversized class from a measurement that never ran',
        run: async (binDir) => {
            const { refactorWindow } = await import(pathToFileURL(join(binDir, 'lib', 'refactor-window.mjs')).href)
            const dir = buildNoBaselineFixture()
            return inDir(dir, () => {
                const cfg = windowConfig(dir, [{ id: 'big', paths: ['src/io/Big.java'], threshold: 1, hint: 'h' }])
                const r = refactorWindow({ configPath: cfg, prs: NO_PRS })
                // The whole run is refused. Anything that reports per-candidate here has already
                // decided it measured something.
                return r.ok === false && typeof r.reason === 'string' && /baseline/i.test(r.reason)
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'refactor-window.mjs'),
            '    if (!base) {', '    if (false) {'),
    },
    {
        id: 'a-ref-that-cannot-be-measured-is-never-counted-as-measured',
        why: 'a ref with no merge-base was added to `matched` and then dropped by a bare `continue`, so it was in neither the maximum nor the unmatched count - a 400-line divergence on a live branch reported as no divergence at all',
        run: async (binDir) => {
            const { refactorWindow } = await import(pathToFileURL(join(binDir, 'lib', 'refactor-window.mjs')).href)
            const dir = buildOrphanFixture()
            return inDir(dir, () => {
                const cfg = windowConfig(dir, [{ id: 'big', paths: ['src/io/Big.java'], threshold: 5, hint: 'h' }])
                const r = refactorWindow({ configPath: cfg, prs: NO_PRS })
                if (!r.ok) return false
                const c = r.candidates[0]
                // The unrelated ref must be visible as unanswerable, and must not leave a verdict of
                // "open" standing on a measurement that skipped it.
                return c.unanswerableRefs >= 1 && c.open === false && c.ok === false
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'refactor-window.mjs'),
            'if (typeof added !== \'number\') { out.unanswerableRefs++; continue }',
            'if (typeof added !== \'number\') { continue }'),
    },
    {
        id: 'a-candidate-no-ref-carries-is-a-broken-config-not-a-quiet-tree',
        why: 'the paths list exists to survive the package rename, so the day a spelling is retired the entry matches nothing - and that reported OPEN forever, which is the config going stale rendered as an instruction to start refactoring',
        run: async (binDir) => {
            const { refactorWindow } = await import(pathToFileURL(join(binDir, 'lib', 'refactor-window.mjs')).href)
            const dir = buildRenameFixture()
            return inDir(dir, () => {
                const cfg = windowConfig(dir, [{ id: 'gone', paths: ['src/io/NoSuchFile.java'], threshold: 5, hint: 'h' }])
                const r = refactorWindow({ configPath: cfg, prs: NO_PRS })
                if (!r.ok) return false
                const c = r.candidates[0]
                return c.ok === false && c.open === false && /path/i.test(c.reason || '')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'refactor-window.mjs'),
            'if (matched.size === 0) {',
            'if (false) {'),
    },
    {
        id: 'growth-is-derived-from-git-and-follows-the-rename',
        why: 'the whole point of deriving it is that it cannot rot the way docs/refactoring.md\'s own "1533 lines" did while the file reached 2405 - and asking only today\'s path would report a renamed file as newly created rather than as grown',
        run: async (binDir) => {
            const { refactorWindow } = await import(pathToFileURL(join(binDir, 'lib', 'refactor-window.mjs')).href)
            const dir = buildGrowthFixture()
            return inDir(dir, () => {
                const cfg = windowConfig(dir, [{ id: 'big', paths: ['src/bz/Big.java', 'src/io/Big.java'], threshold: 5, hint: 'h' }])
                const r = refactorWindow({ configPath: cfg, prs: NO_PRS })
                if (!r.ok) return false
                const g = r.candidates[0].growth
                // 10 lines then under the old name, 30 now under the new one.
                return g !== null && g.now === 30 && g.then === 10 && g.delta === 20
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'refactor-window.mjs'),
            'function firstBlobAtAnyPath(rev, paths) {\n    for (const path of paths) {',
            'function firstBlobAtAnyPath(rev, paths) {\n    for (const path of [paths[0]]) {'),
    },
    {
        id: 'a-failed-merge-base-lookup-is-not-a-new-file',
        why: 'the rewrite onto the shared primitives added the one line that separates "absent at the merge-base" from "git could not be asked", and NOTHING patched notes.mjs - so a future edit could silently restore drift reporting an existing note as created-by-this-branch and nothing would go red',
        run: async (binDir) => {
            const { addedSinceMergeBase } = await import(pathToFileURL(join(binDir, 'lib', 'notes.mjs')).href)
            const dir = buildRenameFixture()
            return inDir(dir, () => {
                // A path containing a NEWLINE injects an extra line into `cat-file --batch-check`'s
                // input, so git returns fewer answers than refs and `blobsForPath` reports ok:false -
                // the only cheap way to reach the branch under test, and a real input rather than a stub.
                const r = addedSinceMergeBase('master', 'renamed', 'src/io/a\nb.java', '0'.repeat(40))
                // null means "unanswerable". `{newFile: true}` would be the old conflation returning.
                return r === null
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            '    if (!mb.ok) return null', '    if (false) return null'),
    },
    {
        id: 'a-deleting-branch-is-a-divergence-too',
        why: 'the signal is conflict-producing divergence, not file growth - counting only added lines scored a branch that deletes or moves a large block at nearly zero, which is exactly the branch a decomposition collides with',
        run: async (binDir) => {
            const { refactorWindow } = await import(pathToFileURL(join(binDir, 'lib', 'refactor-window.mjs')).href)
            const dir = buildDeletionFixture()
            return inDir(dir, () => {
                const cfg = windowConfig(dir, [{ id: 'big', paths: ['src/io/Big.java'], threshold: 3, hint: 'h' }])
                const r = refactorWindow({ configPath: cfg, prs: NO_PRS })
                if (!r.ok) return false
                const c = r.candidates[0]
                // Eight lines removed, none added. Counting additions alone reports 0 and opens.
                return c.ok === true && c.largest !== null && c.largest.churn === 8 && c.open === false
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'refactor-window.mjs'),
            '? d.added + d.removed', '? d.added'),
    },
    {
        id: 'one-unmeasurable-ref-blocks-an-otherwise-open-verdict',
        why: 'the fourth route to the false pass, and the one three earlier reviews walked past: when SOMETHING was measurable the verdict stopped consulting the unanswerable count, so a tiny measured divergence plus an unmeasured ref reported open',
        run: async (binDir) => {
            const { refactorWindow } = await import(pathToFileURL(join(binDir, 'lib', 'refactor-window.mjs')).href)
            const dir = buildMixedFixture()
            return inDir(dir, () => {
                const cfg = windowConfig(dir, [{ id: 'big', paths: ['src/io/Big.java'], threshold: 50, hint: 'h' }])
                const r = refactorWindow({ configPath: cfg, prs: NO_PRS })
                if (!r.ok) return false
                const c = r.candidates[0]
                // largest is +1, far under the threshold of 50 - and one ref was never measured.
                return c.largest !== null && c.largest.churn <= 2 && c.unanswerableRefs >= 1 && c.open === false
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'refactor-window.mjs'),
            'out.open = out.unanswerableRefs === 0 && (out.largest === null || out.largest.churn <= c.threshold)',
            'out.open = out.largest === null ? out.unanswerableRefs === 0 : out.largest.churn <= c.threshold'),
    },
    {
        id: 'the-threshold-boundary-is-inclusive',
        why: 'R8 says at-or-below the threshold is open, and an off-by-one at the boundary is the difference between a window that opens and one that never does',
        run: async (binDir) => {
            const { refactorWindow } = await import(pathToFileURL(join(binDir, 'lib', 'refactor-window.mjs')).href)
            const dir = buildRenameFixture()
            return inDir(dir, () => {
                const at = windowConfig(dir, [{ id: 'big', paths: ['src/bz/Big.java', 'src/io/Big.java'], threshold: 5, hint: 'h' }], 'at.json')
                const below = windowConfig(dir, [{ id: 'big', paths: ['src/bz/Big.java', 'src/io/Big.java'], threshold: 4, hint: 'h' }], 'below.json')
                const rAt = refactorWindow({ configPath: at, prs: NO_PRS })
                const rBelow = refactorWindow({ configPath: below, prs: NO_PRS })
                // Largest is 5. At a threshold of 5 the window is open; at 4 it is not.
                return rAt.ok && rBelow.ok && rAt.candidates[0].open === true && rBelow.candidates[0].open === false
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'refactor-window.mjs'),
            'out.largest.churn <= c.threshold', 'out.largest.churn < c.threshold'),
    },
    {
        id: 'a-broken-refactor-config-is-not-an-empty-candidate-list',
        why: 'an empty list renders identically to a quiet tree, and the two mean opposite things - go and refactor, versus this tool is broken',
        run: async (binDir) => {
            const { loadCandidates } = await import(pathToFileURL(join(binDir, 'lib', 'refactor-window.mjs')).href)
            // Scratch goes to a TEMP DIR, never to binDir. On the mutant pass binDir is a throwaway
            // copy and either would do; on the real pass it is this repository's own bin/, and the
            // first cut of these two checks left fixture JSON sitting in it.
            const scratch = mkdtempSync(join(tmpdir(), 'inflight-refactor-'))
            const bad = join(scratch, 'not-json.json')
            writeFileSync(bad, '{ this is not json')
            const broken = loadCandidates(bad)
            const absent = loadCandidates(join(scratch, 'no-such-file.json'))
            return broken.ok === false && typeof broken.reason === 'string' && broken.reason.length > 0
                && absent.ok === false && typeof absent.reason === 'string' && absent.reason.length > 0
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'refactor-window.mjs'),
            'return { ok: false, reason: `${path} is not valid JSON: ${e.message}` }',
            'return { ok: true, candidates: [] }'),
    },
    {
        id: 'a-refactor-candidate-names-every-path-it-is-known-by',
        why: 'a bare string works today and silently loses the second path the moment somebody edits it in place - which across this fork\'s in-flight package rename is exactly the regression the list exists to prevent',
        run: async (binDir) => {
            const { loadCandidates } = await import(pathToFileURL(join(binDir, 'lib', 'refactor-window.mjs')).href)
            const f = join(mkdtempSync(join(tmpdir(), 'inflight-refactor-')), 'bare-string-paths.json')
            writeFileSync(f, JSON.stringify({ candidates: [{ id: 'x', paths: 'a/B.java', threshold: 1, hint: 'h' }] }))
            const r = loadCandidates(f)
            return r.ok === false && typeof r.reason === 'string' && r.reason.includes('paths')
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'refactor-window.mjs'),
            '        if (!Array.isArray(c.paths)',
            "        if (typeof c.paths === 'string') c.paths = [c.paths]\n        if (!Array.isArray(c.paths)"),
    },
    {
        id: 'the-shipped-refactor-candidate-list-loads',
        why: 'every other check here uses a fixture, so nothing would notice the file the tool actually ships with going malformed',
        run: async (binDir) => {
            const { loadCandidates } = await import(pathToFileURL(join(binDir, 'lib', 'refactor-window.mjs')).href)
            const r = loadCandidates()
            return r.ok === true && r.candidates.length > 0 && r.candidates.every((c) => c.id
                && Array.isArray(c.paths) && c.paths.length > 0
                && typeof c.threshold === 'number' && typeof c.hint === 'string' && c.hint.length > 0)
        },
        mutate: (binDir) => writeFileSync(join(binDir, 'refactor-candidates.json'), '{ "candidates": [ { "id": "x" } ] }'),
    },
    {
        id: 'prior-art-returns-findings-not-a-code',
        why: 'every queued feature - drift, headings, ref clustering - is a view over the data, not over the page',
        run: async (binDir) => {
            const { priorArt } = await lib(binDir)
            return inFixture(() => {
                const r = priorArt(['a-term-that-cannot-match-anything-xyzzy'], { github: false })
                return r !== null && typeof r === 'object'
                    && r.ok === true && Array.isArray(r.sections) && Array.isArray(r.warnings)
                    && typeof r.refsSearched === 'number' && r.refsSearched > 0
                    && r.sections.every((s) => Array.isArray(s.hits) && typeof s.heading === 'string')
            })
        },
        mutate: (binDir) => {
            const f = join(binDir, 'lib', 'prior-art.mjs')
            patch(f, 'export function priorArt(terms, opts = {}) {',
                'export function priorArt(terms, opts = {}) {\n    if (globalThis.__x !== 1) return 0\n')
        },
    },
    {
        id: 'a-search-that-found-nothing-still-ran',
        why: '"no hits" and "could not look" are different answers, and conflating them is the whole incident',
        run: async (binDir) => {
            const { priorArt } = await lib(binDir)
            return inFixture(() => {
                const r = priorArt(['a-term-that-cannot-match-anything-xyzzy'], { github: false })
                return r.ok === true && r.sections.every((s) => s.hits.length === 0) && r.refsSearched > 0
            })
        },
        mutate: (binDir) => {
            const f = join(binDir, 'lib', 'prior-art.mjs')
            patch(f, '        result.sections.push(section)',
                '        if (section.hits.length === 0) return cannot(`nothing in ${heading}`)\n        result.sections.push(section)')
        },
    },
    {
        id: 'sections-do-not-overlap',
        why: 'section 4 is headed "everything else"; a pathspec that re-lists sections 1-3 makes that a lie',
        run: async (binDir) => {
            const { priorArt } = await lib(binDir)
            return inFixture(() => {
                const r = priorArt([TERM_IN_DOCS], { github: false })
                if (!r.ok) return false
                const all = r.sections.flatMap((s) => s.hits.map((h) => h.path))
                if (all.length === 0) return false // a check that cannot see anything cannot pass
                return new Set(all).size === all.length
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'prior-art.mjs'),
            "            'docs/', ...DOC_AREAS.map((a) => `:(exclude)${a.dir}/`)]],",
            "            'docs/*.md']],"),
    },
    {
        id: 'ref-clusters-are-deduplicated',
        why: 'a cluster states how big a finding is; counting one note twice overstates it',
        run: async (binDir) => {
            const { refClusters } = await lib(binDir)
            const dup = { path: 'docs/inflight/x.md', refs: ['a', 'b'], onBaseline: false }
            const clusters = refClusters({
                sections: [{ hits: [dup] }, { hits: [{ ...dup }] }],
            })
            return clusters.length === 1 && clusters[0].paths.length === 1
        },
        mutate: (binDir) => {
            const f = join(binDir, 'lib', 'prior-art.mjs')
            patch(f, '            if (seen.has(h.path)) continue\n            seen.add(h.path)\n', '')
        },
    },
    {
        id: 'help-lists-every-registered-command',
        why: 'a tool reachable only by knowing its filename is the state the front door exists to end',
        run: async (binDir) => {
            const names = await registeredNames(binDir)
            if (names.length < 2) return false // a registry with no subcommands is not this one
            const help = invoke(binDir, ['help']).out
            return names.every((n) => help.includes(n))
        },
        // Through patch() like every other, after this one anchor went stale and failed SILENTLY -
        // a raw .replace on a regex that matches nothing leaves the mutant identical to the original,
        // so the control reports a pass having tested exactly nothing.
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            '        ...ALL.flatMap((c) => [', '        ...[].flatMap((c) => ['),
    },
    {
        id: 'usage-names-the-front-door-not-the-library',
        why: 'a help screen naming a path that no longer runs is citation rot inside the tool',
        run: async (binDir) => (await registeredNames(binDir)).every((n) => {
            const usage = invoke(binDir, ['help', ...n.split(' ')])
            if (usage.code !== 0) return false
            return usage.out.includes(`bin/inflight.mjs ${n}`) && !/bin\/(?!inflight)[a-z-]+\.mjs/.test(usage.out)
        }),
        // Anchored on the usage line, not on the bare path: the first occurrence of that path in
        // the library is in its header comment, so a looser anchor mutates prose and leaves the help
        // screen correct - a mutation that lands somewhere harmless is a control that proves nothing.
        mutate: (binDir) => patch(join(binDir, 'lib', 'prior-art.mjs'),
            'Usage: bin/inflight.mjs prior-art', 'Usage: bin/prior-art.mjs'),
    },
    {
        id: 'unknown-command-cannot-run',
        why: 'a typo that exits 0 reads as a tool that ran and found nothing',
        run: async (binDir) => {
            const r = invoke(binDir, ['nosuchthing'])
            return r.code === 2 && r.out.includes('nosuchthing')
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            '    if (!top) return { ok: false, reason:',
            '    if (!top) return { ok: true } // mutant: a typo now looks like a successful run\n    if (globalThis.__never) return { ok: false, reason:'),
    },
    {
        id: 'bare-invocation-is-a-usage-error',
        why: 'exit 0 on no arguments makes "I ran the tool" true and useless',
        run: async (binDir) => invoke(binDir, []).code === 2,
        mutate: (binDir) => {
            const f = join(binDir, 'inflight.mjs')
            patch(f,
                '    if (!name) return { ok: false, reason: help() }',
                '    if (!name) return { ok: true, reason: help() }')
        },
    },
    {
        id: 'explicit-help-succeeds',
        why: 'asking for help is not an error, and conflating the two trains agents to ignore the code',
        run: async (binDir) => invoke(binDir, ['help']).code === 0,
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            '        if (!rest.length) return { ok: true, reason: help() }',
            '        if (!rest.length) return { ok: false, reason: help() }'),
    },
    {
        id: 'the-front-door-runs-through-a-symlinked-path',
        why: 'the was-I-invoked-directly guard compared a resolved URL to an unresolved argv, so under a symlink the CLI body never ran - and every mutant here is built under one',
        // THE NON-EMPTY HALF IS THE LOAD-BEARING HALF. A front door that never executed exits 0 with
        // nothing on either stream, so a control asserting only the code passes against the exact
        // bug it exists to catch - which is how this survived: `explicit-help-succeeds` asserts
        // `code === 0` and was the ONE check that could see the fault, while every other
        // invoke()-driven mutant was scored "went red" without its mutation ever being exercised.
        //
        // The symlink is made HERE rather than relied on from the environment. macOS `os.tmpdir()`
        // is already one (`/var/folders/...` -> `/private/var/...`), which is why the fault showed
        // up locally and never in CI, where `/tmp` is a real directory - so a control that leaned on
        // the temp root would assert nothing on Linux, the only place this gates a merge.
        run: async (binDir) => {
            const link = join(mkdtempSync(join(tmpdir(), 'inflight-symlink-')), 'linked-bin')
            symlinkSync(realpathSync(binDir), link, 'dir')
            const r = invoke(link, ['help'])
            return r.code === 0 && r.out.trim().length > 0
        },
        // Drops the realpath from the argv side ONLY - the original defect exactly, rather than
        // deleting the guard, which would prove only that some guard ran. Both names stay imported,
        // so the mutant fails by comparing a spelling to a resolved path and not by throwing.
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            '        return realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url))',
            '        return process.argv[1] === realpathSync(fileURLToPath(import.meta.url))'),
    },
    {
        id: 'dispatch-reaches-the-library',
        why: 'a registry that resolves but never calls through is a front door onto nothing',
        run: async (binDir) => {
            // prior-art with no terms returns the LIBRARY's usage, so seeing its text proves the
            // call crossed the boundary rather than being answered by the front door.
            const r = invoke(binDir, ['prior-art'])
            return r.code === 2 && r.out.includes('Grep the MECHANISM, never the symptom')
        },
        mutate: (binDir) => {
            const f = join(binDir, 'inflight.mjs')
            patch(f,
                '            if (terms.length === 0) return { ok: false, reason: priorArtUsage }',
                '            if (terms.length === 0) return { ok: false, reason: "nope" }')
        },
    },
    {
        id: 'blobs-for-path-correlates-by-its-own-token',
        why: 'pairing cat-file output to input by position is correct until anything reorders it',
        run: async (binDir) => {
            const g = await gitlib(binDir)
            return inFixture(() => {
                const tips = g.refTips()
                if (!tips.ok) return false
                const refs = tips.tips.map((r) => r.ref)
                const lookup = g.blobsForPath(refs, 'docs/inflight/shared.md')
                if (!lookup.ok) return false
                const m = lookup.blobs
                const known = new Set(refs)
                if (m.size === 0 || ![...m.keys()].every((k) => known.has(k))) return false
                const base = g.baseline()
                const direct = g.exec('git', ['rev-parse', `${base}:docs/inflight/shared.md`]).out.trim()
                return m.get(base) === direct
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'git.mjs'),
            'refs.map((r) => (selfDescribing ? `${r}:${path} ${r}` : `${r}:${path}`))',
            'refs.map((r) => `${r}:${path}`)'),
    },
    {
        id: 'stranded-excludes-what-the-baseline-once-had',
        why: 'a note the baseline git rm-d landed and closed; reporting it as stranded is a false positive',
        run: async (binDir) => {
            const n = await notes(binDir)
            return inFixture(() => {
                const index = n.corpusIndex()
                if (!index.ok || index.baseEverPaths.size === 0) return false
                // closed.md WAS on master and was git rm'd, and the `behind` branch does not carry
                // it - so it must not be reported. The `reuse` branch's different closed.md must be.
                // THE COLLISION CASE, which the previous assertion could not see. `closed.md`
                // exists on two branches: `stale-closed` carries the exact content master held
                // before removing it (finished work), and `reuse` carries unrelated new content at
                // the recycled name (stranded). The path must be reported, and reported for `reuse`
                // ALONE. The naive path-level filter this replaced excluded the whole path, which
                // an "is any historical path ever reported" assertion satisfies perfectly - so that
                // assertion stayed green against the exact bug it was written for.
                const clusters = n.stranded(index)
                const closed = clusters.find((c) => c.paths.includes('docs/inflight/closed.md'))
                if (!closed) return false
                if (closed.refs.includes('stale-closed')) return false
                if (!closed.refs.includes('reuse')) return false
                const reported = new Set(clusters.flatMap((c) => c.paths))
                return reported.has('docs/inflight/never-landed.md')
            })
        },
        // Reverts to the PATH-level exclusion - the actual pre-fix bug - rather than deleting the
        // filter outright. Deleting it proved only that a filter existed; this proves it is
        // per-version, which is the property that catches the collision.
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            '            if (heldHere.has(blob)) continue',
            '            if (heldHere.size > 0) continue'),
    },
    {
        id: 'stranded-is-clustered-not-listed',
        why: '364 separate lines is a result an agent stops reading, which is the same as no result',
        run: async (binDir) => {
            const n = await notes(binDir)
            return inFixture(() => {
                const index = n.corpusIndex()
                if (!index.ok) return false
                const clusters = n.stranded(index)
                const paths = clusters.reduce((t, c) => t + c.paths.length, 0)
                // The two notes on `stranded-work` share a ref-set and must collapse to one cluster.
                return paths >= 2 && clusters.length < paths
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            "        const key = s.refs.join(' ')", '        const key = s.path'),
    },
    {
        id: 'drift-reports-divergence-not-distance',
        why: 'a branch that has not merged recently is not drift, and it is most of the volume',
        run: async (binDir) => {
            const n = await notes(binDir)
            return inFixture(() => {
                const d = n.drift('docs/inflight/shared.md', { prs: new Map() })
                if (!d.found) return false
                // `behind` carries a version master itself held; `diverged` carries one it never did.
                if (d.behind.versions !== 1 || d.divergent.length !== 1) return false
                const history = n.baselineHistoryBlobs(d.baseline, 'docs/inflight/shared.md')
                return d.divergent.every((c) => !history.has(c.blob))
            })
        },
        // Mutating the `behind` push emptied `behind` too, so the check failed on its own guard
        // rather than on the assertion its `why` names - a control that never exercised the line it
        // exists for. This keeps the bookkeeping intact and corrupts only the SPLIT, so a historical
        // blob lands in `divergent` and the final `.every(...)` is what goes red.
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            '        if (history.has(blob)) { behind.push({ blob, refs }); continue }',
            '        if (history.has(blob)) { behind.push({ blob, refs }); divergent.push(entry); continue }'),
    },
    {
        id: 'drift-clusters-by-blob-not-by-ref',
        why: 'diffing once per ref instead of once per version is 274 diffs where 37 will do',
        run: async (binDir) => {
            const n = await notes(binDir)
            return inFixture(() => {
            const d = n.drift('docs/inflight/shared.md', { prs: new Map() })
            if (!d.found) return false
            const all = [...d.divergent, ...(d.baselineCluster ? [d.baselineCluster] : [])]
            const blobs = all.map((c) => c.blob)
            if (!(blobs.length > 0 && blobs.length < d.refsCarrying && new Set(blobs).size === blobs.length)) return false
            // `added` is measured against each branch's merge-base and rendered as the headline
            // number; a swap or a missing computation would otherwise turn nothing red.
            return d.divergent.every((c) => c.added !== null
                && (c.added.newFile === true || (Number.isInteger(c.added.added) && Number.isInteger(c.added.removed))))
            })
        },
        // The first mutant here truncated each cluster's ref list, which changed nothing the check
        // asserts - a control that cannot fail. This one emits a cluster twice, which is exactly the
        // per-ref shape clustering exists to avoid, and breaks the uniqueness the check requires.
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            '        else divergent.push(entry)',
            '        else { divergent.push(entry); divergent.push(entry) }'),
    },
    {
        id: 'nested-commands-are-listed-and-reachable',
        why: 'a subcommand missing from help is a tool nobody can find, which is the state this ends',
        run: async (binDir) => {
            const help = invoke(binDir, ['help']).out
            if (!help.includes('note find') || !help.includes('note drift')) return false
            const usage = invoke(binDir, ['help', 'note', 'drift'])
            return usage.code === 0 && usage.out.includes('bin/inflight.mjs note drift')
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            '        const topic = [...ALL].sort((a, b) => b.path.length - a.path.length)',
            '        const topic = [...ALL].sort((a, b) => a.path.length - b.path.length)'),
    },
    {
        id: 'a-total-git-failure-cannot-report-nothing',
        why: 'reporting "searched every branch tip, found nothing" over a search that never ran is this repo\'s worst failure class',
        // THE REGRESSION TEST FOR A SHIPPED P0. Outside a git repository, `stranded`, `note find` and
        // `note drift` printed a clean empty result and exited 0 - `note find` said "no note matching
        // /q/ on any of 0 refs. Nothing is a result here: this searched every branch tip", which is a
        // false claim about work never done. `refTips()` returned `[]` for both "no refs" and "git
        // failed", and only prior-art guarded it. Found by a reviewer running it, not by this suite.
        run: async (binDir) => {
            const outside = mkdtempSync(join(tmpdir(), 'inflight-notarepo-'))
            const commands = [['stranded'], ['note', 'find', 'quarantine'],
                ['note', 'drift', 'docs/inflight/x.md'], ['prior-art', 'anything']]
            return commands.every((args) => invoke(binDir, args, { cwd: outside }).code === 2)
        },
        // Targets the GUARD, not refTips: restoring the old `[]`-on-failure alone still tripped the
        // separate empty-refs guard, so the mutant stayed green while proving nothing. EVERY guard
        // goes, including the one on the tree-resolving cat-file below them: outside a repository
        // that batch fails too, so leaving it would catch the case alone and again prove nothing.
        mutate: (binDir) => {
            patch(join(binDir, 'lib', 'notes.mjs'),
                "    if (!ok) return { ok: false, reason: 'cannot list refs - is this a git repository?' }\n"
                + "    if (refs.length === 0) return { ok: false, reason: 'no branch refs found - nothing to search' }\n"
                + "    if (!base) return { ok: false, reason: 'neither origin/master nor master resolves"
                + " - no baseline to compare against' }",
                '    // mutant: every corpus guard removed')
            patch(join(binDir, 'lib', 'notes.mjs'),
                "    if (!trees.ok) return { ok: false, reason: `cannot resolve ${root || 'the root tree'} on any ref - git cat-file failed` }",
                '    // mutant: the cat-file guard removed with the rest')
        },
    },
    {
        id: 'every-command-runs-end-to-end',
        why: 'the library was tested and the CLI was not, so argv parsing, --all, emit and the whole views layer shipped unexercised',
        run: async (binDir) => {
            const dir = fixture()
            const path = 'docs/inflight/shared.md'
            const runs = [
                [['stranded'], 'NEVER reached'],
                [['note', 'find', 'shared'], 'matching'],
                [['note', 'drift', path], path],
                [['note', 'drift', '--all', path], path],
                [['prior-art', 'a-term-that-cannot-match-anything-xyzzy'], 'nothing'],
            ]
            return runs.every(([args, needle]) => {
                const r = invoke(binDir, args, { cwd: dir })
                return r.code === 0 && r.out.includes(needle)
            })
        },
        // Breaks the views layer, which no check reached before: every one of these commands renders
        // through it, and none of them asserted on a rendered line.
        mutate: (binDir) => patch(join(binDir, 'lib', 'views.mjs'),
            'const plural = (n, w) =>', 'const plural = (n, w) => "" && ('),
    },
    {
        id: 'jq-filter-escapes-a-regex-term-completely',
        why: 'a jq string is a JSON string, and `\\b` is a valid JSON escape - so a hand-rolled quoter turns a regex into a backspace and reports "nothing"',
        // REGRESSION TEST FOR A CodeQL FINDING ON THIS PR. The filter escaped `"` and left `\` alone,
        // so `prior-art '\bRetryQueue\b'` asked GitHub for a literal backspace character, matched
        // nothing, and reported it as nothing found - with no error at any layer.
        run: async (binDir) => {
            const { jqFilter } = await lib(binDir)
            const term = String.raw`\bRetryQueue\b`
            const filter = jqFilter(term, '.number')
            // The emitted literal must survive a JSON round-trip back to the exact term, which is
            // the property "escaped completely" actually means.
            const literal = filter.slice(filter.indexOf('test(') + 5, filter.indexOf('; "i")'))
            let parsed
            try { parsed = JSON.parse(literal) } catch { return false }
            if (parsed !== term) return false
            // And a quote-plus-backslash term must not be able to close the literal early.
            const nasty = String.raw`x\"; "i")) | .secret | (.`
            const hostile = jqFilter(nasty, '.number')
            const hostileLiteral = hostile.slice(hostile.indexOf('test(') + 5, hostile.lastIndexOf('; "i")'))
            try { return JSON.parse(hostileLiteral) === nasty } catch { return false }
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'prior-art.mjs'),
            'test(${JSON.stringify(pattern)}; "i")',
            'test("${pattern.replace(/"/g, \'\\\\"\')}"; "i")'),
    },
    {
        id: 'baseline-history-blobs-sees-every-version-the-baseline-held',
        why: 'this is the discriminator; a gap here silently reclassifies real divergence as merely behind and drops it from view',
        // Tested DIRECTLY, not through drift(). Every other check reaches this function through
        // drift's output, so a bug that widened or narrowed the history set could be masked by the
        // clustering above it - and this is the one place a bug produces the worst outcome the tool
        // has: content that exists only on a branch, reported as "already on master, just behind".
        run: async (binDir) => {
            const n = await notes(binDir)
            const g = await gitlib(binDir)
            return inFixture(() => {
                const base = g.baseline()
                const held = n.baselineHistoryBlobs(base, 'docs/inflight/shared.md')
                // master held exactly v1 and v2 at this path.
                if (held.size !== 2) return false
                const v1 = g.exec('git', ['rev-parse', 'behind:docs/inflight/shared.md']).out.trim()
                const v2 = g.exec('git', ['rev-parse', `${base}:docs/inflight/shared.md`]).out.trim()
                const diverged = g.exec('git', ['rev-parse', 'diverged:docs/inflight/shared.md']).out.trim()
                // Both baseline versions in; the branch-only version out. Narrower OR wider fails.
                return held.has(v1) && held.has(v2) && !held.has(diverged)
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            "exec('git', ['rev-list', '--full-history', base, '--', path])",
            "exec('git', ['rev-list', '--max-count=1', base, '--', path])"),
    },
    {
        id: 'headings-mode-matches-headings-only',
        why: 'a heading search that also matches body text is just the default search wearing a flag',
        run: async (binDir) => {
            const { priorArt } = await lib(binDir)
            return inFixture(() => {
                // "v2 on master" is BODY text in the fixture; "Shared note" is a heading.
                const body = priorArt(['v2'], { github: false, headings: true })
                const head = priorArt(['Shared'], { github: false, headings: true })
                if (!body.ok || !head.ok) return false
                // The discriminating pair: a body-only term must find nothing in headings mode,
                // while the same corpus DOES contain it unscoped.
                const bodyHits = body.sections.flatMap((x) => x.hits)
                const unscoped = priorArt(['v2'], { github: false })
                if (bodyHits.length !== 0 || unscoped.sections.flatMap((x) => x.hits).length === 0) return false
                // And a heading term returns the heading TEXT, not merely the path.
                const hits = head.sections.flatMap((x) => x.hits)
                return hits.length > 0 && hits.every((h) => Array.isArray(h.headings)
                    && h.headings.length > 0 && h.headings.every((t) => t.startsWith('#')))
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'prior-art.mjs'),
            'const grepPattern = opts.headings ? `^#{1,6}[[:space:]].*(${pattern})` : pattern',
            'const grepPattern = pattern'),
    },
    {
        id: 'relatedness-is-containment-not-a-guess',
        why: 'an integration branch reported as an orphan is the detector being confidently wrong',
        // The fixture's `diverged` branches from v1 and adds a commit, so it fully contains
        // `behind`, which still sits at v1. That is a parent/child pair with a known right answer.
        run: async (binDir) => {
            const b = await branches(binDir)
            return inFixture(() => {
                const g = b.commitGraph()
                if (!g.ok) return false
                const child = b.relatives(g, 'feature-int')
                const parent = b.relatives(g, 'feature-a')
                // Containment is directional: feature-int has feature-a's work, never the reverse.
                return child.parents.includes('feature-a') && parent.children.includes('feature-int')
                    && !child.children.includes('feature-a') && !parent.parents.includes('feature-int')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'branches.mjs'),
            'if (mine.has(other.sha)) parents.push(other.ref)',
            'if (!mine.has(other.sha)) parents.push(other.ref)'),
    },
    {
        id: 'a-branch-nothing-tracks-gets-a-remedy-not-a-finding',
        why: 'a report gets skimmed; an instruction gets acted on, and this is how work goes missing',
        run: async (binDir) => {
            const b = await branches(binDir)
            return inFixture(() => {
                const g = b.commitGraph()
                if (!g.ok) return false
                // `stranded-work` has no PR, is named in no note on the baseline, and integrates
                // nothing - the shape that loses work. The remedy must name the file to write.
                const orphan = b.trackingGap(b.branchView(g, 'stranded-work', new Map()))
                if (!orphan || !orphan.remedy.includes('docs/inflight/branch-stranded-work.md')) return false
                // `feature-int` contains another branch, so it is an integration branch, not an orphan.
                const integ = b.trackingGap(b.branchView(g, 'feature-int', new Map()))
                return integ !== null && integ.kind === 'integration'
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'branches.mjs'),
            '    if (view.parents.length > 0) {', '    if (false) {'),
    },
    {
        id: 'a-pr-that-bases-on-a-branch-explains-it',
        why: 'the detector called a documented stack base untracked, which is the cry-wolf that gets a detector ignored',
        // REGRESSION TEST FOR A FALSE POSITIVE Antony caught. astubbs/parallel-consumer#271 bases on
        // `feats/ks-streams-reconciled` and names it in its body, so that branch was documented all
        // along - by a PR rather than a note, which is the half the detector could not see.
        run: async (binDir) => {
            const b = await branches(binDir)
            return inFixture(() => {
                const g = b.commitGraph()
                if (!g.ok) return false
                // With no PR anywhere, stranded-work is a gap.
                if (!b.trackingGap(b.branchView(g, 'stranded-work', new Map()))) return false
                // A PR whose BASE is that branch explains it, even though its head is elsewhere.
                const based = new Map([['feature-a', {
                    number: 1, title: 't', state: 'OPEN', baseRefName: 'stranded-work', body: '',
                }]])
                if (b.trackingGap(b.branchView(g, 'stranded-work', based))) return false
                // A body mention is deliberately NOT checked here: carrying every PR body took the
                // bulk fetch from 56K to 2.3MB to answer a question about the rare untracked
                // branch. That question moved to prForBranch, which asks GitHub about ONE branch and
                // only on a miss - so what this check owns is that the base-ref path works and
                // that an unrelated PR does not silently explain anything.
                const unrelated = new Map([['feature-a', {
                    number: 2, title: 't', state: 'OPEN', baseRefName: 'master',
                }]])
                return b.trackingGap(b.branchView(g, 'stranded-work', unrelated)) !== null
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'branches.mjs'),
            '    if (view.explainedBy.length > 0) return null',
            '    if (false) return null'),
    },
    {
        id: 'an-unknown-baseline-moment-never-grandfathers',
        why: 'if "before tracking was expected" cannot be established, silencing everything is the worst possible default',
        run: async (binDir) => {
            const b = await branches(binDir)
            return inFixture(() => {
                const g = b.commitGraph()
                if (!g.ok) return false
                // The fixture has no bin/inflight.mjs, so the moment is unknowable there.
                if (b.baselineMoment(g.baseline) !== null) return false
                const v = b.branchView(g, 'stranded-work', new Map())
                // Unknown must mean "not grandfathered", so the gap still reports loudly.
                return v.baselineKnown === false && v.predatesBaseline === false
                    && b.trackingGap(v)?.kind !== 'pre-baseline'
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'branches.mjs'),
            '    const predatesBaseline = moment !== null && firstCommit !== undefined',
            '    const predatesBaseline = moment === null || firstCommit !== undefined'),
    },
    {
        id: 'the-remedy-points-at-a-note-that-already-owns-the-branch',
        why: 'a second note for one workstream is what the directory rules forbid, and the first real remedy proposed exactly that',
        run: async (binDir) => {
            const b = await branches(binDir)
            return inFixture(() => {
                const g = b.commitGraph()
                if (!g.ok) return false
                // `feature-a` shares the long token `feature` with branch-feature-workstream.md.
                const owned = b.branchView(g, 'feature-a', new Map())
                if (!owned.candidateNotes.some((f) => f.endsWith('branch-feature-workstream.md'))) return false
                const gap = b.trackingGap(owned)
                if (!gap || !gap.remedy.includes('branch-feature-workstream.md')) return false
                // An unrelated branch shares no token, so it gets a filename to write instead.
                const orphan = b.branchView(g, 'stranded-work', new Map())
                return orphan.candidateNotes.length === 0
                    && b.trackingGap(orphan).remedy.includes('branch-stranded-work.md')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'branches.mjs'),
            '        return shared.some((t) => t.length >= 6) || shared.length >= 2',
            '        return false'),
    },
    {
        id: 'a-cache-keyed-on-its-shape-cannot-serve-a-stale-shape',
        why: 'widening the PR field set silently served answers that lacked the new field until the TTL expired',
        // REGRESSION TEST FOR A LIVE MISS. Adding `baseRefName` to the PR query made every branch
        // look unexplained: the code read the field, the cached answer predated it, and nothing said
        // so. The key is the field set, so a widened query is a miss rather than a wrong answer.
        run: async (binDir) => {
            const c = await import(pathToFileURL(join(binDir, 'lib', 'cache.mjs')).href)
            // A FIXED name, not one per pid: keying the file by process id left one 52-byte orphan
            // per suite run - thirteen of them before this was noticed, by the very status command
            // this check exists alongside. The self-test must not litter the thing it tests.
            const name = 'selftest.json'
            c.cacheWrite(name, [1, 2, 3], 'shape-a')
            if (JSON.stringify(c.cacheRead(name, { key: 'shape-a' })) !== '[1,2,3]') return false
            return c.cacheRead(name, { key: 'shape-b' }) === null
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'cache.mjs'),
            '        if (key !== undefined && raw.key !== key) return null',
            '        if (false) return null'),
    },
    {
        id: 'perf-reports-to-stderr-and-never-alters-stdout',
        why: 'a diagnostic flag that changes the answer is worse than no diagnostic, and callers pipe stdout',
        run: async (binDir) => {
            const plain = spawnSync(process.execPath, [join(binDir, 'inflight.mjs'), 'help'], { encoding: 'utf8' })
            const perf = spawnSync(process.execPath, [join(binDir, 'inflight.mjs'), '--perf', 'help'], { encoding: 'utf8' })
            // Same answer, same exit code - the flag is stripped before any command sees it.
            if (plain.stdout !== perf.stdout || plain.status !== perf.status) return false
            // The report goes to stderr, and only when asked.
            return perf.stderr.includes('perf:') && !plain.stderr.includes('perf:')
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            "    if (perf) console.error(perfReport())",
            "    if (perf) console.log(perfReport())"),
    },
    {
        id: 'perf-counts-subprocesses-rather-than-guessing',
        why: 'the cost here is the call COUNT, so a report that does not count calls answers the wrong question',
        run: async (binDir) => {
            const perf = await import(pathToFileURL(join(binDir, 'lib', 'perf.mjs')).href)
            const g = await import(pathToFileURL(join(binDir, 'lib', 'git.mjs')).href)
            // Reset first: the recorder is module state shared with every earlier check, so without
            // this the counts asserted below are whatever those checks left behind.
            perf.perfReset()
            return inFixture(() => {
                g.refTips(); g.refTips(); g.baseline()
                const report = perf.perfReport()
                // Two for-each-ref calls must be counted as two, not one and not three.
                return /git for-each-ref\s+2 call/.test(report) && report.includes('git rev-parse')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'perf.mjs'),
            '    e.n += 1', '    e.n = 1'),
    },
    {
        id: 'a-flake-candidate-needs-two-different-outcomes',
        why: 'a test that failed twice is not flaky, and calling it flaky is how a real regression gets quarantined',
        run: async (binDir) => {
            const cc = await import(pathToFileURL(join(binDir, 'lib', 'codecov.mjs')).href)
            // Three tests, one of each shape the API can hand back. Fixture rather than network:
            // an analysis only reachable through a live API is one nothing can check offline.
            const rows = [
                { computed_name: 'A::steady', commit_sha: 'aaa1111', outcome: 'pass', duration_seconds: 1, timestamp: '2026-09-01T01:00:00Z' },
                { computed_name: 'A::steady', commit_sha: 'bbb2222', outcome: 'pass', duration_seconds: 1, timestamp: '2026-09-02T01:00:00Z' },
                { computed_name: 'B::broke', commit_sha: 'aaa1111', outcome: 'pass', duration_seconds: 1, timestamp: '2026-09-01T01:00:00Z' },
                { computed_name: 'B::broke', commit_sha: 'bbb2222', outcome: 'failure', duration_seconds: 1, timestamp: '2026-09-02T01:00:00Z' },
                { computed_name: 'C::alwaysRed', commit_sha: 'aaa1111', outcome: 'failure', duration_seconds: 1, timestamp: '2026-09-01T01:00:00Z' },
                { computed_name: 'C::alwaysRed', commit_sha: 'bbb2222', outcome: 'failure', duration_seconds: 1, timestamp: '2026-09-02T01:00:00Z' },
            ]
            const names = cc.flakesFrom(rows).map((c) => c.name)
            // Only B changed outcome. A never failed; C never passed, so it is simply broken.
            return names.length === 1 && names[0] === 'B::broke'
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'codecov.mjs'),
            '        if (outcomes.size > 1) {', '        if (outcomes.size >= 1) {'),
    },
    {
        id: 'a-timeline-is-newest-first-so-the-change-point-is-readable',
        why: 'the whole question is WHICH commit it changed at, and an unordered list cannot answer it',
        run: async (binDir) => {
            const cc = await import(pathToFileURL(join(binDir, 'lib', 'codecov.mjs')).href)
            // Deliberately fed oldest-first, so passing proves the sort ran rather than the input order.
            const rows = [
                { computed_name: 'X::t', commit_sha: 'old0000', outcome: 'pass', timestamp: '2026-09-01T01:00:00Z' },
                { computed_name: 'X::t', commit_sha: 'mid0000', outcome: 'pass', timestamp: '2026-09-02T01:00:00Z' },
                { computed_name: 'X::t', commit_sha: 'new0000', outcome: 'failure', timestamp: '2026-09-03T01:00:00Z' },
            ]
            const obs = cc.timelineFrom(rows, 'x::t').matches[0].observations
            return obs[0].sha === 'new0000' && obs[2].sha === 'old0000'
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'codecov.mjs'),
            'for (const obs of m.values()) obs.sort((a, b) => String(b.at).localeCompare(String(a.at)))',
            'for (const obs of m.values()) obs.sort((a, b) => String(a.at).localeCompare(String(b.at)))'),
    },
    {
        id: 'a-search-that-matched-nothing-is-not-a-search-that-failed',
        why: 'this repo has been bitten repeatedly by a false negative wearing the authority of a completed check',
        run: async (binDir) => {
            const cc = await import(pathToFileURL(join(binDir, 'lib', 'codecov.mjs')).href)
            const t = cc.timelineFrom([{ computed_name: 'Only::one', commit_sha: 'a', outcome: 'pass', timestamp: '2026-09-01T01:00:00Z' }], 'nothingmatchesthis')
            // An empty match list, and a corpus size proving there WAS something to search.
            return t.matches.length === 0 && t.corpus === 1
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'codecov.mjs'),
            '    const hits = [...all.entries()].filter(([name]) => name.toLowerCase().includes(q))',
            '    const hits = [...all.entries()]'),
    },
    {
        id: 'codecov-is-reachable-from-the-front-door',
        why: 'bin/inflight.mjs exists because a tool nobody can name is indistinguishable from one that does not exist',
        run: async (binDir) => {
            const names = await registeredNames(binDir)
            return ['codecov', 'codecov test', 'codecov flaky', 'codecov slow'].every((n) => names.includes(n))
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            "        name: 'codecov',", "        name: 'codecovv',"),
    },
    {
        id: 'a-flag-value-is-not-the-search-term',
        why: 'the flag VALUE does not start with -- either, so it was taken as the query - and the fix for THAT dropped the query whenever the flag was absent',
        run: async (binDir) => {
            // PURE, AND NOT THROUGH THE CLI. The first cut of this check ran `inflight codecov
            // test` as a subprocess, which reaches api.codecov.io - in a file whose header says
            // "Network: none" precisely so a rate limit or an outage cannot flake CI. It also only
            // ever tried the --branch form, which is exactly why the sentinel bug below shipped.
            const { cvOpts } = await front(binDir)
            const bare = cvOpts(['ZZQuery'])
            const first = cvOpts(['--branch', 'refs/heads/nope', 'ZZQuery'])
            const last = cvOpts(['ZZQuery', '--branch', 'refs/heads/nope'])
            const numeric = cvOpts(['3'])
            return (
                // -1 is a sentinel, not an index: with no flag, nothing may be excluded.
                bare.rest[0] === 'ZZQuery' && bare.branch === undefined
                && numeric.rest[0] === '3'
                // the flag's VALUE is never the query, whichever side it sits on
                && first.rest[0] === 'ZZQuery' && first.branch === 'refs/heads/nope'
                && last.rest[0] === 'ZZQuery' && last.branch === 'refs/heads/nope'
            )
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            '    const branchValueAt = branchAt >= 0 ? branchAt + 1 : -1',
            '    const branchValueAt = branchAt + 1'),
    },
    {
        id: 'the-latest-timed-observation-wins-not-the-first-or-the-max',
        why: 'slow answers "what owns the wall-clock NOW", so an old slow run or a recent untimed one must not decide it',
        run: async (binDir) => {
            const cc = await import(pathToFileURL(join(binDir, 'lib', 'codecov.mjs')).href)
            // Newest observation carries NO duration; the next one down does. The answer must be
            // 5s (the latest TIMED one), not 99s (the max) and not undefined (the latest).
            const rows = [
                { computed_name: 'T::x', commit_sha: 'c3', outcome: 'pass', timestamp: '2026-09-03T00:00:00Z' },
                { computed_name: 'T::x', commit_sha: 'c2', outcome: 'pass', duration_seconds: 5, timestamp: '2026-09-02T00:00:00Z' },
                { computed_name: 'T::x', commit_sha: 'c1', outcome: 'pass', duration_seconds: 99, timestamp: '2026-09-01T00:00:00Z' },
            ]
            const r = cc.slowestFrom(rows, 10)
            return r.rows.length === 1 && r.rows[0].seconds === 5 && r.rows[0].sha === 'c2'
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'codecov.mjs'),
            "        const latest = observations.find((o) => typeof o.seconds === 'number')",
            '        const latest = observations[0]'),
    },
    {
        id: 'a-skip-is-not-a-flake',
        why: 'an assumption-skipped test alternating skip/pass is not flaky, and counting skips as failures also skewed the sort',
        run: async (binDir) => {
            const cc = await import(pathToFileURL(join(binDir, 'lib', 'codecov.mjs')).href)
            const rows = [
                { computed_name: 'S::skips', commit_sha: 'a', outcome: 'skip', timestamp: '2026-09-01T00:00:00Z' },
                { computed_name: 'S::skips', commit_sha: 'b', outcome: 'pass', timestamp: '2026-09-02T00:00:00Z' },
                { computed_name: 'R::real', commit_sha: 'a', outcome: 'pass', timestamp: '2026-09-01T00:00:00Z' },
                { computed_name: 'R::real', commit_sha: 'b', outcome: 'failure', timestamp: '2026-09-02T00:00:00Z' },
            ]
            const names = cc.flakesFrom(rows).map((c) => c.name)
            return names.length === 1 && names[0] === 'R::real'
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'codecov.mjs'),
            "        const ran = observations.filter((o) => o.outcome !== 'skip')",
            '        const ran = observations'),
    },
    {
        id: 'an-empty-unfiltered-corpus-is-a-failure-not-a-finding',
        why: 'a transient 200 with no rows made `flaky` exit 0 on a clean negative, in the one situation someone uses it to REMOVE a quarantine',
        run: async (binDir) => {
            const src = readFileSync(join(binDir, 'lib', 'codecov.mjs'), 'utf8')
            // The guard has to be scoped to the UNFILTERED query: a branch-scoped miss is a real
            // empty answer (that branch never uploaded), so guarding it too would break `--branch`.
            return /results\.length === 0 && !branch/.test(src)
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'codecov.mjs'),
            '    if (results.length === 0 && !branch) {',
            '    if (false) {'),
    },
    {
        id: 'a-bad-option-set-is-refused-not-answered',
        why: '--branch with no value silently queried every branch, and --branch --fresh queried a branch named --fresh - both answered convincingly',
        run: async (binDir) => {
            const { cvOpts } = await front(binDir)
            return (
                cvOpts(['--branch']).error !== undefined
                && cvOpts(['--branch', '--fresh', 'X']).error !== undefined
                && cvOpts(['--nope']).error !== undefined
                // and a VALID set still parses, or the guard has eaten the feature
                && cvOpts(['Name', '--branch', 'master', '--fresh']).error === undefined
                && cvOpts(['Name', '--branch', 'master']).branch === 'master'
                // a REPEATED flag is ambiguous, not first-wins
                && cvOpts(['--branch', 'a', '--branch', 'b']).error !== undefined
                && cvOpts(['--fresh', '--fresh']).error !== undefined
            )
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            "    if (unknown.length) return { error: `unknown option(s): ${unknown.join(', ')} - known: --fresh, --branch <ref>` }",
            '    if (false) return { error: 0 }'),
    },
    {
        id: 'a-narrow-fetch-cannot-look-fresh',
        why: 'a one-ref fetch resets FETCH_HEAD mtime, silencing the staleness warning over a corpus that is still stale',
        // MEASURED BEFORE IT WAS WRITTEN: mtime forced to 2020, `git fetch origin master`, mtime
        // now. The check read age alone, so the commonest fetch an agent runs - one branch, to
        // update the base - silenced the only warning that says this corpus is old. A full fetch
        // lists every ref it covered even when none of them moved, so width is readable from the
        // same file rather than guessed.
        run: async (binDir) => {
            const g = await gitlib(binDir)
            const dir = buildFetchFixture()
            const before = cwd()
            try {
                chdir(dir)
                const ids = () => g.freshnessWarnings(g.baseline(), g.refTips().tips.length).map((w) => w.id)
                spawnSync('git', ['fetch', '-q', 'origin', 'master'], { cwd: dir })
                if (!ids().includes('narrow-fetch')) return false
                // ...and a FULL fetch of the same repo must not, or the warning is only noise.
                spawnSync('git', ['fetch', '-q', 'origin'], { cwd: dir })
                return !ids().includes('narrow-fetch')
            } finally { chdir(before) }
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'git.mjs'),
            'if (last.refs !== null && last.refs * 4 < refCount) {',
            'if (false && last.refs !== null && last.refs * 4 < refCount) {'),
    },
    {
        id: 'a-fresh-clone-is-not-a-never-fetched-clone',
        why: 'the newest corpus obtainable was told it may never have fetched - the opposite error, and the loudest one',
        // `git clone` writes no FETCH_HEAD at all, so keying "never fetched" on that file's absence
        // fires hardest on the freshest possible checkout. packed-refs is written BY the clone, so
        // its mtime dates the refs actually held.
        run: async (binDir) => {
            const g = await gitlib(binDir)
            const dir = buildFetchFixture()
            const before = cwd()
            try {
                chdir(dir)
                const ids = g.freshnessWarnings(g.baseline(), g.refTips().tips.length).map((w) => w.id)
                // A clone this new is neither never-fetched nor stale - both claims it used to make.
                return !ids.includes('never-fetched') && !ids.includes('stale-fetch')
            } finally { chdir(before) }
        },
        // Reverts to keying on FETCH_HEAD alone - the actual pre-fix behaviour - rather than
        // deleting the fallback, which would prove only that some fallback existed.
        mutate: (binDir) => patch(join(binDir, 'lib', 'git.mjs'),
            "        return { at: statSync(`${commonDir}/packed-refs`).mtimeMs, refs: null, source: 'packed-refs' }",
            '        return null'),
    },
    {
        id: 'origin-head-is-not-a-branch-tip',
        why: 'git shortens refs/remotes/origin/HEAD to plain "origin", so a /HEAD filter on the short name never fires',
        // It entered the corpus as a ref named `origin` carrying a duplicate of origin/master's
        // tip - so every count was one high, and `note find` could name "origin" as a branch
        // carrying a note. The filter existed and looked right; it was reading the wrong string.
        run: async (binDir) => {
            const g = await gitlib(binDir)
            const dir = buildFetchFixture()
            const before = cwd()
            try {
                chdir(dir)
                const tips = g.refTips()
                if (!tips.ok) return false
                // A clone always has refs/remotes/origin/HEAD, so this fixture always exercises it.
                if (tips.tips.some((r) => r.ref === 'origin' || r.ref.endsWith('/HEAD'))) return false
                // ...and the real branches must survive the filter, or it is just deleting refs.
                return tips.tips.some((r) => r.ref === 'origin/master')
            } finally { chdir(before) }
        },
        // Reverts to filtering the SHORT name - the actual pre-fix bug - rather than removing the
        // filter, which would prove only that some filter ran.
        mutate: (binDir) => patch(join(binDir, 'lib', 'git.mjs'),
            "            .filter(([, , full]) => !full.endsWith('/HEAD') && full !== 'refs/stash')",
            "            .filter((r) => true)"),
    },
    {
        id: 'freshness-reads-this-worktree-not-the-main-checkout',
        why: 'FETCH_HEAD is per-worktree, so reading only the common dir answered with the main checkout - the one place AGENTS.md says never to work',
        // The refs a fetch updates ARE shared, so a fetch in any worktree refreshes what this search
        // reads; the answer is the newest FETCH_HEAD across all of them. Measured before the fix: a
        // fetch in this worktree, and the check still reported the main checkout's, four hours older.
        run: async (binDir) => {
            const g = await gitlib(binDir)
            const dir = buildFetchFixture()
            const git = (where, ...args) => spawnSync('git', args, { cwd: where, encoding: 'utf8' })
            const before = cwd()
            try {
                // The common dir fetches, then is aged past the one-hour threshold.
                git(dir, 'fetch', '-q', 'origin')
                // AGED IN NODE, NOT BY `touch -d`. `-d 2020-01-01` is a GNU spelling; BSD touch -
                // macOS - rejects it as "illegal time specification" and exits 1, and nothing here
                // read that status. The file kept its real mtime, the common dir looked freshly
                // fetched, and the mutant below - which reverts to reading the common dir alone -
                // reported no staleness and stayed GREEN. Same class as the guard this commit
                // fixes: a step that did not run, scored as one that ran and agreed. utimesSync is
                // cross-platform and throws rather than exiting into a status nobody checks.
                const aged = new Date('2020-01-01T00:00:00Z')
                utimesSync(join(dir, '.git', 'FETCH_HEAD'), aged, aged)
                const wt = join(dir, 'wt')
                git(dir, 'worktree', 'add', '-q', '-b', 'wt-branch', wt)
                git(wt, 'fetch', '-q', 'origin')
                chdir(wt)
                const ids = g.freshnessWarnings(g.baseline(), g.refTips().tips.length).map((w) => w.id)
                // This worktree fetched seconds ago; only the main checkout's copy is from 2020.
                return !ids.includes('stale-fetch') && !ids.includes('never-fetched')
            } finally { chdir(before) }
        },
        // Reverts to consulting the common dir alone - the actual pre-fix behaviour - by pointing
        // the worktree scan at a directory that does not exist.
        mutate: (binDir) => patch(join(binDir, 'lib', 'git.mjs'),
            'for (const w of readdirSync(`${commonDir}/worktrees`))',
            'for (const w of readdirSync(`${commonDir}/no-such-worktrees-dir`))'),
    },
    {
        id: 'the-cache-layer-owns-freshness-not-its-callers',
        why: 'a policy stated at each read site is a cache with several answers, and nothing reports the difference',
        // This is the property that let an external hook be deleted. The layer refuses to store an
        // absence for kinds whose policy says so, and it does it in cacheWrite - so a caller added
        // later inherits the decision instead of having to remember it.
        run: async (binDir) => {
            const c = await import(pathToFileURL(join(binDir, 'lib', 'cache.mjs')).href)
            const dir = mkdtempSync(join(tmpdir(), 'inflight-cachepolicy-'))
            const envBefore = process.env.PC_INFLIGHT_CACHE_DIR
            try {
                process.env.PC_INFLIGHT_CACHE_DIR = dir
                // A kind whose policy refuses empties: the write is dropped, so the read is a miss
                // and the next call goes to the network - which is how a PR opened anywhere is seen.
                if (c.policyFor('pr-branch.json').cacheEmpty !== false) return false
                c.cacheWrite('pr-branch.json', [], 'head:some-branch')
                if (c.cacheRead('pr-branch.json', { key: 'head:some-branch' }) !== null) return false
                // ...and a real answer for the same kind IS stored, or the guard has just turned the
                // cache off rather than shaped it.
                c.cacheWrite('pr-branch.json', [{ number: 1 }], 'head:some-branch')
                const back = c.cacheRead('pr-branch.json', { key: 'head:some-branch' })
                if (!Array.isArray(back) || back.length !== 1) return false
                // A kind whose policy allows empties is unaffected - the bulk listing legitimately
                // caches "no PRs at all", and one policy must not silently become the other's.
                return c.policyFor('prs.json').cacheEmpty === true
            } finally {
                if (envBefore === undefined) delete process.env.PC_INFLIGHT_CACHE_DIR
                else process.env.PC_INFLIGHT_CACHE_DIR = envBefore
            }
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'cache.mjs'),
            "    if (!policyFor(name).cacheEmpty && Array.isArray(value) && value.length === 0) return",
            '    // policy removed'),
    },
    {
        id: 'absence-from-the-pr-snapshot-is-asked-again',
        why: 'a branch missing from a 24h listing is not a branch without a PR, and treating them alike is what the deleted hook patched',
        // Asserted on the SOURCE OF THE ANSWER rather than on a network result: branchView must not
        // be able to return `pr: null` straight from a snapshot miss. Driven with a fake `gh` so
        // both outcomes are reachable with no network and no dependence on gh being authed.
        run: async (binDir) => {
            const b = await branches(binDir)
            const root = mkdtempSync(join(tmpdir(), 'inflight-fallthrough-'))
            const fakeBin = join(root, 'fakebin')
            mkdirSync(fakeBin, { recursive: true })
            writeFileSync(join(fakeBin, 'gh'),
                '#!/bin/sh\ncat <<\'JSON\'\n[{"headRefName":"diverged","baseRefName":"master","number":7,"title":"found by fall-through","state":"OPEN"}]\nJSON\n',
                { mode: 0o755 })
            const pathBefore = process.env.PATH
            const envBefore = process.env.PC_INFLIGHT_CACHE_DIR
            return inFixture(() => {
                try {
                    process.env.PATH = `${fakeBin}:${pathBefore}`
                    process.env.PC_INFLIGHT_CACHE_DIR = join(root, 'cache')
                    const graph = b.commitGraph()
                    if (!graph.ok) return false
                    // An EMPTY snapshot - the branch is absent from it, which is exactly the case
                    // the deleted hook existed to paper over.
                    const view = b.branchView(graph, 'diverged', new Map())
                    return view.pr !== null && view.pr.number === 7
                } finally {
                    process.env.PATH = pathBefore
                    if (envBefore === undefined) delete process.env.PC_INFLIGHT_CACHE_DIR
                    else process.env.PC_INFLIGHT_CACHE_DIR = envBefore
                }
            })
        },
        // Reverts to reading the snapshot alone - the actual pre-fix behaviour.
        mutate: (binDir) => patch(join(binDir, 'lib', 'branches.mjs'),
            "        pr: prs.get(ref.replace(/^origin\\//, '')) ?? prForBranch(ref).pr,",
            "        pr: prs.get(ref.replace(/^origin\\//, '')) ?? null,"),
    },
    {
        id: 'the-known-cache-list-is-derived-not-repeated',
        why: 'it was listed twice and the copies drifted, so `cache` called the live file an orphan and the dead file live',
        // The inverted report is the whole failure: that view exists to tell a leftover from a live
        // cache, and a stale second copy made it confidently wrong in both directions at once.
        run: async (binDir) => {
            const c = await import(pathToFileURL(join(binDir, 'lib', 'cache.mjs')).href)
            const known = c.knownCaches()
            if (!Array.isArray(known) || known.length === 0) return false
            // Every known name must carry a policy - that is what "derived" means here.
            if (!known.every((n) => c.policyFor(n) && typeof c.policyFor(n).maxAgeMs === 'number')) return false
            // The front door must not carry its own copy: a literal list of cache filenames there
            // is the duplication this replaced, whatever its variable is called.
            const front = readFileSync(join(binDir, 'inflight.mjs'), 'utf8')
            return !/\[\s*'[a-z-]+\.json'\s*,/.test(front)
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            '            const known = knownCaches()',
            "            const known = ['prs.json', 'pr-search.json']"),
    },
    {
        id: 'the-corpus-looks-everywhere-not-just-at-branches',
        why: 'tags and refs/backup were outside it while the help text said "every branch tip", and that is where work is parked before a re-cut',
        // Nothing was ever blacklisted - `for-each-ref` was simply given two patterns. The fixture
        // grows a tag and a refs/backup ref holding content no branch has, which is the shape of
        // the 12 tags in this repository that point at commits reachable from nothing else.
        run: async (binDir) => {
            const g = await gitlib(binDir)
            return inFixture((dir) => {
                const git = (...a) => spawnSync('git', a, { cwd: dir, encoding: 'utf8' })
                git('tag', 'backup/pre-recut-probe', 'diverged')
                git('update-ref', 'refs/backup/probe', 'stranded-work')
                const tips = g.refTips()
                if (!tips.ok) return false
                const kinds = new Map(tips.tips.map((t) => [t.ref, t]))
                const tag = kinds.get('backup/pre-recut-probe')
                const backup = [...kinds.values()].find((t) => t.full === 'refs/backup/probe')
                if (!tag || !backup) return false
                // Present AND labelled - the label is what stops preserved work being reported as
                // stranded, so finding them without it would be the wrong fix.
                return tag.kind === 'tag' && tag.archival === true
                    && backup.kind === 'archive' && backup.archival === true
                    && kinds.get('master')?.archival === false
            })
        },
        // Reverts to the two patterns this replaced - the actual pre-fix enumeration.
        mutate: (binDir) => patch(join(binDir, 'lib', 'git.mjs'),
            "        '--format=%(objectname)\\t%(*objectname)\\t%(refname)\\t%(refname:short)'])",
            "        '--format=%(objectname)\\t%(*objectname)\\t%(refname)\\t%(refname:short)',\n        'refs/heads', 'refs/remotes/origin'])"),
    },
    {
        id: 'archive-only-work-is-not-reported-as-stranded',
        why: 'a note parked in a tag before a re-cut is preserved on purpose; a remedy telling someone to rescue it is a wrong answer',
        // The half that makes widening safe. Without it, looking everywhere turns every preserved
        // ref into a finding, and the report gets less trustworthy for having more data in it.
        run: async (binDir) => {
            const n = await notes(binDir)
            return inFixture((dir) => {
                const git = (...a) => spawnSync('git', a, { cwd: dir, encoding: 'utf8' })
                // `stranded-work` carries two notes master has never had. Park a copy in an archive
                // ref and delete nothing: the live branch still has them, so the cluster stays live.
                git('update-ref', 'refs/backup/parked', 'stranded-work')
                const clusters = n.stranded(n.corpusIndex())
                const live = clusters.find((c) => c.paths.includes('docs/inflight/never-landed.md'))
                if (!live || live.preserved !== false) return false
                if (!live.liveRefs.includes('stranded-work')) return false
                // Now a ref that exists ONLY in the archive space, carrying a note nothing else has.
                git('checkout', '-q', '-b', 'to-be-archived', 'master')
                mkdirSync(join(dir, 'docs', 'inflight'), { recursive: true })
                writeFileSync(join(dir, 'docs', 'inflight', 'parked-only.md'),
                    '# Parked\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\nz\n')
                git('add', '-A')
                git('-c', 'user.email=t@t', '-c', 'user.name=t', 'commit', '-q', '-m', 'park it')
                git('update-ref', 'refs/backup/only-here', 'to-be-archived')
                git('checkout', '-q', 'master')
                git('branch', '-q', '-D', 'to-be-archived')
                const after = n.stranded(n.corpusIndex())
                const parked = after.find((c) => c.paths.includes('docs/inflight/parked-only.md'))
                // Found - because the corpus looks everywhere - and marked preserved, not stranded.
                return !!parked && parked.preserved === true && parked.liveRefs.length === 0
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            '                preserved: live.length === 0,',
            '                preserved: false,'),
    },
    {
        id: 'repository-facts-live-in-one-module',
        why: 'REPO was declared three times while the note above it said it was a single constant, which is the drift class that already shipped one bug here',
        // A copied constant is correct until exactly one copy changes, and nothing goes red at that
        // moment. Asserted structurally rather than by value: what matters is that there is one
        // declaration, not what it currently says.
        run: async (binDir) => {
            const r = await import(pathToFileURL(join(binDir, 'lib', 'repo.mjs')).href)
            if (typeof r.REPO !== 'string' || !r.REPO.includes('/')) return false
            if (r.NOTES_DIR !== 'docs/inflight') return false
            const declarations = readdirSync(join(binDir, 'lib'))
                .filter((f) => f.endsWith('.mjs'))
                .filter((f) => /^(export )?const (REPO|NOTES_DIR) = '/m.test(readFileSync(join(binDir, 'lib', f), 'utf8')))
            // repo.mjs, and nothing else.
            return declarations.length === 1 && declarations[0] === 'repo.mjs'
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'branches.mjs'),
            "import { NOTES_DIR, REPO } from './repo.mjs'",
            "const REPO = 'astubbs/parallel-consumer'\nimport { NOTES_DIR } from './repo.mjs'"),
    },
    // ---------------------------------------------------------------------------------------------
    // THE DOCUMENT CONTEXT QUERY - one `drift` at two costs, over three docs areas and every ref.
    // docs/plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md, unit U1.
    // ---------------------------------------------------------------------------------------------
    {
        id: 'summary-tier-answers-with-one-merge-base',
        why: 'the read-time hook runs on every docs read inside a 500ms budget; a summary that quietly does the full work per cluster blows it on a busy note',
        run: async (binDir) => {
            const n = await notes(binDir)
            const perf = await perfOf(binDir)
            return inDir(docsFixture(), () => {
                const full = n.drift('docs/inflight/note.md', { prs: new Map(), at: { ref: 'adds-heading' } })
                if (!full.found || full.divergent.length === 0) return false
                perf.perfReset()
                const s = n.drift('docs/inflight/note.md', { detail: 'summary', at: { ref: 'adds-heading' } })
                const report = perf.perfReport()
                if (!s.found || s.detail !== 'summary') return false
                // The copy at hand is this branch's own edit, so its size costs one merge-base and
                // one diff - and no other cluster gets either.
                if (callCount(report, 'git merge-base') !== 1 || callCount(report, 'git diff') !== 1) return false
                if (s.at?.state !== 'own-divergent') return false
                // Same divergent set as the full tier, without the branch facts the full tier adds.
                const same = new Set(full.divergent.map((c) => c.blob))
                return s.divergent.length === same.size && s.divergent.every((c) => same.has(c.blob))
                    && s.divergent.every((c) => c.branches === undefined)
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            "    const summary = detail === 'summary'", '    const summary = false'),
    },
    {
        id: 'preview-shows-added-headings-else-the-first-added-line',
        why: 'the header shows evidence of what a divergent version adds, never a claim that it is newer',
        run: async (binDir) => {
            const n = await notes(binDir)
            return inDir(docsFixture(), () => {
                const d = n.drift('docs/inflight/note.md', { prs: new Map() })
                if (!d.found) return false
                const byRef = (ref) => d.divergent.find((c) => c.refs.includes(ref))
                const heading = byRef('adds-heading')?.preview
                const line = byRef('adds-line')?.preview
                if (!heading || !line) return false
                if (!(heading.headings.length === 1 && heading.headings[0] === '## What the branch learned')) return false
                return line.headings.length === 0 && line.firstLine === 'one plain added line'
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            '        headings: added.filter((l) => /^#{1,6}\\s/.test(l)),',
            '        headings: [],'),
    },
    {
        id: 'a-tag-only-version-is-preserved-not-divergent',
        why: 'a version parked in a tag before a re-cut is preserved on purpose; counting it as divergent sends someone to rescue what nobody lost',
        run: async (binDir) => {
            const n = await notes(binDir)
            return inDir(docsFixture(), () => {
                const d = n.drift('docs/inflight/note.md', { prs: new Map() })
                if (!d.found) return false
                if (d.divergent.some((c) => c.refs.includes('preserved/parked'))) return false
                const parked = d.preserved.find((p) => p.refs.includes('preserved/parked'))
                // Named by its ref KIND, which is what a reader needs to know where to look.
                return !!parked && parked.kinds.includes('tag') && d.divergent.length === 2
                    && d.archivalRefsTotal >= 1 && d.liveRefsTotal + d.archivalRefsTotal === d.refsTotal
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            '        if (live.length === 0) preserved.push', '        if (false) preserved.push'),
    },
    {
        id: 'copy-state-names-baseline-own-divergent-and-branch-only',
        why: 'a branch-only document mistaken for a landed one, or an own edit reported as divergence elsewhere, is the stale-copy incident again',
        run: async (binDir) => {
            const n = await notes(binDir)
            return inDir(docsFixture(), () => {
                const at = (path, ref) => n.drift(path, { prs: new Map(), at: { ref } }).at
                const base = at('docs/inflight/note.md', 'master')
                const own = at('docs/inflight/note.md', 'adds-line')
                const only = at('docs/inflight/branch-only.md', 'only-here')
                if (base?.state !== 'baseline') return false
                if (own?.state !== 'own-divergent' || !(own.added?.added > 0)) return false
                return only?.state === 'branch-only'
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            "    if (blob === baseBlob) return 'baseline'", "    if (blob === baseBlob) return 'own-divergent'"),
    },
    {
        id: 'corpus-index-spans-every-docs-area',
        why: 'every delivery renders the same corpus; an index that reads notes alone hides the solutions and plans that live only on branches',
        run: async (binDir) => {
            const n = await notes(binDir)
            return inDir(docsFixture(), () => {
                const index = n.corpusIndex()
                if (!index.ok) return false
                return index.byPath.has('docs/solutions/ci/sol.md')
                    && index.byPath.has('docs/plans/2026-01-01-001-plan.md')
                    && index.byPath.has('docs/inflight/note.md')
                    && index.byPath.has('docs/inflight/branch-only.md')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            'export function corpusIndex({ areas = DOC_AREAS } = {})',
            'export function corpusIndex({ areas = DOC_AREAS.filter((a) => a.dir === NOTES_DIR) } = {})'),
    },
    {
        id: 'note-find-still-reads-notes-only',
        why: '`note find` is a question about in-flight notes; widening the default index must not change its answer',
        run: async (binDir) => {
            const r = invoke(binDir, ['note', 'find', 'md'], { cwd: docsFixture() })
            return r0(r) && r.out.includes('docs/inflight/note.md') && !r.out.includes('docs/solutions/')
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            '                    const index = corpusIndex({ areas: NOTES_AREA })\n'
            + '                    if (!index.ok) return { ok: false, reason: `note find:',
            '                    const index = corpusIndex()\n'
            + '                    if (!index.ok) return { ok: false, reason: `note find:'),
    },
    {
        id: 'prior-art-sections-derive-from-doc-areas',
        why: 'the section list was hard-coded in prior-art.mjs, and a second copy of the area list would drift the way REPO did',
        run: async (binDir) => {
            const p = await lib(binDir)
            return inDir(docsFixture(), () => {
                const r = p.priorArt(['note'], { github: false })
                if (!r.ok || r.sections.length !== 4) return false
                // Byte-identical to the headings the hard-coded list produced, captured before the
                // derivation - the point is that deriving them changed nothing a reader sees.
                const expected = [
                    ['1', 'Prior investigations - docs/plans/', ['docs/plans/']],
                    ['2', 'Solved problems - docs/solutions/', ['docs/solutions/']],
                    ['3', 'In-flight state - docs/inflight/', ['docs/inflight/']],
                    ['4', 'Everything else under docs/',
                        ['docs/', ':(exclude)docs/plans/', ':(exclude)docs/solutions/', ':(exclude)docs/inflight/']],
                ]
                return r.sections.every((s, i) => s.n === expected[i][0] && s.heading === expected[i][1]
                    && JSON.stringify(s.pathspec) === JSON.stringify(expected[i][2]))
                    && r.sections[2].hits.some((h) => h.path === 'docs/inflight/note.md')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'repo.mjs'), "name: 'Solved problems'", "name: 'Solved problem'"),
    },
    {
        id: 'blob-titles-are-read-in-one-batch',
        why: 'one cat-file per blob is a fork per document, and the session-start budget has no room for it',
        run: async (binDir) => {
            const g = await gitlib(binDir)
            const n = await notes(binDir)
            const perf = await perfOf(binDir)
            return inDir(docsFixture(), () => {
                const blobs = g.treeEntries('master', 'docs').entries.map((e) => e.blob)
                if (blobs.length < 3) return false
                perf.perfReset()
                const titles = n.blobTitles(blobs)
                if (callCount(perf.perfReport(), 'git cat-file') !== 1) return false
                if (titles.size !== blobs.length) return false
                return blobs.every((b) => titles.get(b) === n.blobTitle(b))
                    && titles.get(blobs[0]) !== null
            })
        },
        // Forks once per blob, which is the loop the batch replaced.
        mutate: (binDir) => patch(join(binDir, 'lib', 'git.mjs'),
            'export function blobContents(blobs) {',
            'export function blobContents(blobs) {\n'
            + "    return { ok: true, contents: new Map(blobs.map((b) => [b, exec('git', ['cat-file', '-p', b]).out])) }"),
    },
    {
        id: 'corpus-index-lists-each-distinct-docs-tree-once',
        why: 'one ls-tree per ref was the session-start budget; most tips share the baseline docs tree and one listing serves them all',
        run: async (binDir) => {
            const g = await gitlib(binDir)
            const n = await notes(binDir)
            const perf = await perfOf(binDir)
            return inDir(sharedTreesFixture(), () => {
                // The truth the batch must match, computed BEFORE the perf window so its own
                // subprocesses do not count: how many distinct `docs/` tree objects the refs name.
                const refs = g.refTips().tips.map((r) => r.ref)
                const distinct = new Set(refs
                    .map((r) => g.exec('git', ['rev-parse', '-q', '--verify', `${r}:docs`]))
                    .filter((r) => r.ok).map((r) => r.out.trim()))
                // The fixture is only a test of sharing if sharing exists: fewer trees than refs.
                if (refs.length < 6 || distinct.size >= refs.length - 1) return false
                perf.perfReset()
                const index = n.corpusIndex()
                const report = perf.perfReport()
                if (!index.ok) return false
                if (callCount(report, 'git cat-file') !== 1) return false
                return callCount(report, 'git ls-tree') === distinct.size
            })
        },
        // Lists once per ref again: the memo that shares a listing between refs is bypassed.
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            'if (!listed.has(tree)) listed.set(tree, treeEntries(tree, pathspec))',
            'listed.set(tree, treeEntries(tree, pathspec))'),
    },
    {
        id: 'corpus-index-rows-equal-a-per-ref-ls-tree',
        why: 'the shared listing must fan out to exactly the rows each ref would have listed on its own, in order',
        run: async (binDir) => {
            const g = await gitlib(binDir)
            const n = await notes(binDir)
            const { DOC_AREAS } = await import(pathToFileURL(join(binDir, 'lib', 'repo.mjs')).href)
            return inDir(sharedTreesFixture(), () => {
                const dirs = DOC_AREAS.map((a) => a.dir)
                const index = n.corpusIndex()
                if (!index.ok || index.refs.length < 6) return false
                let rows = 0
                for (const { ref } of index.refs) {
                    // The pre-batch code path, ref by ref: paths as ls-tree prints them from the
                    // ref, so a prefix the fan-out dropped or doubled shows here.
                    const truth = g.treeEntries(ref, dirs).entries
                    const got = index.byRef.get(ref) ?? []
                    if (JSON.stringify(got) !== JSON.stringify(truth)) return false
                    rows += got.length
                }
                return rows > 0
            })
        },
        // Hands back the tree-relative path, which is what ls-tree prints for a bare tree SHA.
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            '[e.blob, prefix + e.path]', '[e.blob, e.path]'),
    },
    {
        id: 'corpus-index-treats-a-ref-without-docs-as-empty',
        why: 'an orphan history with no docs/ is an empty corpus, and reading it as a failure would poison the aggregate',
        run: async (binDir) => {
            const n = await notes(binDir)
            return inDir(sharedTreesFixture(), () => {
                const index = n.corpusIndex()
                if (!index.ok) return false
                if (!index.refs.some((r) => r.ref === 'no-docs')) return false
                const rows = index.byRef.get('no-docs')
                return Array.isArray(rows) && rows.length === 0 && index.unreadableRefs.length === 0
            })
        },
        // A missing tree counts as an unreadable ref.
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            'if (!tree) return [ref, []]',
            'if (!tree) { unreadable.push(ref); return [ref, []] }'),
    },
    {
        id: 'source-frame-puts-the-label-first-and-the-command-last',
        why: 'an agent tells a fresh signal from a repeat by its first line, and always needs the next command',
        run: async (binDir) => {
            const v = await docsViews(binDir)
            const cases = [
                ['header', 'docs/inflight/x.md', 'docs context: divergence header for docs/inflight/x.md'],
                ['terms', ['RetryQueue', 'writeLock'], 'docs context: prompt terms RetryQueue, writeLock'],
                ['branch', null, 'docs context: branch facts'],
                ['index', null, 'docs context: session index'],
            ]
            return cases.every(([kind, subject, label]) => {
                const framed = v.sourceFrame(kind, subject, 'body line one\nbody line two', 'bin/inflight.mjs docs')
                const ls = framed.split('\n')
                return ls[0] === label && ls[ls.length - 1] === 'more: bin/inflight.mjs docs'
                    && ls.includes('body line two')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-views.mjs'),
            '    return [label, body.trimEnd(), `more: ${moreCommand}`].join(\'\\n\')',
            '    return [`more: ${moreCommand}`, body.trimEnd(), label].join(\'\\n\')'),
    },
    {
        id: 'the-divergence-header-previews-and-names-the-rest-command',
        why: 'a header that shows counts without the evidence or the next command is an assertion, not a finding',
        run: async (binDir) => {
            const n = await notes(binDir)
            const v = await docsViews(binDir)
            return inDir(docsFixture(), () => {
                const full = n.drift('docs/inflight/note.md', { prs: new Map(), at: { ref: 'master' } })
                const box = v.formatDivergenceHeader(full, { tier: 'full' })
                if (!box.includes('## What the branch learned') || !box.includes('one plain added line')) return false
                if (!box.includes('bin/inflight.mjs note drift docs/inflight/note.md')) return false
                if (!box.includes('adds-heading') || !/preserved/.test(box)) return false
                const s = n.drift('docs/inflight/note.md', { detail: 'summary', at: { ref: 'master' } })
                const line = v.formatDivergenceHeader(s, { tier: 'summary' })
                // One line, counting versions and refs, naming the scope and the copy state.
                return line.length > 0 && !line.trim().includes('\n') && /2 divergent versions/.test(line)
                    && /refs searched/.test(line) && /baseline/.test(line) && /1 preserved/.test(line)
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-views.mjs'),
            '    const rest = `bin/inflight.mjs note drift ${d.path}`', "    const rest = 'bin/inflight.mjs note drift'"),
    },
    // ---------------------------------------------------------------------------------------------
    // `docs show` AND `docs header` - the tool's own channel for the divergence header.
    // docs/plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md, unit U3.
    // ---------------------------------------------------------------------------------------------
    {
        id: 'docs-show-prefers-the-baseline-copy-under-a-header-that-matches-note-drift',
        why: 'a document shown from the first branch that happens to carry it, under a header counting something else, is the stale-copy incident with a tool badge on it',
        run: async (binDir) => {
            const n = await notes(binDir)
            const dir = docsShowFixture()
            const path = 'docs/inflight/note.md'
            const r = invoke(binDir, ['docs', 'show', path], { cwd: dir })
            if (r.code !== 0 || !r.out.trim()) return false
            // The first line names the ref shown; the baseline carries this note, so it is master.
            if (!/\bmaster\b/.test(r.out.split('\n')[0])) return false
            const d = await inDir(dir, () => n.drift(path, { prs: new Map() }))
            if (!d.found || !r.out.includes(`${d.divergent.length} divergent versions`)) return false
            const body = bodyShown(r.out, path, 'master')
            // The baseline's copy: the note's own body line, and none of what a branch added.
            return body !== null && body.includes('\nbody\n') && !body.includes('## What the branch learned')
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'),
            '(lookup.blobs.has(base) ? base : liveCarriers[0] ?? null)', '(liveCarriers[0] ?? null)'),
    },
    {
        id: 'docs-show-falls-back-to-the-first-sorted-live-carrier',
        why: 'a note the baseline lacks has no canonical copy; "first in sorted ref order" is the one choice an agent can predict and repeat',
        run: async (binDir) => {
            const dir = docsShowFixture()
            const only = invoke(binDir, ['docs', 'show', 'docs/inflight/branch-only.md'], { cwd: dir })
            if (only.code !== 0 || !/only-here/.test(only.out.split('\n')[0])) return false
            if (!(bodyShown(only.out, 'docs/inflight/branch-only.md', 'only-here') ?? '').includes('# Only here')) return false
            const parked = invoke(binDir, ['docs', 'show', 'docs/inflight/parked.md'], { cwd: dir })
            if (parked.code !== 0 || !/yy-live/.test(parked.out.split('\n')[0])) return false
            return (bodyShown(parked.out, 'docs/inflight/parked.md', 'yy-live') ?? '').includes('first live copy')
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'),
            'const liveCarriers = carriers.filter((r) => liveSet.has(r)).sort()',
            'const liveCarriers = carriers.filter((r) => liveSet.has(r)).sort().reverse()'),
    },
    {
        id: 'docs-show-never-selects-an-archival-carrier-by-default-and-ref-overrides',
        why: 'a tag is where this repository parks work before a re-cut; showing it as the document is presenting preserved history as the live one',
        run: async (binDir) => {
            const dir = docsShowFixture()
            const tagOnly = 'docs/inflight/tag-only.md'
            const parked = invoke(binDir, ['docs', 'show', tagOnly], { cwd: dir })
            // Ran, named the archival carrier and the flag that reaches it, and showed no body.
            if (parked.code !== 0 || !parked.out.includes('aa-tagonly') || !parked.out.includes('--ref')) return false
            if (bodyShown(parked.out, tagOnly, 'aa-tagonly') !== null) return false
            const asked = invoke(binDir, ['docs', 'show', tagOnly, '--ref', 'aa-tagonly'], { cwd: dir })
            if (asked.code !== 0 || !(bodyShown(asked.out, tagOnly, 'aa-tagonly') ?? '').includes('# Tag only')) return false
            const other = invoke(binDir, ['docs', 'show', 'docs/inflight/note.md', '--ref', 'adds-heading'], { cwd: dir })
            if (other.code !== 0 || !/adds-heading/.test(other.out.split('\n')[0])) return false
            return (bodyShown(other.out, 'docs/inflight/note.md', 'adds-heading') ?? '').includes('## What the branch learned')
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'),
            'const liveCarriers = carriers.filter((r) => liveSet.has(r)).sort()',
            'const liveCarriers = carriers.filter(() => true).sort()'),
    },
    {
        id: 'docs-header-is-docs-show-header-only',
        why: 'the hook names docs header as its more command; if that prints a different header from docs show, the two channels disagree about the same file',
        run: async (binDir) => {
            const dir = docsShowFixture()
            const path = 'docs/inflight/note.md'
            const header = invoke(binDir, ['docs', 'header', path], { cwd: dir })
            const show = invoke(binDir, ['docs', 'show', path, '--header-only'], { cwd: dir })
            if (header.code !== 0 || show.code !== 0 || !header.out.trim()) return false
            // A header, and only a header: no document body follows either.
            if (!header.out.includes('=== divergence:') || bodyShown(header.out, path, 'master') !== null) return false
            return header.out === show.out
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'),
            "export const showHeader = (args, emit) => showDocument([...args, '--header-only'], emit)",
            'export const showHeader = (args, emit) => showDocument(args, emit)'),
    },
    {
        id: 'docs-show-says-what-it-searched-and-cannot-run-outside-a-repo',
        why: 'a path on no ref is a result and must name the refs it covered; a repository git cannot read is not, and exit 0 there is the worst failure class this tool has',
        run: async (binDir) => {
            const dir = docsShowFixture()
            const missing = invoke(binDir, ['docs', 'show', 'docs/inflight/never-written.md'], { cwd: dir })
            if (missing.code !== 0 || !/refs searched/.test(missing.out)) return false
            const outside = mkdtempSync(join(tmpdir(), 'inflight-docs-notarepo-'))
            return [['docs', 'show', 'docs/inflight/x.md'], ['docs', 'header', 'docs/inflight/x.md']]
                .every((args) => {
                    const r = invoke(binDir, args, { cwd: outside })
                    return r.code === 2 && r.out.trim().length > 0
                })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'),
            "if (!tips.ok) return { ok: false, reason: 'docs show: cannot list refs - is this a git repository?' }",
            'if (!tips.ok) return { ok: true }'),
    },
    // MALFORMED ARGUMENTS. Each validation branch refuses with the reason the code states, verbatim,
    // and exit 2 - and each has a mutant, because a typo that lands in a wrong answer instead of a
    // refusal is the answer-a-different-question class cvOpts was hardened against. The docs family
    // never passes through cvOpts, so these branches are the only guard.
    {
        id: 'docs-show-refuses-a-repeated-ref-in-either-order',
        why: 'a second --ref was silently dropped when it followed the path, and when it preceded the path its VALUE became the path and the command said a ref name was outside the areas - the answer-a-different-question shape cvOpts guards against',
        run: async (binDir) => {
            const dir = docsShowFixture()
            const reason = 'docs show: --ref given more than once - which one did you mean?'
            return [
                ['docs', 'show', 'docs/inflight/note.md', '--ref', 'master', '--ref', 'adds-heading'],
                ['docs', 'show', '--ref', 'master', '--ref', 'adds-heading', 'docs/inflight/note.md'],
            ].every((args) => refuses(binDir, args, reason, dir))
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'),
            "if (args.filter((a) => a === '--ref').length > 1) return", 'if (false) return'),
    },
    {
        id: 'docs-show-refuses-an-unknown-option',
        why: 'an unknown flag dropped from the positionals would run the command with the option silently ignored',
        run: async (binDir) => refuses(binDir, ['docs', 'show', 'docs/inflight/note.md', '--bogus-flag'],
            'docs show: unknown option(s): --bogus-flag - known: --ref <ref>, --header-only', docsShowFixture()),
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'), 'if (unknown.length) return', 'if (false) return'),
    },
    {
        id: 'docs-show-refuses-a-ref-flag-with-nothing-after-it',
        why: 'a trailing --ref with no value would fall through to the default ref and show a copy the caller did not ask for',
        run: async (binDir) => refuses(binDir, ['docs', 'show', 'docs/inflight/note.md', '--ref'], 'docs show: --ref needs a ref after it', docsShowFixture()),
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'),
            "if (refAt >= 0 && (requested === undefined || requested.startsWith('--'))) return", 'if (false) return'),
    },
    {
        id: 'docs-show-refuses-to-run-without-a-path',
        why: 'the -1 sentinel arithmetic on refValueAt is the known trap here; with no path at all the refusal must name where to find one',
        run: async (binDir) => refuses(binDir, ['docs', 'show'], 'docs show: give a document path (see: note find, prior-art)', docsShowFixture()),
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'), 'if (!path) return', 'if (false) return'),
    },
    {
        id: 'docs-show-says-which-refs-carry-a-path-the-requested-ref-does-not',
        why: 'a ref that does not carry the path is a refusal that has to name the refs which do, else the caller is left guessing at a ref set several hundred long',
        run: async (binDir) => {
            const dir = docsShowFixture()
            const sha = windowGit(dir)('rev-parse', 'master')
            const r = invoke(binDir, ['docs', 'show', 'docs/inflight/branch-only.md', '--ref', sha], { cwd: dir })
            return r.code === 2 && r.out.includes(`docs show: ${sha} does not carry docs/inflight/branch-only.md - `)
                && r.out.includes('refs carry it (1 live, 0 archival), e.g. only-here')
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'), '        if (blob === null) {\n', '        if (false) {\n'),
    },
    {
        id: 'docs-list-refuses-more-than-an-area-and-a-group',
        why: 'a third positional silently ignored would list a level the caller did not name',
        run: async (binDir) => refuses(binDir, ['docs', 'list', 'a', 'b', 'c'], "docs list: takes an area and at most one group, not 'a b c'", docsShowFixture()),
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'), 'if (extra.length > 0) return', 'if (false) return'),
    },
    {
        id: 'docs-for-branch-refuses-a-second-ref',
        why: 'two refs would answer for the first and drop the second - the repeated-argument shape, on the command the session hook runs',
        run: async (binDir) => {
            const env = { ...process.env, PC_INFLIGHT_CACHE_DIR: mkdtempSync(join(tmpdir(), 'inflight-for-branch-cache-')) }
            const r = invoke(binDir, ['docs', 'for-branch', 'ref1', 'ref2'], { cwd: docsShowFixture(), env })
            return r.code === 2 && r.out.includes("docs for-branch: takes one ref at most, not 'ref1 ref2'")
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'),
            "if (args.length > 1 || args.some((a) => a.startsWith('--'))) {", "if (args.some((a) => a.startsWith('--'))) {"),
    },
    {
        id: 'docs-show-outside-the-corpus-names-the-areas-it-covers',
        why: 'a README shown with a divergence header would be a claim the query never measured; saying which areas it covers is the answer, and it is not an error',
        run: async (binDir) => {
            const dir = docsShowFixture()
            const r = invoke(binDir, ['docs', 'show', 'README.md'], { cwd: dir })
            if (r.code !== 0) return false
            return ['docs/inflight', 'docs/solutions', 'docs/plans'].every((area) => r.out.includes(area))
                && !r.out.includes('=== divergence:')
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'),
            "if (!inCorpus(path)) {\n        emit(", "if (!inCorpus(path)) {\n        return { ok: false, reason: 'mutant' }\n        emit("),
    },
    {
        id: 'help-lists-docs-show-and-docs-header',
        why: 'the read-time hook tells an agent to run docs header; a command help cannot find is one the agent will not trust',
        run: async (binDir) => {
            const help = invoke(binDir, ['help'])
            if (help.code !== 0 || !help.out.includes('docs show') || !help.out.includes('docs header')) return false
            return [['docs', 'show'], ['docs', 'header']].every((sub) => {
                const usage = invoke(binDir, ['help', ...sub])
                return usage.code === 0 && usage.out.includes(`Usage: bin/inflight.mjs ${sub.join(' ')}`)
            })
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'),
            "        name: 'header',", "        name: 'hdr',"),
    },
    // ---------------------------------------------------------------------------------------------
    // BARE `docs` AND `docs list` - the corpus shape, and the walk from it to one document.
    // docs/plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md, unit U5.
    // ---------------------------------------------------------------------------------------------
    {
        id: 'tag-vocabulary-round-trips-through-the-shell-library',
        why: 'the gate reads the vocabulary by sourcing the shell wrapper, which evals what the Node file prints; a printer that renders a set wrongly is a gate comparing against the wrong values, and nothing else would report it',
        run: async (binDir) => {
            const t = await tagsLib(binDir)
            const names = Object.keys(t.SHELL_VARIABLES)
            // SOURCED UNDER BASH, NOT PARSED: the wrapper runs node and evals, so this exercises
            // the path the gate takes - node resolution, the render, the eval - not the values alone.
            const script = `source "$1" || exit 9; for v in ${names.join(' ')}; do eval "printf '%s\\n' \\"\\$\${v}\\""; done`
            const r = spawnSync('bash', ['-c', script, '_', join(binDir, 'lib', 'inflight-tags.sh')], { encoding: 'utf8' })
            if (r.status !== 0) return false
            const got = r.stdout.split('\n').slice(0, names.length).map((l) => l.trim().split(/\s+/).filter(Boolean))
            if (got.length !== names.length || got.some((v) => v.length === 0)) return false
            // And the wrapper must fail LOUDLY, not define nothing, when the render fails: an empty
            // set would let the gate call every note invalid, or - with set -u off - valid.
            // bash by absolute path: spawnSync resolves the executable against the env it is
            // GIVEN, so an emptied PATH would fail to find bash and prove nothing about node.
            const bashPath = spawnSync('sh', ['-c', 'command -v bash'], { encoding: 'utf8' }).stdout.trim()
            const broken = spawnSync(bashPath, ['-c', 'source "$1"; echo "rc=$?"', '_', join(binDir, 'lib', 'inflight-tags.sh')],
                { encoding: 'utf8', env: { ...process.env, PATH: '/nonexistent' } })
            if (!/rc=[1-9]/.test(broken.stdout) || !broken.stderr.includes('node')) return false
            return names.every((n, i) => JSON.stringify(got[i]) === JSON.stringify(t.SHELL_VARIABLES[n]))
        },
        // The printer joins one set on the wrong separator: bash then reads the whole set as one
        // word, and the gate would reject every real value.
        mutate: (binDir) => patch(join(binDir, 'lib', 'inflight-tags.mjs'),
            "        return `${name}=\"${values.join(' ')}\"`",
            "        return `${name}=\"${values.join(name === 'INFLIGHT_TYPES' ? ',' : ' ')}\"`"),
    },
    {
        id: 'bare-docs-prints-the-shape-the-guide-and-any-recorded-delivery-failure',
        why: 'a hook that fails open prints nothing to the agent it failed; the bare call is the one place that failure is visible, beside the map of what the corpus holds',
        run: async (binDir) => {
            const dir = docsFixture()
            const plain = invoke(binDir, ['docs'], { cwd: dir })
            if (plain.code !== 0 || !plain.out.trim()) return false
            // Each area with its count, the off-baseline count, and the guide from the registry.
            if (!/docs\/inflight\/ {2}2 documents, 1 only off the baseline/.test(plain.out)) return false
            if (!/docs\/solutions\/ {2}1 document\b/.test(plain.out) || !/docs\/plans\/ {2}1 document\b/.test(plain.out)) return false
            if ((plain.out.match(/^ +when: /gm) ?? []).length < 3) return false
            if (!['docs list', 'docs show', 'docs header'].every((c) => plain.out.includes(c))) return false
            if (plain.out.includes('DELIVERY FAILED')) return false
            // A failure recorded through the cache library, in a cache directory of this check's own.
            const cache = await cacheLib(binDir)
            const cacheDir = mkdtempSync(join(tmpdir(), 'inflight-docs-cache-'))
            const envBefore = process.env.PC_INFLIGHT_CACHE_DIR
            process.env.PC_INFLIGHT_CACHE_DIR = cacheDir
            try { cache.recordDeliveryFailure('header', 'the hook threw on a fixture') } finally {
                if (envBefore === undefined) delete process.env.PC_INFLIGHT_CACHE_DIR
                else process.env.PC_INFLIGHT_CACHE_DIR = envBefore
            }
            const noticed = invoke(binDir, ['docs'], { cwd: dir, env: { ...process.env, PC_INFLIGHT_CACHE_DIR: cacheDir } })
            return noticed.code === 0 && /DELIVERY FAILED: header - the hook threw on a fixture/.test(noticed.out)
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-views.mjs'),
            'for (const [delivery, f] of Object.entries(failures)) {', 'for (const [delivery, f] of []) {'),
    },
    {
        id: 'docs-list-walks-from-the-bare-call-to-a-document-by-copied-commands',
        why: 'every level must print the next level\'s commands, or the walk needs input the agent has to invent - the plan\'s R14, and its acceptance example AE5',
        run: async (binDir) => {
            const dir = docsFixture()
            const t = await tagsLib(binDir)
            const cmds = (out) => out.split('\n').map((l) => /bin\/inflight\.mjs (.+)$/.exec(l)).filter(Boolean).map((m) => m[1].trim().split(/\s+/))
            const first = (out, prefix, length) => cmds(out).find((c) => c.length === length && prefix.every((p, i) => c[i] === p))
            const bare = invoke(binDir, ['docs'], { cwd: dir })
            const toArea = first(bare.out, ['docs', 'list', 'inflight'], 3)
            if (bare.code !== 0 || !toArea) return false
            const area = invoke(binDir, toArea, { cwd: dir })
            const toGroup = first(area.out, ['docs', 'list', 'inflight'], 4)
            if (area.code !== 0 || !toGroup) return false
            const group = invoke(binDir, toGroup, { cwd: dir })
            if (group.code !== 0) return false
            // Titles with paths, and the off-baseline note marked as such with its ref.
            if (!group.out.includes('The note  docs/inflight/note.md')) return false
            if (!/Only here {2}docs\/inflight\/branch-only\.md {2}\(off baseline - on only-here\)/.test(group.out)) return false
            const toDoc = first(group.out, ['docs', 'show'], 3)
            if (!toDoc) return false
            const doc = invoke(binDir, toDoc, { cwd: dir })
            if (doc.code !== 0 || !doc.out.includes(`--- ${toDoc[2]} @ `)) return false
            // The group commands come out in the index's order: registers, the impact order the
            // shell library states, then the four trailing groups.
            const listed = invoke(binDir, ['docs', 'list', 'inflight', 'no-such-group'], { cwd: dir })
            const order = cmds(listed.out).filter((c) => c.length === 4 && c[2] === 'inflight').map((c) => c[3])
            const want = ['registers', ...t.INFLIGHT_IMPACT_ORDER, 'feature', 'unmatched', 'closed', 'deferred']
            return listed.code === 0 && JSON.stringify(order) === JSON.stringify(want)
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-views.mjs'),
            'out.push(`        ${TOOL} docs show ${d.path}`)', 'out.push(`        ${TOOL} docs show`)'),
    },
    {
        id: 'a-deferred-note-groups-as-deferred-and-a-note-that-mentions-the-marker-stays-open',
        why: 'the session index once filed `parked - deferred` under nothing and read a note QUOTING the marker as closed; the shape inherits both rules or repeats both incidents',
        run: async (binDir) => {
            const t = await tagsLib(binDir)
            const s = await docsShapeLib(binDir)
            const g = (text) => s.inflightGroupOf(t.classifyNote(text, 'docs/inflight/x.md'))
            const tagged = (state) => `# X\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n${state}\nbody\n`
            if (g(tagged('<!-- inflight-state: parked - deferred until the next major -->')) !== 'deferred') return false
            if (g(tagged('<!-- inflight-state: deferred - after v6 -->')) !== 'deferred') return false
            if (g(tagged('<!-- inflight-state: closed - landed in astubbs#1 -->')) !== 'closed') return false
            if (g(tagged('the marker is `inflight-state:` and this note quotes it in prose')) !== 'ci') return false
            if (g(tagged('')) !== 'ci') return false
            // And through the tool: its own repository, so the shared fixture's exact counts stand.
            const { dir, commit } = windowRepo()
            mkdirSync(join(dir, 'docs', 'inflight'), { recursive: true })
            const note = (name, state) => writeFileSync(join(dir, 'docs', 'inflight', `${name}.md`), `# ${name}\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n${state}\n`)
            note('parked-one', '<!-- inflight-state: parked - deferred until v6 -->')
            note('quotes-the-marker', 'prose that says inflight-state: and moves on')
            note('open-one', '')
            commit('three notes')
            const deferred = invoke(binDir, ['docs', 'list', 'inflight', 'deferred'], { cwd: dir })
            const ci = invoke(binDir, ['docs', 'list', 'inflight', 'ci'], { cwd: dir })
            if (deferred.code !== 0 || ci.code !== 0) return false
            if (!deferred.out.includes('parked-one') || deferred.out.includes('open-one') || deferred.out.includes('quotes-the-marker')) return false
            return ci.out.includes('quotes-the-marker') && ci.out.includes('open-one') && !ci.out.includes('parked-one')
        },
        // Requires the word at the FRONT of the state - the exact rule the index once had, under
        // which `parked - deferred` fell out of every section.
        mutate: (binDir) => patch(join(binDir, 'lib', 'inflight-tags.mjs'),
            'export const DEFERRED_RE = /inflight-state:[^>]*(deferred|parked)[^>]*-->/',
            'export const DEFERRED_RE = /inflight-state:\\s*deferred[^>]*-->/'),
    },
    {
        id: 'an-empty-area-prints-its-heading-with-a-zero-count-and-the-refs-searched',
        why: 'an area the shape drops because it is empty reads as an area that does not exist, and zero across N refs is a result where a missing heading is not',
        run: async (binDir) => {
            const { dir, commit } = windowRepo()
            mkdirSync(join(dir, 'docs', 'inflight'), { recursive: true })
            writeFileSync(join(dir, 'docs', 'inflight', 'only.md'), '# Only\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\nbody\n')
            commit('one note, no plans, no solutions')
            const bare = invoke(binDir, ['docs'], { cwd: dir })
            if (bare.code !== 0) return false
            if (!/docs\/solutions\/ {2}0 documents/.test(bare.out) || !/docs\/plans\/ {2}0 documents/.test(bare.out)) return false
            if (!/searched \d+ refs? \(\d+ live, \d+ archival\)/.test(bare.out)) return false
            const area = invoke(binDir, ['docs', 'list', 'plans'], { cwd: dir })
            return area.code === 0 && /docs\/plans\/ {2}0 documents/.test(area.out) && /searched \d+ ref/.test(area.out)
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-shape.mjs'),
            'const shaped = areas.map((a) => {', 'const shaped = areas.filter((a) => perArea.get(a.dir).length > 0).map((a) => {'),
    },
    {
        id: 'docs-list-with-an-unknown-name-answers-with-the-valid-names-and-exit-0',
        why: 'a typo is not a failure to run; the valid names, each as the command that would have worked, are the answer',
        run: async (binDir) => {
            const dir = docsFixture()
            const area = invoke(binDir, ['docs', 'list', 'nowhere'], { cwd: dir })
            if (area.code !== 0 || !area.out.includes("no area named 'nowhere'")) return false
            if (!['inflight', 'solutions', 'plans'].every((a) => area.out.includes(`bin/inflight.mjs docs list ${a}`))) return false
            const group = invoke(binDir, ['docs', 'list', 'inflight', 'nowhere'], { cwd: dir })
            if (group.code !== 0 || !group.out.includes("no group named 'nowhere' in inflight")) return false
            if (!group.out.includes('bin/inflight.mjs docs list inflight crash')) return false
            const none = invoke(binDir, ['docs', 'list'], { cwd: dir })
            return none.code === 0 && none.out.includes('bin/inflight.mjs docs list inflight')
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-views.mjs'),
            "const out = [area === null ? 'give an area to list:' : `no area named '${area}' - the areas are:`]",
            "return area === null ? 'give an area to list' : `no area named '${area}'`\n        const out = []"),
    },
    {
        id: 'docs-shape-reads-every-document-in-one-cat-file-batch',
        why: 'the three-area index alone is most of the 8 s budget; one fork per document for its title and markers would spend the rest several times over',
        run: async (binDir) => {
            const n = await notes(binDir)
            const s = await docsShapeLib(binDir)
            const perf = await perfOf(binDir)
            return inDir(docsFixture(), () => {
                const index = n.corpusIndex()
                const clusters = n.stranded(index)
                perf.perfReset()
                const shape = s.docsShape({ index, stranded: clusters })
                if (!shape.ok || callCount(perf.perfReport(), 'git cat-file') !== 1) return false
                const titles = shape.areas.flatMap((a) => a.groups.flatMap((g) => g.docs.map((d) => d.title)))
                if (!['The note', 'Only here', 'A solved problem', 'A plan'].every((t) => titles.includes(t))) return false
                const inflight = shape.areas.find((a) => a.key === 'inflight')
                return inflight.groups.find((g) => g.key === 'ci').docs.every((d) => d.note.type === 'task')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-shape.mjs'),
            'const batch = blobContents(wanted.map((w) => w.blob))',
            'const batch = { ok: true, contents: new Map(wanted.map((w) => [w.blob, blobContents([w.blob]).contents.get(w.blob)])) }'),
    },
    {
        id: 'document-titles-follow-the-index-fallback-chain',
        why: 'a solution\'s title is YAML in its frontmatter and quoted whenever it holds a colon; a shape that lists the heading or the filename instead names the document differently from the index the agent already read',
        run: async (binDir) => {
            const t = await tagsLib(binDir)
            if (t.titleOf('---\ntitle: "Race: the drainer ran twice"\ntags: [x]\n---\n# Some heading\n', 'docs/solutions/ci/a.md') !== 'Race: the drainer ran twice') return false
            if (t.titleOf("---\ntitle: 'it''s quoted'\n---\n", 'docs/solutions/ci/a.md') !== "it''s quoted") return false
            if (t.titleOf('intro\n# The heading\n\ntitle: not frontmatter\n', 'docs/plans/2026-01-01-001-p.md') !== 'The heading') return false
            // A note is named by its heading even when a handoff frontmatter carries another title.
            if (t.titleOf('---\ntitle: "The frontmatter sentence"\n---\n# The heading sentence\n', 'docs/inflight/handoff-x.md') !== 'The heading sentence') return false
            return t.titleOf('no heading at all\n', 'docs/plans/2026-01-01-001-p.md') === '2026-01-01-001-p'
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'inflight-tags.mjs'),
            ' ? null : /^---\\r?\\n([\\s\\S]*?)\\r?\\n---/.exec(text)', ' ? null : null'),
    },
    {
        id: 'help-lists-docs-list-with-its-when-line',
        why: 'the bare call points at docs list; a command help cannot find is one the agent will not trust',
        run: async (binDir) => {
            const help = invoke(binDir, ['help'])
            if (help.code !== 0) return false
            const lines = help.out.split('\n')
            const at = lines.findIndex((l) => /^ {2}docs list\b/.test(l))
            if (at < 0 || !/^\s+when: \S/.test(lines[at + 1] ?? '')) return false
            const usage = invoke(binDir, ['help', 'docs', 'list'])
            return usage.code === 0 && usage.out.includes('Usage: bin/inflight.mjs docs list')
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'), "        name: 'list',", "        name: 'ls',"),
    },
    // ---------------------------------------------------------------------------------------------
    // THE SESSION INDEX - `docs index`, what .claude/hooks/inject-recorded-knowledge.sh injects.
    // docs/plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md, unit U6. The hook's own
    // cases, and the equivalence check against the pre-migration hook, are in
    // bin/test-check-agent-hooks.sh; these cover the command.
    // ---------------------------------------------------------------------------------------------
    {
        id: 'docs-index-lists-an-off-baseline-workstream-as-one-heading-naming-its-branch-set',
        why: 'a workstream\'s notes on one branch are one fact, not one line each - the plan\'s R18 and its acceptance example AE6; the heading has to name the branch or the reader cannot go there',
        run: async (binDir) => {
            const r = invoke(binDir, ['docs', 'index'], { cwd: docsIndexFixture() })
            if (r.code !== 0 || !r.out.trim()) return false
            // ONE heading for the workstream in the in-flight area - once per area it touches, so
            // three in all for a branch carrying a note, a solution and a plan - with every one
            // of its notes directly under it.
            const inflight = r.out.split('# In flight only on branches')[1]?.split('\n# ')[0] ?? ''
            if (inflight.split('\n').filter((l) => l === '## only on feats/workstream').length !== 1) return false
            if (r.out.split('\n').filter((l) => l === '## only on feats/workstream').length !== 3) return false
            const section = inflight.split('## only on feats/workstream\n')[1].split('\n## ')[0]
            if (!['Workstream note A', 'Workstream note B', 'Workstream note C'].every((t) => section.includes(`- [task] ${t}`))) return false
            // The on-baseline listing keeps the hook's headings, so a grep an agent learned still works.
            return ['# Open work - what it costs you to not know', '## ci', '# Dated plans and investigations', '## 2026-01']
                .every((h) => r.out.includes(`${h}\n`)) && r.out.startsWith('docs context: session index\n')
        },
        // Only the first path of each cluster is grouped; the rest silently vanish from the index.
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-views.mjs'),
            'for (const p of c.paths) setOf.set(p, key)', 'for (const p of c.paths.slice(0, 1)) setOf.set(p, key)'),
    },
    {
        id: 'docs-index-max-lines-collapses-the-tail-to-a-count-equal-to-the-groups-omitted',
        why: 'a cap that silently drops groups is the partial index the hook was rewritten to end; the count is what makes the omission visible, so a wrong count is a lie about the corpus',
        run: async (binDir) => {
            const dir = docsIndexFixture()
            // Four in-flight branch sets exist off the baseline (workstream, only-here, second, third).
            // Twelve lines is four per area: the workstream group (heading, three notes, blank) fits
            // with the solutions area's spare line and nothing after it does.
            const capped = invoke(binDir, ['docs', 'index', '--max-lines', '12'], { cwd: dir })
            if (capped.code !== 0) return false
            const inflight = capped.out.split('# In flight only on branches')[1]?.split('\n# ')[0] ?? ''
            const shown = inflight.split('\n').filter((l) => l.startsWith('## only on ')).length
            if (shown !== 1 || !inflight.includes('... 3 more branch sets holding 3 documents, past the 12-line cap')) return false
            if (!inflight.includes('bin/inflight.mjs docs list inflight')) return false
            // Uncapped, every set is shown and no count line appears anywhere.
            const full = invoke(binDir, ['docs', 'index', '--max-lines', '1000'], { cwd: dir })
            const all = full.out.split('# In flight only on branches')[1]?.split('\n# ')[0] ?? ''
            if (full.code !== 0 || all.split('\n').filter((l) => l.startsWith('## only on ')).length !== 4 || /more branch set/.test(full.out)) return false
            const bad = invoke(binDir, ['docs', 'index', '--max-lines', 'lots'], { cwd: dir })
            return bad.code === 2
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-views.mjs'), '                omitted++\n', '                omitted += 2\n'),
    },
    {
        id: 'docs-index-reads-on-baseline-titles-from-the-baseline-blob-not-the-working-tree',
        why: 'the plan\'s KTD16: an index built on a checkout behind the baseline must not be wrong, so the copy the checkout holds is never what is listed - the equivalence check in the hook suite classifies the one title that differs for exactly this reason',
        run: async (binDir) => {
            const dir = docsFixture()
            const note = join(dir, 'docs', 'inflight', 'note.md')
            const committed = readFileSync(note, 'utf8')
            writeFileSync(note, committed.replace('# The note', '# The note, retitled in the working tree'))
            try {
                const r = invoke(binDir, ['docs', 'index'], { cwd: dir })
                return r.code === 0 && r.out.includes('- [task] The note\n') && !r.out.includes('retitled in the working tree')
            } finally {
                writeFileSync(note, committed)
            }
        },
        // The shape reads the working-tree file whenever one exists, which is the old hook's scan.
        mutate: (binDir) => {
            const f = join(binDir, 'lib', 'docs-shape.mjs')
            patch(f, "import { blobContents } from './git.mjs'", "import { existsSync, readFileSync } from 'node:fs'\nimport { blobContents } from './git.mjs'")
            patch(f, "const text = batch.contents.get(w.blob) ?? ''",
                "const text = existsSync(w.path) ? readFileSync(w.path, 'utf8') : (batch.contents.get(w.blob) ?? '')")
        },
    },
    {
        id: 'the-front-door-drains-a-large-page-before-exiting',
        why: 'process.exit() drops stdout still queued on a pipe, so a page over 64 KiB read through $(...) arrived cut at exactly 65536 bytes with exit 0 - the session hook lost its plans section this way; only setting exitCode lets the loop drain',
        run: async (binDir) => {
            // The source half holds on every platform; the behavioural half reproduces the cut
            // where pipes are asynchronous (macOS), and merely passes where they are not (Linux).
            const src = code(join(binDir, 'inflight.mjs'))
            if (src.includes('process.exit(') || !src.includes('process.exitCode')) return false
            const dir = bigIndexFixture()
            const tool = join(binDir, 'inflight.mjs')
            const file = join(dir, 'page.txt')
            const direct = spawnSync('bash', ['-c', 'node "$1" docs index > "$2"', '_', tool, file], { cwd: dir, encoding: 'utf8' })
            if (direct.status !== 0) return false
            const bytes = readFileSync(file).length
            if (bytes <= 65536) return false // the fixture is not big enough to show anything
            const captured = spawnSync('bash', ['-c', 'x=$(node "$1" docs index 2>/dev/null); printf "%s\\n" "$x"', '_', tool], { cwd: dir, encoding: 'utf8' })
            return captured.status === 0 && Buffer.byteLength(captured.stdout) === bytes
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'), '    process.exitCode = ok ? 0 : 2\n', '    process.exit(ok ? 0 : 2)\n'),
    },
    {
        id: 'docs-index-records-its-own-failure-and-clears-it-on-the-next-success',
        why: 'the hook that calls this fails open, so without the record (the plan\'s KTD13) a session whose index never rendered looks like one with nothing to list; a record that outlives the fix is the same lie the other way',
        run: async (binDir) => {
            const cache = await cacheLib(binDir)
            const cacheDir = mkdtempSync(join(tmpdir(), 'inflight-index-cache-'))
            const env = { ...process.env, PC_INFLIGHT_CACHE_DIR: cacheDir }
            const withCache = (fn) => {
                const before = process.env.PC_INFLIGHT_CACHE_DIR
                process.env.PC_INFLIGHT_CACHE_DIR = cacheDir
                try { return fn() } finally {
                    if (before === undefined) delete process.env.PC_INFLIGHT_CACHE_DIR
                    else process.env.PC_INFLIGHT_CACHE_DIR = before
                }
            }
            const outside = mkdtempSync(join(tmpdir(), 'inflight-index-notarepo-'))
            const failed = invoke(binDir, ['docs', 'index'], { cwd: outside, env })
            if (failed.code !== 2 || !withCache(() => 'session index' in cache.deliveryFailures())) return false
            // Another delivery's record is printed; the index's own is cleared by this success.
            withCache(() => cache.recordDeliveryFailure('read-time header', 'stub reason'))
            const ok = invoke(binDir, ['docs', 'index'], { cwd: docsFixture(), env })
            if (ok.code !== 0 || !ok.out.includes('DELIVERY FAILED: read-time header - stub reason')) return false
            if (ok.out.includes('DELIVERY FAILED: session index')) return false
            return withCache(() => !('session index' in cache.deliveryFailures()))
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'), '    clearDeliveryFailure(INDEX_DELIVERY)\n', ''),
    },
    {
        id: 'help-lists-docs-index-with-its-when-line',
        why: 'the hook names this command as the way to get the list on a host without hooks; a command help cannot find is one the agent will not trust',
        run: async (binDir) => {
            const help = invoke(binDir, ['help'])
            if (help.code !== 0) return false
            const lines = help.out.split('\n')
            const at = lines.findIndex((l) => /^ {2}docs index\b/.test(l))
            if (at < 0 || !/^\s+when: \S/.test(lines[at + 1] ?? '')) return false
            const usage = invoke(binDir, ['help', 'docs', 'index'])
            return usage.code === 0 && usage.out.includes('Usage: bin/inflight.mjs docs index') && usage.out.includes('--max-lines')
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'), "        name: 'index',", "        name: 'idx',"),
    },
    // ---------------------------------------------------------------------------------------------
    // THE PROMPT HALF OF THE QUERY - term extraction in isolation, then one grep over the live refs.
    // docs/plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md, unit U4.
    // ---------------------------------------------------------------------------------------------
    {
        id: 'terms-from-prompt-keeps-identifier-shapes-and-drops-prose',
        why: 'a term that is prose matches the whole corpus and a term that is a class name matches its documents; the extractor is the whole difference between a signal and a wall',
        run: async (binDir) => {
            const t = await termsLib(binDir)
            // `Broker` is the single-hump control the stop list does NOT also cover: `Kafka` is
            // dropped twice over, so on its own it cannot tell the hump rule from the list. The bare
            // issue number is the SUBJECT here - the extractor collapses every spelling to it.
            // issue-refs: exempt-begin
            const got = t.termsFromPrompt('Fix the ProducerManager commit_lock in bin/inflight.mjs, see astubbs#419 and `commit lock`; Kafka Broker Fix abc, then #419 again and ProducerManager again')
            const kept = ['ProducerManager', 'commit_lock', 'bin/inflight.mjs', '#419', 'lock']
            const dropped = ['the', 'Fix', 'fix', 'Kafka', 'Broker', 'abc', 'commit', 'astubbs#419', 'see']
            if (!kept.every((k) => got.includes(k))) return false
            if (dropped.some((d) => got.includes(d))) return false
            // Deduplicated: the second ProducerManager and the second #419 add nothing.
            if (got.filter((x) => x === 'ProducerManager').length !== 1 || got.filter((x) => x === '#419').length !== 1) return false
            // issue-refs: exempt-end
            // Capped in order of first appearance.
            const many = Array.from({ length: t.MAX_TERMS + 5 }, (_, i) => `ClassName${i}Alpha`).join(' ')
            const capped = t.termsFromPrompt(many)
            if (capped.length !== t.MAX_TERMS || capped[0] !== 'ClassName0Alpha') return false
            return t.termsFromPrompt('please fix the tests').length === 0 && t.termsFromPrompt('').length === 0
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'terms.mjs'),
            '(t.match(/[A-Z]/g) ?? []).length >= 2', '(t.match(/[A-Z]/g) ?? []).length >= 1'),
    },
    {
        id: 'match-docs-frontmatter-field-on-a-branch-only-solution-ranks-first-and-is-off-baseline',
        why: 'a related_components field is a claim the author made on purpose; a branch-only solution naming the class is exactly the prior art the working tree cannot show',
        run: async (binDir) => {
            const t = await termsLib(binDir)
            return inDir(termsFixture(), () => {
                const m = t.matchDocs(['RetryQueueDrainer'])
                if (!m.ok || m.hits.length !== 1 || m.refsSearched < 1) return false
                const h = m.hits[0]
                return h.path === 'docs/solutions/ci/retry-queue.md' && h.tier === 'frontmatter'
                    && h.onBaseline === false && h.title === 'The retry queue drained twice'
                    && h.terms.length === 1 && h.terms[0] === 'RetryQueueDrainer' && h.refs.includes('terms-only')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'terms.mjs'),
            '|| /^\\s+-\\s/.test(text)))', '|| false))'),
    },
    {
        id: 'match-docs-heading-hit-ranks-as-heading-and-reads-the-title-from-the-blob',
        why: 'a term in a `##` heading is not the title; the title has to be read, once, for the hits that are shown',
        run: async (binDir) => {
            const t = await termsLib(binDir)
            return inDir(termsFixture(), () => {
                const m = t.matchDocs(['WidgetSpinner'])
                if (!m.ok || m.hits.length !== 1) return false
                const h = m.hits[0]
                return h.tier === 'heading' && h.onBaseline === true && h.divergent === false && h.title === 'A rollout plan'
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'terms.mjs'),
            "if (/^#{1,6}\\s/.test(text)) return 'heading'", "if (/^#{1,6}\\s/.test(text)) return 'body'"),
    },
    {
        id: 'match-docs-body-hits-are-capped-per-term-and-the-rest-counted',
        why: 'a mechanism named in prose across forty notes is forty lines nobody reads; the cap keeps the block short and the count keeps the truncation honest',
        run: async (binDir) => {
            const t = await termsLib(binDir)
            return inDir(termsFixture(), () => {
                const m = t.matchDocs(['flux_capacitor'])
                if (!m.ok || m.hits.length !== t.BODY_CAP_PER_TERM || m.truncated !== 5 - t.BODY_CAP_PER_TERM) return false
                if (!m.hits.every((h) => h.tier === 'body')) return false
                const wide = t.matchDocs(['flux_capacitor'], { bodyCap: 10 })
                return wide.ok && wide.hits.length === 5 && wide.truncated === 0
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'terms.mjs'),
            'budget.set(t, budget.get(t) - 1)', 'budget.set(t, budget.get(t) - 0)'),
    },
    {
        id: 'match-docs-is-one-git-grep-and-no-ls-tree',
        why: 'the per-prompt budget is 2500ms; a corpus-index build is five seconds on this repository, and one grep per term is one budget per term',
        run: async (binDir) => {
            const t = await termsLib(binDir)
            const perf = await perfOf(binDir)
            return inDir(termsFixture(), () => {
                perf.perfReset()
                const m = t.matchDocs(['RetryQueueDrainer', 'WidgetSpinner', 'flux_capacitor'])
                const report = perf.perfReport()
                return m.ok && callCount(report, 'git grep') === 1 && callCount(report, 'git ls-tree') === 0
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'terms.mjs'),
            "    const res = exec('git', ['grep', '-n', '-i', '-F', ...patterns,",
            "    const ignoredMutant = exec('git', ['ls-tree', 'HEAD'])\n    const res = exec('git', ['grep', '-n', '-i', '-F', ...patterns,"),
    },
    {
        id: 'match-docs-outside-a-repository-cannot-answer-rather-than-finds-nothing',
        why: 'ok:false is the only thing that separates "git could not run" from "no document names this", and the hook records the first and stays silent on the second',
        run: async (binDir) => {
            const t = await termsLib(binDir)
            const outside = mkdtempSync(join(tmpdir(), 'inflight-terms-notarepo-'))
            const m = await inDir(outside, () => t.matchDocs(['RetryQueueDrainer']))
            return m.ok === false && typeof m.reason === 'string' && m.reason.length > 0 && m.hits.length === 0
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'terms.mjs'),
            "if (!tips.ok) return cannot('cannot list refs - is this a git repository?')",
            'if (!tips.ok) return { ...result, ok: true }'),
    },
    {
        id: 'match-docs-an-issue-number-is-not-found-inside-a-longer-one',
        why: 'the fixed-string grep finds `#41` inside `#411`, `#416` and `#419`; on this repository that made `#41` an eighteen-hit term whose first two hits were about `#411`, and a document whose only match is the longer number must not be a hit at all',
        run: async (binDir) => {
            const t = await termsLib(binDir)
            return inDir(termsFixture(), () => {
                // issue-refs: exempt-begin
                const short = t.matchDocs(['#41'])
                if (!short.ok || short.hits.length !== 1 || short.hits[0].path !== 'docs/inflight/issue-41.md') return false
                if (short.hits[0].terms.length !== 1 || short.hits[0].terms[0] !== '#41') return false
                // The longer number is still its own term, unharmed by the boundary.
                const long = t.matchDocs(['#419'])
                return long.ok && long.hits.length === 1 && long.hits[0].path === 'docs/inflight/issue-419.md'
                // issue-refs: exempt-end
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'terms.mjs'), '(?![0-9])', ''),
    },
    // ---------------------------------------------------------------------------------------------
    // THE BRANCH HALF OF THE QUERY - terms from the branch's own facts, the block the session hook
    // injects. docs/plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md, unit U7.
    // ---------------------------------------------------------------------------------------------
    {
        id: 'terms-from-branch-splits-the-issue-number-off-the-slug-and-drops-the-type-prefix',
        why: 'a document names a branch by its slug and its issue by its number; the type prefix names a kind of work and would match every note there is',
        run: async (binDir) => {
            const t = await termsLib(binDir)
            // issue-refs: exempt-begin
            const got = t.termsFromBranch('fix/857-commit-lock', { prs: new Map() })
            if (got.length !== 2 || got[0] !== '#857' || got[1] !== 'commit-lock') return false
            // A hyphenated prefix survives the shape rules, so only the prefix set can drop it.
            const picked = t.termsFromBranch('cherry-pick/893-drain-twice', { prs: new Map() })
            if (picked.includes('cherry-pick') || !picked.includes('#893') || !picked.includes('drain-twice')) return false
            // A remote-tracking spelling names the same branch.
            const remote = t.termsFromBranch('origin/fix/857-commit-lock', { prs: new Map() })
            if (remote.length !== 2 || remote[0] !== '#857') return false
            // issue-refs: exempt-end
            return t.termsFromBranch('master', { prs: new Map() }).length === 0 && t.termsFromBranch('', { prs: new Map() }).length === 0
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'terms.mjs'),
            'const m = seg.match(/^(\\d+)(?:[-_](.+))?$/)', 'const m = seg.match(/^(\\d+)$/)'),
    },
    {
        id: 'terms-from-branch-adds-the-cached-pr-number-and-title-identifiers-and-nothing-on-a-miss',
        why: 'the PR title is where the mechanism is named in full; a cache miss must fall back to the branch name rather than reach for gh at session start',
        run: async (binDir) => {
            const t = await termsLib(binDir)
            // issue-refs: exempt-begin
            const prs = new Map([['fix/857-commit-lock', { number: 41, title: 'fix(core) astubbs#857: give ProducerManager a lock', state: 'OPEN' }]])
            const got = t.termsFromBranch('fix/857-commit-lock', { prs })
            const kept = ['#857', 'commit-lock', 'ProducerManager', '#41']
            const dropped = ['fix', 'core', 'give', 'lock', 'astubbs#857']
            if (!kept.every((k) => got.includes(k)) || dropped.some((d) => got.includes(d))) return false
            // The branch's own segments come first: they are the terms a cap must never drop.
            if (got[0] !== '#857' || got[1] !== 'commit-lock') return false
            // The PR's number is not the issue's, and both are terms.
            if (got.filter((x) => x === '#857').length !== 1) return false
            // A backticked span in a title is not a shape claim here: `inflight docs` in a PR
            // title must not yield the word `inflight`, which names the whole directory.
            const ticked = new Map([['feats/x-y', { number: 9, title: 'feat: `inflight docs` and `commit lock`', state: 'OPEN' }]])
            const fromTicks = t.termsFromBranch('feats/x-y', { prs: ticked })
            if (fromTicks.includes('inflight') || fromTicks.includes('lock')) return false
            // A miss: the branch name alone, and `prs` absent is the same as a miss.
            const miss = t.termsFromBranch('fix/857-commit-lock', { prs: new Map() })
            const none = t.termsFromBranch('fix/857-commit-lock')
            // issue-refs: exempt-end
            return miss.length === 2 && none.length === 2 && !miss.includes('ProducerManager')
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'terms.mjs'),
            'const pr = prs?.get(name) ?? null', 'const pr = null'),
    },
    {
        id: 'docs-for-branch-lists-the-document-that-names-the-branch-pr-marked-off-baseline',
        why: 'the session hook injects this block verbatim; a document naming the PR on a branch the baseline never had is exactly the prior art a working-tree grep cannot show',
        run: async (binDir) => {
            const n = await notes(binDir)
            const cache = await cacheLib(binDir)
            const cacheDir = mkdtempSync(join(tmpdir(), 'inflight-for-branch-cache-'))
            const env = { ...process.env, PC_INFLIGHT_CACHE_DIR: cacheDir }
            const before = process.env.PC_INFLIGHT_CACHE_DIR
            process.env.PC_INFLIGHT_CACHE_DIR = cacheDir
            try {
                // The cached PR list, written through the library under the key it reads by.
                cache.cacheWrite('prs.json', [['terms-only', { number: 7, title: 'feat: RetryQueueDrainer drains once', state: 'OPEN', baseRefName: 'master' }]], n.PR_LIST_FIELDS)
            } finally {
                if (before === undefined) delete process.env.PC_INFLIGHT_CACHE_DIR
                else process.env.PC_INFLIGHT_CACHE_DIR = before
            }
            const r = invoke(binDir, ['docs', 'for-branch', 'terms-only'], { cwd: termsFixture(), env })
            if (r.code !== 0) return false
            const lines = r.out.split('\n')
            if (lines[0] !== 'docs context: branch facts') return false
            // The terms it used, on the first body line - the branch slug, the PR number, the title's class.
            // issue-refs: exempt-begin
            if (!/^terms from terms-only .*#7/.test(lines[1]) || !lines[1].includes('RetryQueueDrainer') || !lines[1].includes('terms-only')) return false
            // issue-refs: exempt-end
            if (!r.out.includes('- The retry queue drained twice  docs/solutions/ci/retry-queue.md  (off baseline)')) return false
            if (!/across \d+ live ref\(s\)/.test(r.out)) return false
            const last = lines.filter((l) => l.length > 0).at(-1)
            return last.startsWith('more: bin/inflight.mjs prior-art --headings ') && last.includes('RetryQueueDrainer')
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'), "sourceFrame('branch', ref,", "sourceFrame('index', ref,"),
    },
    {
        id: 'docs-for-branch-defaults-to-head-and-says-so-on-the-baseline',
        why: 'the hook passes no ref, so HEAD is the branch it answers for; on master there is nothing to look up, and the one line saying so is a note on stderr - the hook captures stdout and injected it into every master session while it was printed there',
        run: async (binDir) => {
            const git = windowGit(termsFixture())
            const wt = mkdtempSync(join(tmpdir(), 'inflight-for-branch-wt-'))
            git('worktree', 'add', '-q', '--detach', wt, 'terms-only')
            spawnSync('git', ['checkout', '-q', 'terms-only'], { cwd: wt, encoding: 'utf8' })
            // The prompt hook's fixture branch matches no document by its slug alone; a document that
            // names the branch is committed on it, so the default-ref path has a hit to show.
            mkdirSync(join(wt, 'docs', 'inflight'), { recursive: true })
            writeFileSync(join(wt, 'docs', 'inflight', 'ci-drainer.md'), '# The drainer workstream\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\nlives on terms-only\n')
            spawnSync('git', ['add', '-A'], { cwd: wt })
            spawnSync('git', ['-c', 'user.email=t@t', '-c', 'user.name=t', 'commit', '-q', '-m', 'a note naming its branch'], { cwd: wt })
            const cacheDir = mkdtempSync(join(tmpdir(), 'inflight-for-branch-cache-'))
            const env = { ...process.env, PC_INFLIGHT_CACHE_DIR: cacheDir }
            try {
                const onBranch = invoke(binDir, ['docs', 'for-branch'], { cwd: wt, env })
                if (onBranch.code !== 0 || !onBranch.out.startsWith('docs context: branch facts')) return false
                if (!onBranch.out.includes('- The drainer workstream  docs/inflight/ci-drainer.md  (off baseline)')) return false
                // On the baseline the note is the whole answer, and it is on STDERR: stdout, which
                // the session hook captures, is empty.
                const onMaster = spawnSync(process.execPath, [join(binDir, 'inflight.mjs'), 'docs', 'for-branch'], { cwd: termsFixture(), encoding: 'utf8', env })
                if (onMaster.status !== 0 || onMaster.stdout !== '') return false
                if (!onMaster.stderr.includes('docs for-branch: master is on the baseline - nothing to look up')) return false
                const explicit = spawnSync(process.execPath, [join(binDir, 'inflight.mjs'), 'docs', 'for-branch', 'master'], { cwd: wt, encoding: 'utf8', env })
                if (explicit.status !== 0 || explicit.stdout !== '' || !explicit.stderr.includes('on the baseline - nothing to look up')) return false
                // The library returns the note and emits nothing: the front door owns the channel.
                const dc = await docsCommandsLib(binDir)
                const emitted = []
                const direct = await inDir(wt, () => dc.docsForBranch(['master'], (s) => emitted.push(s)))
                return direct.ok === true && emitted.length === 0 && typeof direct.note === 'string' && direct.note.includes('nothing to look up')
            } finally {
                git('worktree', 'remove', '--force', wt)
            }
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'), "ref = head.out.trim()", "ref = 'master'"),
    },
    {
        id: 'docs-for-branch-never-calls-gh-and-is-silent-on-stdout-when-nothing-matches',
        why: 'it runs at every session start - the budget and the shared rate limit both forbid a network call there; and silence is the R12 rule for an injected block, with the coverage on stderr for a human',
        run: async (binDir) => {
            // A `gh` first on PATH that records that it was called and then fails, as an unauthenticated one would.
            const stub = mkdtempSync(join(tmpdir(), 'inflight-gh-stub-'))
            const log = join(stub, 'calls.log')
            writeFileSync(join(stub, 'gh'), `#!/bin/sh\nprintf '%s\\n' "$*" >> '${log}'\nexit 1\n`, { mode: 0o755 })
            const cacheDir = mkdtempSync(join(tmpdir(), 'inflight-for-branch-cache-'))
            const env = { ...process.env, PC_INFLIGHT_CACHE_DIR: cacheDir, PATH: `${stub}:${process.env.PATH}` }
            // A branch no fixture document names - `terms-only` is named by the note the previous
            // check commits on it, and the fixture is shared across checks in order.
            const r = spawnSync(process.execPath, [join(binDir, 'inflight.mjs'), 'docs', 'for-branch', 'feats/nothing-names-this'], { cwd: termsFixture(), encoding: 'utf8', env })
            if (r.status !== 0 || r.stdout !== '') return false
            // Positive control for the stub: a command that DOES reach for gh on a miss calls it.
            const control = spawnSync(process.execPath, [join(binDir, 'inflight.mjs'), 'note', 'drift', 'docs/inflight/note.md'], { cwd: termsFixture(), encoding: 'utf8', env })
            const stubCalled = existsSync(log)
            if (control.status !== 0 || !stubCalled) return false
            const calls = readFileSync(log, 'utf8').split('\n').filter(Boolean)
            // Exactly the control's call: for-branch added none.
            if (calls.length !== 1) return false
            return /nothing-names-this/.test(r.stderr) && /\d+ live ref/.test(r.stderr) && /not proof/i.test(r.stderr)
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'), 'prsByBranch({ network: false })', 'prsByBranch()'),
    },
    {
        id: 'docs-for-branch-records-its-own-failure-and-clears-it-on-the-next-success',
        why: 'the session hook prints nothing for this block when the command cannot run, so the record is the only place the failure exists; bare docs reads it back',
        run: async (binDir) => {
            const cache = await cacheLib(binDir)
            const cacheDir = mkdtempSync(join(tmpdir(), 'inflight-for-branch-cache-'))
            const env = { ...process.env, PC_INFLIGHT_CACHE_DIR: cacheDir }
            const withCache = (fn) => {
                const before = process.env.PC_INFLIGHT_CACHE_DIR
                process.env.PC_INFLIGHT_CACHE_DIR = cacheDir
                try { return fn() } finally {
                    if (before === undefined) delete process.env.PC_INFLIGHT_CACHE_DIR
                    else process.env.PC_INFLIGHT_CACHE_DIR = before
                }
            }
            const outside = mkdtempSync(join(tmpdir(), 'inflight-for-branch-notarepo-'))
            const failed = invoke(binDir, ['docs', 'for-branch', 'feats/anything-at-all'], { cwd: outside, env })
            if (failed.code !== 2 || !withCache(() => 'branch facts' in cache.deliveryFailures())) return false
            const ok = invoke(binDir, ['docs', 'for-branch', 'terms-only'], { cwd: termsFixture(), env })
            if (ok.code !== 0) return false
            return withCache(() => !('branch facts' in cache.deliveryFailures()))
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'), '    clearDeliveryFailure(BRANCH_DELIVERY)\n', ''),
    },
    {
        id: 'help-lists-docs-for-branch-with-its-when-line',
        why: 'the hook names this command as the branch-facts block; a command help cannot find is one the agent will not trust',
        run: async (binDir) => {
            const help = invoke(binDir, ['help'])
            if (help.code !== 0) return false
            const lines = help.out.split('\n')
            const at = lines.findIndex((l) => /^ {2}docs for-branch\b/.test(l))
            if (at < 0 || !/^\s+when: \S/.test(lines[at + 1] ?? '')) return false
            const usage = invoke(binDir, ['help', 'docs', 'for-branch'])
            return usage.code === 0 && usage.out.includes('Usage: bin/inflight.mjs docs for-branch')
        },
        mutate: (binDir) => patch(join(binDir, 'inflight.mjs'), "        name: 'for-branch',", "        name: 'for-br',"),
    },
    // ---------------------------------------------------------------------------------------------
    // THE FULL TIER'S COST IS BOUNDED BY WHAT THE HEADER SHOWS, and callers hand `drift` what they
    // already resolved. docs/plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md, KTD2.
    // ---------------------------------------------------------------------------------------------
    {
        id: 'drift-details-every-divergent-cluster-by-default-and-only-the-largest-under-previewLimit',
        why: 'note drift renders branch facts and a preview for every cluster and must keep doing so; docs show renders three, and paying for the rest was the cost this option removes',
        run: async (binDir) => {
            const n = await notes(binDir)
            return inDir(docsFixture(), () => {
                const all = n.drift('docs/inflight/note.md', { prs: new Map() })
                if (!all.found || all.divergent.length < 2) return false
                if (!all.divergent.every((c) => Array.isArray(c.branches) && c.preview !== undefined)) return false
                const one = n.drift('docs/inflight/note.md', { prs: new Map(), previewLimit: 1 })
                const detailed = one.divergent.filter((c) => c.branches !== undefined)
                // Exactly one, and it is the one the header would put first.
                return detailed.length === 1 && detailed[0].blob === n.largestFirst(one.divergent)[0].blob
                    && one.divergent.every((c) => c.branches !== undefined || c.preview === undefined)
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            'lookup: givenLookup = null, previewLimit = Infinity,', 'lookup: givenLookup = null, previewLimit = 1,'),
    },
    {
        id: 'docs-show-hands-drift-the-refs-baseline-and-lookup-it-resolved-for-the-selection',
        why: 'the selection lists the refs and looks the path up across them; asking git the same questions again inside drift doubled the header\'s fixed cost for identical answers',
        run: async (binDir) => {
            const r = invoke(binDir, ['--perf', 'docs', 'show', 'docs/inflight/note.md', '--header-only'], { cwd: manyVersionsFixture() })
            if (r.code !== 0 || !r.out.includes('=== divergence:')) return false
            return callCount(r.out, 'git for-each-ref') === 1
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'),
            '        tips: tips.tips, base, lookup, previewLimit: HEADER_TOP,', '        previewLimit: HEADER_TOP,'),
    },
    {
        id: 'docs-show-previews-only-the-versions-its-header-shows',
        why: 'one diff per divergent cluster is the size, which the ranking needs; the preview diff is for the rows the header prints, and a corpus note with a dozen divergent versions paid for nine nobody saw',
        run: async (binDir) => {
            const n = await notes(binDir)
            const dir = manyVersionsFixture()
            const divergent = await inDir(dir, () => n.drift('docs/inflight/note.md', { prs: new Map(), detail: 'summary', at: null }).divergent.length)
            if (divergent <= 3) return false
            const r = invoke(binDir, ['--perf', 'docs', 'show', 'docs/inflight/note.md', '--header-only'], { cwd: dir })
            if (r.code !== 0) return false
            // The sizes for every cluster, plus one preview for each of the three rows shown.
            return callCount(r.out, 'git diff') === divergent + 3
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-commands.mjs'),
            '        tips: tips.tips, base, lookup, previewLimit: HEADER_TOP,', '        tips: tips.tips, base, lookup,'),
    },
    {
        id: 'the-header-caps-the-adds-line-at-five-headings-and-counts-the-rest',
        why: 'a version that restructured a note adds dozens of headings, and a header line that names them all is the wall of text every other list in these views is capped against',
        run: async (binDir) => {
            const n = await notes(binDir)
            const v = await docsViews(binDir)
            return inDir(manyVersionsFixture(), () => {
                const d = n.drift('docs/inflight/note.md', { prs: new Map(), at: { ref: 'master' } })
                const box = v.formatDivergenceHeader(d, { tier: 'full' })
                const adds = box.split('\n').find((l) => l.includes('adds: "## Added 1"'))
                if (!adds) return false
                return adds.includes('"## Added 5"') && !adds.includes('"## Added 6"') && adds.endsWith(' and 2 more')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'docs-views.mjs'), 'const ADDS_SHOWN = 5', 'const ADDS_SHOWN = 50'),
    },

    // --- `rank` --------------------------------------------------------------------------------
    {
        id: 'rank-openness-follows-the-marker-not-the-word-inside-it',
        why: "`classifyNote` sets `open` from the PRESENCE of a state marker, so a note declaring `inflight-state: open - <reason>` is not open - and one is on the baseline today. A requirement written as a list of state words (closed/blocked/deferred/parked) would keep it, and the two readings disagree with nothing in the output to say which happened",
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const r = await rankIndex(binDir)
                if (!r.ok) return false
                const paths = new Set(r.groups.flatMap((g) => g.rows.map((row) => row.path)))
                // Excluded: every note carrying ANY marker, whatever word it holds.
                for (const excluded of ['bug-closed-thing', 'bug-blocked-thing', 'bug-parked-thing', 'bug-says-open']) {
                    if (paths.has(`docs/inflight/${excluded}.md`)) return false
                }
                // Kept: no marker, and a prose mention that never closes one.
                return paths.has('docs/inflight/bug-open-stall.md')
                    && paths.has('docs/inflight/bug-mentions-marker.md')
            })
        },
        // Reading openness off the WORDS keeps `bug-says-open`, which is the whole finding.
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), "new Set(['closed', 'deferred', 'registers'])", "new Set(['registers'])"),
    },
    {
        id: 'rank-emits-open-notes-no-impact-bucket-claimed-rather-than-dropping-them',
        why: 'keeping only the impact buckets silently drops the corpus\'s largest group of open notes - registers, impact-less features and unknown or misspelt impacts - and the standing register names notes in two of them, so they would vanish from the very view meant to rank them',
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const r = await rankIndex(binDir)
                if (!r.ok) return false
                const at = (key) => r.groups.find((g) => g.key === key)
                const has = (key, stem) => (at(key)?.rows ?? []).some((row) => row.path === `docs/inflight/${stem}.md`)
                return has('unmatched', 'bug-misspelt-impact') && has('feature', 'core-no-impact')
                    && has('stall', 'bug-open-stall')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), "[...INFLIGHT_IMPACT_ORDER, 'feature', 'unmatched']", '[...INFLIGHT_IMPACT_ORDER]'),
    },
    {
        id: 'rank-accounts-for-every-open-note-the-impact-buckets-did-not-claim',
        why: 'an enumeration that does not say what it excluded is a false negative wearing the authority of a completed check - the failure docs/inflight-tool.md names as the reason exit 0 and exit 2 are different answers',
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const r = await rankIndex(binDir)
                if (!r.ok) return false
                const excluded = new Map(r.excluded.map((e) => [e.key, e.count]))
                // Four notes carry a state marker in the fixture; all four are accounted for, and
                // the accounting names WHICH disposition rather than a single total.
                return (excluded.get('closed') ?? 0) === 3 && (excluded.get('deferred') ?? 0) === 1
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), 'excluded.set(key, (excluded.get(key) ?? 0) + 1)', 'excluded.set(key, 0)'),
    },
    {
        id: 'rank-says-carriage-names-no-owner-for-a-note-on-the-baseline',
        why: 'every branch cut from the baseline carries a baseline note, so listing its carrying refs reads like evidence and is none - while for a branch-only note the same field is the most informative thing in the row. Getting the asymmetry backwards makes the highest-signal rows read like the lowest',
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const r = await rankIndex(binDir)
                if (!r.ok) return false
                const row = (stem) => r.groups.flatMap((g) => g.rows).find((x) => x.path === `docs/inflight/${stem}.md`)
                const onBase = row('bug-open-stall')
                const branchOnly = row('bug-branch-only-stall')
                if (!onBase || !branchOnly) return false
                return onBase.onBaseline === true && branchOnly.onBaseline === false
                    && branchOnly.readRef === 'origin/carries-a-note'
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), 'onBaseline: index.basePaths.has(path),', 'onBaseline: false,'),
    },
    {
        id: 'rank-reads-an-archive-only-note-from-its-archival-ref',
        why: "`stranded` marks a cluster preserved when it has no live refs and `docsShape` then drops those paths entirely, so the first-sorted-LIVE-ref rule is undefined for exactly this case - an implementer following it gets `undefined` for the ref and cannot read the note at all",
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const r = await rankIndex(binDir)
                if (!r.ok) return false
                const row = r.groups.flatMap((g) => g.rows).find((x) => x.path === 'docs/inflight/bug-archived-only.md')
                return !!row && row.preserved === true && row.readRef === 'preserved/rank'
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), 'const readable = live.length > 0 ? live : all', 'const readable = live'),
    },
    {
        id: 'rank-never-prints-a-pr-number-as-an-issue',
        why: "docs/inflight/AGENTS.md makes `pr-` the deliberate exception whose number is a fork PULL REQUEST, so a `gh issue view` command built from it resolves to a different thing entirely - and AGENTS.md's own rule is that a wrong reference which resolves is worse than a broken one",
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const r = await rankIndex(binDir)
                if (!r.ok) return false
                const row = (stem) => r.groups.flatMap((g) => g.rows).find((x) => x.path === `docs/inflight/${stem}.md`)
                const pr = row('pr-207-a-pr-note')
                const issue = row('core-141-numbered-thing')
                const none = row('bug-open-stall')
                if (!pr || !issue || !none) return false
                return pr.number.kind === 'pull-request' && pr.number.command.includes('gh pr view 207')
                    && issue.number.kind === 'issue' && issue.number.command.includes('gh issue view 141')
                    && issue.number.command.includes('-R astubbs/parallel-consumer')
                    && none.number === null
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), "path.startsWith(`${NOTES_DIR}/pr-`)", 'false'),
    },
    {
        id: 'rank-never-claims-a-branch-or-pull-request-fixes-a-note',
        why: 'a note travels on the branch that produced it, so carriage is cheap and ownership is unavailable - and the worked case is a data-loss note whose own text says the bug predates the pull request of the only branch carrying it. A row reading "fixed by" that PR would be confidently wrong',
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const r = await rankIndex(binDir)
                if (!r.ok) return false
                const rendered = JSON.stringify(r.groups)
                return !/\bfix(es|ed)\b/i.test(rendered) && !/\bowns?\b/i.test(rendered)
                    && r.groups.flatMap((g) => g.rows).every((row) => row.relation === 'carries')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), "relation: 'carries'", "relation: 'fixed-by'"),
    },
    {
        id: 'rank-reports-an-unanswered-pr-snapshot-as-unknown-not-as-no-pull-request',
        why: 'an unauthenticated or rate-limited `gh` is indistinguishable from a branch that genuinely has no pull request unless the shape can carry the difference - the exact defect `prsByBranch` was changed to fix, reintroduced one layer up',
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const idx = await rankCorpus(binDir)
                const bad = rank(idx, { prs: { ok: false, reason: 'gh unavailable', map: new Map() }, register: { ok: true, text: '' } })
                if (!bad.ok) return false
                const rows = bad.groups.flatMap((g) => g.rows)
                return bad.prsOk === false && rows.length > 0 && rows.every((row) => row.prKnown === false)
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), 'prKnown: prs.ok === true', 'prKnown: true'),
    },
    {
        id: 'rank-cannot-report-a-failed-corpus-as-an-empty-backlog',
        why: '"nothing is open" and "the search never ran" are different answers, and this repository has shipped two P0s where a failure rendered as a confident empty result',
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            const r = rank({ ok: false, reason: 'cannot list refs' }, { prs: { ok: true, map: new Map() }, register: { ok: true, text: '' } })
            return r.ok === false && typeof r.reason === 'string' && r.reason.length > 0
        },
        // ANCHORED PAST `registerBlob`'s IDENTICAL GUARD. `patch` replaces the FIRST occurrence, and
        // the shorter anchor hit that one instead - so the mutant left `rank`'s own guard intact and
        // the control passed while asserting nothing. The reason expression is what makes it unique.
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'),
            'if (!index.ok) return { ok: false, reason: index.reason', 'if (false) return { ok: false, reason: index.reason'),
    },
    {
        id: 'rank-delta-gives-each-ranked-entry-the-reason-it-is-no-longer-open',
        why: 'the reason IS the finding - a note the register ranks that is gone needs deleting from the register, one that is deferred needs a schedule decision, and one sitting outside the impact buckets needs a tag. Reporting all three as "not open" turns three different actions into one shrug',
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const r = await rankIndex(binDir)
                if (!r.ok) return false
                const why = new Map(r.delta.ranked.map((e) => [e.name, e.reason]))
                return why.get('bug-gone-forever.md') === 'absent'
                    && why.get('bug-parked-thing.md') === 'deferred'
                    && why.get('bug-closed-thing.md') === 'closed'
                    && why.get('core-no-impact.md') === 'feature'
                    && !why.has('bug-open-stall.md')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), "reason: 'absent'", "reason: 'closed'"),
    },
    {
        id: 'rank-counts-a-note-the-register-ranks-by-number-as-ranked',
        why: "the register's ranked section leads EVERY line with `astubbs#NNN` rather than a filename, so a delta keyed on filenames alone reports the notes it actually ranks as unranked - the finding fires hardest exactly where the register is doing its job",
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const r = await rankIndex(binDir)
                if (!r.ok) return false
                const unranked = new Set(r.delta.unranked.map((e) => e.path))
                return !unranked.has('docs/inflight/core-141-numbered-thing.md')
                    && r.delta.unresolvable.includes(999)
                    && !r.delta.unresolvable.includes(141)
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), 'const numbered = positionalNumber(path)', 'const numbered = null'),
    },
    {
        id: 'rank-lists-unranked-rows-only-when-a-group-scopes-the-call',
        why: 'the register names a handful of notes against a corpus of hundreds, so an unscoped unranked list is every open note in the repository - the whole-corpus dump the command exists to avoid, arriving under the name "delta"',
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const bare = await rankIndex(binDir)
                const scoped = await rankIndex(binDir, 'stall')
                if (!bare.ok || !scoped.ok) return false
                return bare.delta.unranked.length === 0 && bare.delta.unrankedCounts.length > 0
                    && scoped.delta.unranked.length > 0
                    && scoped.delta.unranked.every((e) => e.group === 'stall')
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), 'const listUnranked = group !== null', 'const listUnranked = true'),
    },
    {
        id: 'rank-fails-the-run-when-the-register-could-not-be-read',
        why: '"the delta was empty" and "the delta never ran" are different answers, and the delta is the deliverable - so a register that could not be read is a failed run, reported after everything that did run, the way refactor-window already reports an unmeasurable candidate',
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const idx = await rankCorpus(binDir)
                const r = rank(idx, { prs: { ok: true, map: new Map() }, register: { ok: false, reason: 'no such blob' } })
                // It still ANSWERED the part it could: groups are present, and only the delta failed.
                return r.ok === true && r.groups.length > 0 && r.delta.ok === false
                    && typeof r.delta.reason === 'string' && r.delta.reason.length > 0
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'), 'if (register.ok !== true) {', 'if (false) {'),
    },
    {
        id: 'rank-does-not-pick-one-note-when-a-number-resolves-to-several',
        why: "filenames get recycled and renamed here, so a note and its own dead predecessor sit on different refs carrying the same positional number - and a map keeping the last writer reported the register's LIVE entry as stale while naming the dead copy. Found by running the command against this repository, where astubbs#177 resolves to two notes",
        run: async (binDir) => {
            const { rank } = await rankLib(binDir)
            return inRankFixture(async () => {
                const bare = await rankIndex(binDir)
                // SCOPED, because the unranked half is a count until a group scopes the call - an
                // assertion about it on the bare call is vacuously true and the mutant survived it.
                const scoped = await rankIndex(binDir, 'unmatched')
                if (!bare.ok || !scoped.ok) return false
                // 141 resolves to the open `crash` note AND to a dead twin sitting in `unmatched`.
                // The register's entry is satisfied by the open one, so neither is a finding - and
                // above all the dead twin is never named as the reason the entry is stale.
                const named = JSON.stringify(bare.delta.byNumber) + JSON.stringify(bare.delta.ranked)
                const unranked = new Set(scoped.delta.unranked.map((e) => e.name))
                return !named.includes('bug-141-older-name.md')
                    && !bare.delta.byNumber.some((e) => e.number === 141)
                    && !unranked.has('bug-141-older-name.md')
            })
        },
        // Keeping only ONE candidate per number is the defect: the dead twin wins and is reported.
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'),
            'byNumber.get(numbered.value).push(name)', 'byNumber.set(numbered.value, [name])'),
    },
    {
        id: 'rank-resolves-a-pull-request-for-a-remote-ref-by-its-bare-branch-name',
        why: "`prsByBranch` maps `headRefName`, which never carries the `origin/` prefix, while a corpus ref almost always does - so a lookup on the full ref returns null for every remote branch and the entire corpus reads as pull-request-less while `prs.ok` is true. That is the shape where a wrong answer is indistinguishable from a true one, since no row says UNKNOWN",
        run: async (binDir) => {
            const { rank, registerBlob } = await rankLib(binDir)
            return inRankFixture(async () => {
                const index = await rankCorpus(binDir)
                const prs = { ok: true, map: new Map([['carries-a-note', { number: 4242, state: 'OPEN', title: 't' }]]) }
                const r = rank(index, { prs, register: registerBlob(index) })
                if (!r.ok) return false
                const row = r.groups.flatMap((g) => g.rows).find((x) => x.name === 'bug-branch-only-stall.md')
                return !!row && row.readRef === 'origin/carries-a-note' && row.pr !== null && row.pr.number === 4242
            })
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'rank.mjs'),
            "prs.map.get(readRef.replace(/^origin\\//, ''))", 'prs.map.get(readRef)'),
    },
]

/**
 * The corpus fixture plus MANY divergent versions of one note: five branches each appending a
 * distinct line, and one adding seven headings - more clusters than the header shows and more
 * headings than its adds line names. Its own repository for the reason bin/lib/fixture-repos.mjs
 * gives: the drift checks assert exact counts on the shared one.
 */
let MANY = null
function manyVersionsFixture() {
    if (MANY) return MANY
    const { dir, git, commit, write, NOTE } = buildDocsFixture()
    for (let i = 1; i <= 5; i++) {
        git('checkout', '-q', '-b', `version-${i}`, 'master')
        write('docs/inflight/note.md', `${NOTE}line only version ${i} adds\n`)
        commit(`version ${i}`)
    }
    git('checkout', '-q', '-b', 'many-headings', 'master')
    write('docs/inflight/note.md', `${NOTE}${Array.from({ length: 7 }, (_, i) => `\n## Added ${i + 1}\n\ntext\n`).join('')}`)
    commit('seven headings')
    git('checkout', '-q', 'master')
    MANY = dir
    return dir
}

/**
 * THE `rank` CORPUS: one note per situation `rank` has to get right, so every check below has a
 * known right answer rather than an ambient one.
 *
 * Its own repository, and deliberately NOT an extension of `buildFixture()`: several checks there
 * assert exact counts against that tree, so adding a note to it would break them for a reason that
 * has nothing to do with what they test. `manyVersionsFixture` and `buildRenameFixture` are the
 * precedent.
 *
 * What each note is for:
 *
 *   bug-open-stall              open, impact stall - the ordinary row, and the register names it
 *   bug-closed-thing            state `closed`     - excluded, and the register names it
 *   bug-blocked-thing           state `blocked`    - excluded; the word is neither closed nor parked
 *   bug-parked-thing            `parked - deferred`- excluded, and the word sits mid-state
 *   bug-says-open               `open - <reason>`  - EXCLUDED, because the marker's PRESENCE decides
 *                                                    openness, not the word inside it
 *   bug-mentions-marker         prose only         - still open: no closing `-->`, so no marker
 *   bug-misspelt-impact         impact `stalll`    - unmatched, never silently dropped
 *   core-no-impact              feature, no impact - feature group, and the register names it
 *   core-141-numbered-thing     `<area>-NNN-<slug>`- the positional number, which the register
 *                                                    ranks by number rather than by filename
 *   pr-207-a-pr-note            `pr-` prefix       - a fork PR number, never printed as an issue
 *   bug-branch-only-stall       one branch only    - carriage names a branch here, unlike on master
 *   bug-archived-only           one TAG only       - preserved, and read from an archival ref
 */
let RANK = null
function rankFixture() {
    if (RANK) return RANK
    const { dir, git, commit } = windowRepo()
    const write = (rel, body) => {
        mkdirSync(join(dir, dirname(rel)), { recursive: true })
        writeFileSync(join(dir, rel), body)
    }
    const note = (name, body) => write(`docs/inflight/${name}.md`, body)
    const tags = (type, impact, state) => `<!-- inflight-type: ${type} -->\n`
        + (impact ? `<!-- inflight-impact: ${impact} -->\n` : '')
        + (state ? `<!-- inflight-state: ${state} -->\n` : '')

    note('bug-open-stall', `# An open stall\n\n${tags('bug', 'stall')}\nbody\n`)
    note('bug-closed-thing', `# A closed thing\n\n${tags('bug', 'stall', 'closed - it landed')}\nbody\n`)
    note('bug-blocked-thing', `# A blocked thing\n\n${tags('bug', 'stall', 'blocked - waiting on a decision')}\nbody\n`)
    note('bug-parked-thing', `# A parked thing\n\n${tags('bug', 'stall', 'parked - deferred, after v6')}\nbody\n`)
    // The word `open` inside the state does NOT make it open - the marker's presence decides.
    note('bug-says-open', `# It says open\n\n${tags('bug', 'stall', 'open - still going')}\nbody\n`)
    // A mention with no closing `-->` is prose, not a marker. Last line, so nothing closes it later.
    note('bug-mentions-marker', `# It only mentions the marker\n\n${tags('bug', 'stall')}\nprose about inflight-state: closed\n`)
    note('bug-misspelt-impact', `# A misspelt impact\n\n${tags('bug', 'stalll')}\nbody\n`)
    note('core-no-impact', `# A feature with no impact\n\n${tags('feature', '')}\nbody\n`)
    note('core-141-numbered-thing', `# A numbered note\n\n${tags('bug', 'crash')}\nbody\n`)
    note('pr-207-a-pr-note', `# A note about a pull request\n\n${tags('task', 'ci')}\nbody\n`)
    write('docs/inflight/process-candidate-ranking.md', [
        '# Next candidates, ranked', '', '<!-- inflight-type: register -->', '',
        '- `bug-open-stall.md` - open, so no delta row',
        '- `bug-gone-forever.md` - on no ref at all',
        '- `bug-parked-thing.md` - deferred',
        '- `bug-closed-thing.md` - closed',
        '- `core-no-impact.md` - open, but no impact bucket claims it',
        '- astubbs#141 - ranked by NUMBER, and the note that carries it is open',
        '- astubbs#999 - resolves to no note on any ref', '',
    ].join('\n'))
    commit('the rank corpus')

    git('checkout', '-q', '-b', 'carries-a-note')
    note('bug-branch-only-stall', `# A note only this branch has\n\n${tags('bug', 'stall')}\nbody\n`)
    // The SAME positional number as core-141-numbered-thing, under a dead earlier name. Filenames
    // get recycled and renamed here, so one number resolving to two notes is ordinary, not exotic.
    note('bug-141-older-name', `# The renamed predecessor\n\n${tags('bug', 'stalll')}\nbody\n`)
    commit('a note master never had, and a stale twin of a numbered one')
    // A REMOTE ref, because a real corpus is overwhelmingly `origin/*` - and the PR snapshot is
    // keyed on the bare branch name, so a lookup that forgets to strip the prefix silently reports
    // every remote branch as having no pull request.
    git('update-ref', 'refs/remotes/origin/carries-a-note', git('rev-parse', 'HEAD'))
    git('checkout', '-q', 'master')
    git('branch', '-q', '-D', 'carries-a-note')

    // Held ONLY by a tag: preserved on purpose, so it has no live ref to be read from.
    git('checkout', '-q', '-b', 'to-tag', 'master')
    note('bug-archived-only', `# A note only an archive has\n\n${tags('bug', 'stall')}\nbody\n`)
    commit('parked before a re-cut')
    git('tag', 'preserved/rank')
    git('checkout', '-q', 'master')
    git('branch', '-q', '-D', 'to-tag')

    RANK = dir
    return dir
}

/** Run a predicate with the rank fixture as the working directory, so the libraries read it. */
async function inRankFixture(fn) {
    const before = cwd()
    chdir(rankFixture())
    try { return await fn(rankFixture()) } finally { chdir(before) }
}

console.log('bin/test-inflight.mjs - front door and prior-art library self-test\n')

console.log('REAL tree:')
for (const c of CHECKS) {
    let ok
    try { ok = await c.run(BIN) } catch (e) { ok = false; console.log(`       (threw: ${e.message})`) }
    report(ok, c.id)
}

console.log('\nNEGATIVE CONTROLS - each mutant must make its own check go RED:')
for (const c of CHECKS) {
    // THE MUTANT MIRRORS THE REPO LAYOUT, not just bin/. A hook now imports the library, so the
    // unit under test spans `.claude/hooks/` too - and a flat copy of bin/ left the hook's own
    // mutation with no file to patch. `binDir` still points at the bin directory, so every check
    // that reaches for `lib/` or `inflight.mjs` is unchanged.
    const root = mkdtempSync(join(tmpdir(), 'inflight-selftest-'))
    const tmp = join(root, 'bin')
    cpSync(BIN, tmp, { recursive: true })
    cpSync(join(BIN, '..', '.claude', 'hooks'), join(root, '.claude', 'hooks'), { recursive: true })
    try {
        c.mutate(tmp)
    } catch (e) {
        report(false, `mutant of ${c.id} COULD NOT BE BUILT - ${e.message}`)
        continue
    }
    let stillGreen
    // A mutant that crashes the check is red, which is the point.
    try { stillGreen = await c.run(tmp) } catch { stillGreen = false }
    report(!stillGreen, `mutant of ${c.id} goes red`)
}

console.log()
if (failures === 0) {
    console.log(`All ${CHECKS.length * 2} self-test(s) passed`)
    process.exit(0)
}
console.log(`${failures} self-test(s) FAILED`)
process.exit(1)
