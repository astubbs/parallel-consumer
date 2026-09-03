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
import { chdir, cwd } from 'node:process'
import {
    cpSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, symlinkSync, utimesSync,
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

/** Run a front door (real or mutant) as a subprocess - the CLI contract is a process-level fact. */
/** A command that RAN, whatever it found - exit 0. `could not run` is 2. */
const r0 = (r) => r.code === 0

function invoke(binDir, args, opts = {}) {
    const r = spawnSync(process.execPath, [join(binDir, 'inflight.mjs'), ...args], { encoding: 'utf8', ...opts })
    return { code: r.status, out: `${r.stdout ?? ''}${r.stderr ?? ''}` }
}

const lib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'prior-art.mjs')).href)
const notes = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'notes.mjs')).href)
const gitlib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'git.mjs')).href)
const front = (binDir) => import(pathToFileURL(join(binDir, 'inflight.mjs')).href)
const branches = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'branches.mjs')).href)

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

function windowGit(dir) {
    return (...args) => {
        const r = spawnSync('git', args, { cwd: dir, encoding: 'utf8' })
        if (r.status !== 0) throw new Error(`fixture: git ${args.join(' ')} failed: ${r.stderr}`)
        return r.stdout.trim()
    }
}

function windowRepo() {
    const dir = mkdtempSync(join(tmpdir(), 'inflight-window-'))
    const git = windowGit(dir)
    git('init', '-q', '-b', 'master')
    return { dir, git, commit: (m) => { git('add', '-A'); git('-c', 'user.email=t@t', '-c', 'user.name=t', 'commit', '-q', '-m', m) } }
}

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
 * ITS OWN REPOSITORY, NOT THE SHARED FIXTURE. The drift checks assert exact counts on shared.md
 * (`divergent.length === 1`), and the mutant phase re-runs every check against whatever the earlier
 * ones left behind - so growing that note's divergent set here would turn an unrelated check red
 * one phase later, with nothing pointing back at the cause.
 */
function buildDocsFixture() {
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
    return dir
}

let DOCS = null
const docsFixture = () => (DOCS ??= buildDocsFixture())

const views = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'views.mjs')).href)
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
        // separate empty-refs guard, so the mutant stayed green while proving nothing.
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            "    if (!ok) return { ok: false, reason: 'cannot list refs - is this a git repository?' }\n"
            + "    if (refs.length === 0) return { ok: false, reason: 'no branch refs found - nothing to search' }\n"
            + "    if (!base) return { ok: false, reason: 'neither origin/master nor master resolves"
            + " - no baseline to compare against' }",
            '    // mutant: every corpus guard removed'),
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
        id: 'source-frame-puts-the-label-first-and-the-command-last',
        why: 'an agent tells a fresh signal from a repeat by its first line, and always needs the next command',
        run: async (binDir) => {
            const v = await views(binDir)
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
        mutate: (binDir) => patch(join(binDir, 'lib', 'views.mjs'),
            '    return [label, body.trimEnd(), `more: ${moreCommand}`].join(\'\\n\')',
            '    return [`more: ${moreCommand}`, body.trimEnd(), label].join(\'\\n\')'),
    },
    {
        id: 'the-divergence-header-previews-and-names-the-rest-command',
        why: 'a header that shows counts without the evidence or the next command is an assertion, not a finding',
        run: async (binDir) => {
            const n = await notes(binDir)
            const v = await views(binDir)
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
        mutate: (binDir) => patch(join(binDir, 'lib', 'views.mjs'),
            '    const rest = `bin/inflight.mjs note drift ${d.path}`', "    const rest = 'bin/inflight.mjs note drift'"),
    },
]

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
