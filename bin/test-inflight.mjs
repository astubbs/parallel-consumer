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
import { cpSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { mkdtempSync as mkTmp } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const BIN = dirname(fileURLToPath(import.meta.url))

let failures = 0
const report = (ok, label) => {
    console.log(`${ok ? '  ok  ' : '  FAIL'} ${label}`)
    if (!ok) failures++
}

/** Run a front door (real or mutant) as a subprocess - the CLI contract is a process-level fact. */
function invoke(binDir, args, opts = {}) {
    const r = spawnSync(process.execPath, [join(binDir, 'inflight.mjs'), ...args], { encoding: 'utf8', ...opts })
    return { code: r.status, out: `${r.stdout ?? ''}${r.stderr ?? ''}` }
}

const lib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'prior-art.mjs')).href)
const notes = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'notes.mjs')).href)
const gitlib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'git.mjs')).href)
const front = (binDir) => import(pathToFileURL(join(binDir, 'inflight.mjs')).href)

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
        id: 'prior-art-returns-findings-not-a-code',
        why: 'every queued feature - drift, headings, ref clustering - is a view over the data, not over the page',
        run: async (binDir) => {
            const { priorArt } = await lib(binDir)
            const r = priorArt(['a-term-that-cannot-match-anything-xyzzy'], { github: false })
            return r !== null && typeof r === 'object'
                && r.ok === true && Array.isArray(r.sections) && Array.isArray(r.warnings)
                && typeof r.refsSearched === 'number' && r.refsSearched > 0
                && r.sections.every((s) => Array.isArray(s.hits) && typeof s.heading === 'string')
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
            const r = priorArt(['a-term-that-cannot-match-anything-xyzzy'], { github: false })
            return r.ok === true && r.sections.every((s) => s.hits.length === 0) && r.refsSearched > 0
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
            const r = priorArt([TERM_IN_DOCS], { github: false })
            if (!r.ok) return false
            const all = r.sections.flatMap((s) => s.hits.map((h) => h.path))
            if (all.length === 0) return false // a check that cannot see anything cannot pass
            return new Set(all).size === all.length
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'prior-art.mjs'),
            "            'docs/', ':(exclude)docs/plans/', ':(exclude)docs/solutions/', ':(exclude)docs/inflight/']],",
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
            const tips = g.refTips()
            if (!tips.ok) return false
            const refs = tips.tips.map((r) => r.ref)
            const m = g.blobsForPath(refs, 'docs/inflight/AGENTS.md')
            const known = new Set(refs)
            if (m.size === 0 || ![...m.keys()].every((k) => known.has(k))) return false
            const base = g.baseline()
            const direct = g.exec('git', ['rev-parse', `${base}:docs/inflight/AGENTS.md`]).out.trim()
            return m.get(base) === direct
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
            const index = n.corpusIndex()
            if (index.baseEverPaths.size === 0) return false
            const reported = new Set(n.stranded(index).flatMap((c) => c.paths))
            return [...index.baseEverPaths].every((p) => !reported.has(p))
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            '        if (index.baseEverPaths.has(path)) {', '        if (false) {'),
    },
    {
        id: 'stranded-is-clustered-not-listed',
        why: '364 separate lines is a result an agent stops reading, which is the same as no result',
        run: async (binDir) => {
            const n = await notes(binDir)
            const clusters = n.stranded(n.corpusIndex())
            const paths = clusters.reduce((t, c) => t + c.paths.length, 0)
            return paths > 0 && clusters.length < paths
        },
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            "        const key = s.refs.join(' ')", '        const key = s.path'),
    },
    {
        id: 'drift-reports-divergence-not-distance',
        why: 'a branch that has not merged recently is not drift, and it is most of the volume',
        run: async (binDir) => {
            const n = await notes(binDir)
            const found = await aDriftedNote(binDir)
            if (!found) return false // no note qualifies: cannot test, which is not a pass
            const history = n.baselineHistoryBlobs(found.drift.baseline, found.path)
            return found.drift.divergent.every((c) => !history.has(c.blob))
        },
        // Mutating the `behind` push emptied `behind` too, so the check failed on its own guard
        // rather than on the assertion its `why` names - a control that never exercised the line it
        // exists for. This keeps the bookkeeping intact and corrupts only the SPLIT, so a historical
        // blob lands in `divergent` and the final `.every(...)` is what goes red.
        mutate: (binDir) => patch(join(binDir, 'lib', 'notes.mjs'),
            '        if (history.has(blob)) behind.push({ blob, refs })\n        else divergent.push(entry)',
            '        if (history.has(blob)) { behind.push({ blob, refs }); divergent.push(entry) }\n'
            + '        else divergent.push(entry)'),
    },
    {
        id: 'drift-clusters-by-blob-not-by-ref',
        why: 'diffing once per ref instead of once per version is 274 diffs where 37 will do',
        run: async (binDir) => {
            const found = await aDriftedNote(binDir)
            if (!found) return false
            const d = found.drift
            const all = [...d.divergent, ...(d.baselineCluster ? [d.baselineCluster] : [])]
            const blobs = all.map((c) => c.blob)
            if (!(blobs.length > 0 && blobs.length < d.refsCarrying && new Set(blobs).size === blobs.length)) return false
            // `added` is measured against each branch's merge-base and rendered as the headline
            // number; a swap or a missing computation would otherwise turn nothing red.
            return d.divergent.every((c) => c.added !== null
                && (c.added.newFile === true || (Number.isInteger(c.added.added) && Number.isInteger(c.added.removed))))
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
            const found = await aDriftedNote(binDir)
            if (!found) return false
            const runs = [
                [['stranded'], 'NEVER reached'],
                [['note', 'find', 'inflight'], 'matching'],
                [['note', 'drift', found.path], found.path],
                [['note', 'drift', '--all', found.path], found.path],
                [['prior-art', 'a-term-that-cannot-match-anything-xyzzy'], 'nothing'],
            ]
            return runs.every(([args, needle]) => {
                const r = invoke(binDir, args)
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
    const tmp = mkdtempSync(join(tmpdir(), 'inflight-selftest-'))
    cpSync(BIN, tmp, { recursive: true })
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
