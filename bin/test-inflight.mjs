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
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const BIN = dirname(fileURLToPath(import.meta.url))

let failures = 0
const report = (ok, label) => {
    console.log(`${ok ? '  ok  ' : '  FAIL'} ${label}`)
    if (!ok) failures++
}

/** Run a front door (real or mutant) as a subprocess - the CLI contract is a process-level fact. */
function invoke(binDir, args) {
    const r = spawnSync(process.execPath, [join(binDir, 'inflight.mjs'), ...args], { encoding: 'utf8' })
    return { code: r.status, out: `${r.stdout ?? ''}${r.stderr ?? ''}` }
}

const lib = (binDir) => import(pathToFileURL(join(binDir, 'lib', 'prior-art.mjs')).href)

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
const registeredNames = (binDir) =>
    [...readFileSync(join(binDir, 'inflight.mjs'), 'utf8').matchAll(/^\s{8}name: '([a-z-]+)',$/gm)].map((m) => m[1])

/** A term that exists in docs/inflight/ on many refs - so overlap, if present, is visible. */
const TERM_IN_DOCS = 'inflight-impact'

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
            const names = registeredNames(binDir)
            if (names.length === 0) return false
            const help = invoke(binDir, ['help']).out
            return names.every((n) => help.includes(n))
        },
        mutate: (binDir) => {
            const f = join(binDir, 'inflight.mjs')
            writeFileSync(f, readFileSync(f, 'utf8').replace(/\.\.\.COMMANDS\.flatMap\(\(c\) => \[[\s\S]*?\n {8}\]\),/, ''))
        },
    },
    {
        id: 'usage-names-the-front-door-not-the-library',
        why: 'a help screen naming a path that no longer runs is citation rot inside the tool',
        run: async (binDir) => registeredNames(binDir).every((n) => {
            const usage = invoke(binDir, ['help', n]).out
            return usage.includes(`bin/inflight.mjs ${n}`) && !/bin\/(?!inflight)[a-z-]+\.mjs/.test(usage)
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
        mutate: (binDir) => {
            const f = join(binDir, 'inflight.mjs')
            patch(f,
                "    if (!command) return { ok: false, reason: `inflight: no such command '${name}'\\n\\n${help()}` }",
                '    if (!command) return { ok: true }')
        },
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
        mutate: (binDir) => {
            const f = join(binDir, 'inflight.mjs')
            patch(f,
                '        return { ok: true, reason: help() }',
                '        return { ok: false, reason: help() }')
        },
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
