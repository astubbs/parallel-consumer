// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE VETTING WORKLIST: which open notes on the baseline to re-read first, and what about each one
// looks stale before anyone opens it.
//
// WHY THIS EXISTS. docs/inflight/AGENTS.md asks that a note be removed or given a state the moment
// it stops describing reality, and that duty fires at add time - "when you add a note, look at the
// others". It did not hold: on 2026-09-07 the baseline carried 145 open notes over about 16k lines,
// and nothing recorded when any of them was last confirmed against the tree. `git log` could not
// say - the package rename of 2026-08-26/27 rewrote every file - so "still real?" had to be answered
// by reading all of them, which is the cost that stops it being done. This view makes the sweep
// incremental: the `inflight-vetted` marker partitions vetted from unvetted, and the cheap signals
// below order the unvetted so the likeliest-dead notes are read first.
//
// A SIGNAL IS A REASON TO LOOK, NEVER A VERDICT. Every one of them has an innocent reading: a note
// cites a merged pull request because that pull request is where the problem was found; an
// identifier is missing from the tree because the note proposes it. The row says what fired and
// leaves the decision to whoever reads the note - the five outcomes are AGENTS.md's, not this
// file's, and none of them is "the tool said so".
//
// ORDER IS IMPACT, THEN AGE OLDEST FIRST - operator ruling, 2026-09-07, the same rule `rank` now
// follows and imports from here rather than restating. Signals annotate a row; they never move it.
// A scoring scheme was considered and rejected: a note with three weak signals is not more suspect
// than a note with one strong one, and inventing weights would put a number on a judgement.
//
// BASELINE ONLY, deliberately. A note that exists only on a branch is vetted when that branch
// merges, by the merge; what needs a sweep is the set every session inherits. `rank` reads every
// ref for the opposite reason - it wants to know what any branch still holds open.
//
// No process.exit, no printing: bin/inflight.mjs owns the process boundary and bin/lib/views.mjs
// renders. `vet` is pure over its inputs so bin/test-inflight.mjs can drive it on a fixture with no
// network; the thin git wrappers at the bottom are held apart for exactly that reason.

import { baseline, blobContents, exec, lines, treeEntries } from './git.mjs'
import { INFLIGHT_IMPACT_ORDER, classifyNote } from './inflight-tags.mjs'
import { NOTES_DIR } from './repo.mjs'
import { DIRECTORY_DOCS_RE, DOCUMENT_RE, INFLIGHT_GROUPS, INFLIGHT_GROUP_ORDER, inflightGroupOf } from './docs-shape.mjs'
import { byAgeThenPath, numberFor } from './rank.mjs'

/** A fork number as notes cite it, either spelling; the number is the capture. */
const FORK_NUMBER_RE = /astubbs(?:\/parallel-consumer)?#(\d+)(?!\d)/g

/** The states a number can be in and still be open work. Everything else is settled. */
const OPEN_STATES = new Set(['OPEN'])

/**
 * The code-shaped strings a note cites in backticks, split by how they are checked.
 *
 * ONLY WHAT IS INSIDE BACKTICKS, because that is the convention for a citation here - "cite the path
 * plus the smallest distinctive greppable string" - and prose outside them is prose. Three shapes:
 * a path with a source extension (checked against the tree), a CamelCase identifier of the kind a
 * Java class or a constant carries (checked against the source), and a `name()` call. All-caps
 * words are excluded: `README`, `CI`, `PR` are vocabulary, not anchors. Anything shorter than four
 * letters is excluded for the same reason.
 */
export function anchorsOf(text) {
    const paths = new Set()
    const symbols = new Set()
    for (const m of text.matchAll(/`([^`\n]+)`/g)) {
        const s = m[1].trim()
        // AN ELIDED PATH IS NOT A CITATION - `a/b/.../c.java` names a shape, not a file.
        if (s.includes('...')) continue
        if (/^[\w./-]+\.(java|kt|mjs|js|sh|md|yml|yaml|xml|adoc|json|properties)$/.test(s)) {
            // A RELATIVE CITATION IS RELATIVE TO THE NOTE, which lives in NOTES_DIR: `../solutions/x.md`
            // is a link that resolves in a rendered note and must resolve the same way here.
            if (s.startsWith('../')) { paths.add(`${NOTES_DIR}/${s}`.replace(/\/[^/]+\/\.\.\//g, '/')); continue }
            // A BARE FILENAME IS CHECKED ONLY WHEN IT IS THE KIND THIS REPOSITORY AUTHORS. Bare
            // `spotbugsXml.xml`, `daemon.json`, `maven-metadata.xml` are tool output a note names by
            // its well-known name; a bare `.java` or `.md` is something an author wrote and can lose.
            if (!s.includes('/') && !/\.(java|kt|mjs|js|sh|md|adoc|yml|yaml)$/.test(s)) continue
            paths.add(s)
            continue
        }
        if (/^[A-Z][a-z][A-Za-z0-9]{2,}(?:\.[A-Za-z_][A-Za-z0-9_]*)?(?:\(\))?$/.test(s)) { symbols.add(s.replace(/\(\)$/, '').split('.')[0]); continue }
        if (/^[a-z][A-Za-z0-9_]{3,}\(\)$/.test(s)) symbols.add(s.replace(/\(\)$/, ''))
    }
    return { paths: [...paths], symbols: [...symbols] }
}

/**
 * The condition a note says it should be deleted on, or null.
 *
 * A `## Delete when` heading (`##` or deeper, `this note`/`this file` optional) yields the first
 * non-empty, non-comment line under it; a sentence of the form "delete this note when ..." yields itself.
 * "Remove" is not matched - on this corpus it is prose about code ("throws on `remove`"), not an
 * instruction about the note. Capped so a paragraph-long condition stays one row.
 */
export function deleteWhenCondition(text) {
    const all = text.split('\n')
    for (let i = 0; i < all.length; i += 1) {
        const l = all[i]
        // `##` OR DEEPER, never the H1: a note TITLED "Delete when ..." is not stating a condition
        // under that title, and the first non-empty line beneath an H1 is the tag block.
        if (/^#{2,}\s*delete(\s+(this|the)\s+(note|file|it))?\s+when\b/i.test(l)) {
            const body = all.slice(i + 1).find((x) => x.trim().length > 0 && !x.trim().startsWith('<!--')) ?? ''
            return (body.trim() || l.trim()).slice(0, 160)
        }
        if (/\bdelete\s+(this|the)\s+(note|file)\s+when\b/i.test(l) && !/never leave/i.test(l)) return l.trim().slice(0, 160)
    }
    return null
}

/**
 * Does a cited path resolve on the tree? A bare filename resolves when any tree path ends in it -
 * notes cite `PartitionState.java` without its directory as often as with - and so does a PARTIAL
 * path, `state/PartitionState.java`, which is how a note names a class under a package without
 * spelling the module. Exact first because it is the common case and a Set lookup; the suffix
 * scan is the fallback and is paid once per partial citation, not per note.
 */
const pathResolves = (cited, tree, basenames, suffixes) => {
    if (tree.has(cited)) return true
    if (!cited.includes('/')) return basenames.has(cited)
    return suffixes.some((p) => p.endsWith(`/${cited}`))
}

/**
 * The worklist.
 *
 * @param {{path: string, text: string}[]} notes every note on the baseline, with its content
 * @param {{numbers: {ok: boolean, map: Map<number, {kind: string, state: string}>, reason?: string},
 *          tree: {ok: boolean, paths: Set<string>},
 *          symbols: {ok: boolean, present: Set<string>},
 *          ages: {ok: boolean, dates: Map<string, string>},
 *          area?: string|null, all?: boolean, baseline: string}} opts
 *   `symbols.present` is the subset of every cited symbol that the source actually contains -
 *   `symbolsPresent` computes it in one process; `tree.paths` is the baseline's file list.
 */
export function vet(notes, { numbers, tree, symbols, ages, area = null, all = false, baseline: base }) {
    const basenames = new Set([...tree.paths].map((p) => p.replace(/^.*\//, '')))
    const suffixes = [...tree.paths]
    const rows = []
    const excluded = new Map()
    let vettedCount = 0
    for (const { path, text } of notes) {
        if (!DOCUMENT_RE.test(path) || DIRECTORY_DOCS_RE.test(path)) continue
        const name = path.slice(NOTES_DIR.length + 1)
        if (area !== null && !name.startsWith(`${area}-`)) continue
        const note = classifyNote(text, path)
        const group = inflightGroupOf(note)
        // DEFERRED AND CLOSED ARE OUT unless asked for: AGENTS.md's schedule rule is that all open
        // work happens before any deferred work, so a sweep reads the deferred section when the
        // open one is empty - and a closed note is one the sweep already produced.
        if (!all && (group === 'closed' || group === 'deferred')) {
            excluded.set(group, (excluded.get(group) ?? 0) + 1)
            continue
        }

        const signals = []

        // EVERY CITED NUMBER SETTLED. The innocent reading is the common one - a note names the
        // pull request that found the problem - so the signal fires only when NOTHING it cites is
        // still open, which is when "the work this note tracks has all landed" becomes plausible.
        const cited = [...new Set([...text.matchAll(FORK_NUMBER_RE)].map((m) => Number(m[1])))]
        if (numbers.ok && cited.length > 0) {
            const known = cited.filter((n) => numbers.map.has(n))
            const open = known.filter((n) => OPEN_STATES.has(numbers.map.get(n).state))
            if (known.length > 0 && open.length === 0) {
                signals.push({
                    key: 'all-cited-numbers-settled',
                    detail: `cites ${known.length === 1 ? 'one fork number and it is' : `${known.length} fork numbers and every one is`} settled: `
                        + known.map((n) => `astubbs#${n} ${numbers.map.get(n).state} (${numbers.map.get(n).kind})`).join(', '),
                })
            }
        }

        // THE NUMBER IN THE FILENAME IS SETTLED. Stronger than a citation: the filename says the
        // note is ABOUT that issue or pull request, and a closed subject is a closed note unless the
        // text says what survived it.
        const own = numberFor(path, text)
        if (numbers.ok && own && own.attribution !== 'upstream' && numbers.map.has(own.value)) {
            const { kind, state } = numbers.map.get(own.value)
            if (!OPEN_STATES.has(state)) {
                signals.push({ key: 'filename-number-settled', detail: `the ${kind} in its filename, astubbs#${own.value}, is ${state}` })
            }
        }

        // A CITED ANCHOR THAT NO LONGER RESOLVES. The path half of this is what bin/check-file-refs.sh
        // enforces; the symbol half is the part the convention leaves to the author, and the part
        // that goes stale when a class is renamed or a method removed.
        const anchors = anchorsOf(text)
        const missingPaths = tree.ok ? anchors.paths.filter((p) => !pathResolves(p, tree.paths, basenames, suffixes)) : []
        const missingSymbols = symbols.ok ? anchors.symbols.filter((s) => !symbols.present.has(s)) : []
        const missing = [...missingPaths, ...missingSymbols]
        if (missing.length > 0) {
            signals.push({
                key: 'anchor-missing',
                detail: `${missing.length === 1 ? 'a cited anchor does' : `${missing.length} cited anchors do`} not resolve on ${base}: `
                    + missing.slice(0, 6).map((m) => `\`${m}\``).join(', ') + (missing.length > 6 ? ', ...' : ''),
            })
        }

        // A "DELETE WHEN" CONDITION. AGENTS.md: never leave a "delete this when #NN merges" marker
        // on master, because the merge is exactly when nobody is looking here. On this corpus the
        // marker is usually a `## Delete when` section, so the detail is the CONDITION under it - a
        // vetter can evaluate that on sight, where the heading alone only says one exists.
        const deleteWhen = deleteWhenCondition(text)
        if (deleteWhen) signals.push({ key: 'delete-when', detail: `states its own deletion condition: "${deleteWhen}"` })

        if (note.vetted) vettedCount += 1
        rows.push({
            path,
            name,
            title: note.title,
            group,
            impact: note.impact,
            type: note.type,
            vetted: note.vetted,
            age: ages.ok ? (ages.dates.get(path) ?? null) : null,
            signals,
        })
    }

    // The group order is the index's - registers first, then the impact scale, then the open notes
    // no impact claims - so the sweep meets the notes in the order every session does.
    const order = new Map(INFLIGHT_GROUP_ORDER.map((k, i) => [k, i]))
    rows.sort((a, b) => (order.get(a.group) ?? 99) - (order.get(b.group) ?? 99) || byAgeThenPath(a, b))

    const groups = []
    for (const key of INFLIGHT_GROUP_ORDER) {
        const here = rows.filter((r) => r.group === key)
        if (here.length > 0) groups.push({ key, label: INFLIGHT_GROUPS[key] ?? key, rows: here })
    }

    return {
        ok: true,
        baseline: base,
        area,
        all,
        groups,
        total: rows.length,
        vetted: vettedCount,
        unvetted: rows.length - vettedCount,
        withSignals: rows.filter((r) => r.signals.length > 0).length,
        excluded: [...excluded].map(([key, count]) => ({ key, count })),
        numbersOk: numbers.ok === true,
        numbersReason: numbers.ok ? null : (numbers.reason ?? 'the number snapshot did not answer'),
        treeOk: tree.ok === true,
        symbolsOk: symbols.ok === true,
        agesOk: ages.ok === true,
        impacts: INFLIGHT_IMPACT_ORDER,
    }
}

// --- The git half, held apart so `vet` stays pure. -----------------------------------------------

/**
 * Every note on one ref with its content, from the refs - never the working tree.
 *
 * THE BASELINE BY DEFAULT, and `--ref` for the branch a sweep has just merged into: the doc calls
 * this the sweep's progress view, and a sweep's own result is not on the baseline until it lands.
 * Any ref git resolves is accepted; the caller names it in the output so a reader knows which tree
 * answered.
 */
export function baselineNotes({ ref = null } = {}) {
    const base = ref ?? baseline()
    if (!base) return { ok: false, reason: 'no baseline - neither origin/master nor master resolves' }
    if (ref !== null && !exec('git', ['rev-parse', '--verify', '--quiet', `${ref}^{commit}`]).ok) {
        return { ok: false, reason: `'${ref}' does not resolve to a commit` }
    }
    const listed = treeEntries(base, `${NOTES_DIR}/`)
    if (!listed.ok) return { ok: false, reason: `git could not list ${NOTES_DIR} on ${base}` }
    const read = blobContents(listed.entries.map((e) => e.blob))
    if (!read.ok) return { ok: false, reason: 'git could not read the note blobs' }
    const notes = listed.entries
        .map((e) => ({ path: e.path, text: read.contents.get(e.blob) }))
        .filter((n) => n.text !== undefined)
    if (notes.length < listed.entries.length) return { ok: false, reason: 'git listed a note it could not read' }
    return { ok: true, baseline: base, notes }
}

/** Every path on the baseline's tree, one process. */
export function baselineTree(base) {
    const res = exec('git', ['ls-tree', '-r', '-z', '--name-only', base])
    if (!res.ok) return { ok: false, paths: new Set() }
    return { ok: true, paths: new Set(res.out.split('\0').filter(Boolean)) }
}

/**
 * Which of `candidates` the baseline's source contains - two processes, however many candidates.
 *
 * TOKENISED IN-PROCESS, NOT `git grep`. The obvious `git grep -o -h -F -w -e a -e b ...` over ~600
 * candidates took 4.8s on this repository; listing the source blobs and reading them in one
 * `cat-file --batch`, then tokenising once into a Set, took 112ms and answers the same whole-word
 * question. Measured 2026-09-07.
 *
 * SOURCE FILES ONLY, by extension, and never the notes themselves: a symbol mentioned only in a
 * note or a plan is exactly the kind that has gone from the code, and reading the notes as source
 * would find every cited symbol in the note that cites it. An empty candidate list is an answer,
 * not a failure.
 */
export function symbolsPresent(base, candidates) {
    const unique = [...new Set(candidates)]
    if (unique.length === 0) return { ok: true, present: new Set() }
    const listed = exec('git', ['ls-tree', '-r', '-z', base])
    if (!listed.ok) return { ok: false, present: new Set() }
    const blobs = []
    for (const rec of listed.out.split('\0')) {
        const [meta, path] = rec.split('\t')
        if (!path || !SOURCE_RE.test(path)) continue
        blobs.push(meta.split(/\s+/)[2])
    }
    const read = blobContents(blobs)
    if (!read.ok) return { ok: false, present: new Set() }
    const tokens = new Set()
    for (const text of read.contents.values()) for (const m of text.matchAll(/[A-Za-z_][A-Za-z0-9_]{3,}/g)) tokens.add(m[0])
    return { ok: true, present: new Set(unique.filter((c) => tokens.has(c))) }
}

/** What counts as source for the symbol check. Notes, plans and solutions are deliberately not. */
const SOURCE_RE = /\.(java|kt|mjs|js|sh|bash|yml|yaml|xml|properties)$/

/** The candidates `symbolsPresent` needs, gathered over every note in one pass. */
export const symbolCandidates = (notes) => notes.flatMap((n) => anchorsOf(n.text).symbols)
