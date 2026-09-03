// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE SHAPE OF THE DOCS CORPUS - every area, its groups, and the documents in each, read from the
// refs rather than the working tree. This is what bare `inflight docs` prints and what `docs list`
// walks one level at a time (the plan's R13, R14, R16).
//
// GROUPING FOLLOWS THE SESSION INDEX, not a fresh taxonomy: solutions by their category directory,
// in-flight notes by the cost-of-not-knowing order the index already presents them in (registers
// first, then open work by impact, then features with no consequence attached, then whatever no
// group claimed, then closed, then deferred last), plans by date, newest first. An agent that has
// read the index at session start recognises the same groups here, and a group it can name is a
// group it can ask for.
//
// WHICH DOCUMENTS COUNT. The corpus is what the baseline carries plus what exists ONLY off it on a
// live branch - `stranded`'s clusters with a live carrier. A path that is off the baseline because
// the baseline once held it and `git rm`d it is a closed item, and a path whose blob reached the
// baseline under another name is a rename; both are `stranded`'s filters, and counting them as
// documents would count finished work as knowledge at risk. Archival-only copies (tags, refs/backup)
// are preserved, not in flight, and are not documents here either.
//
// ONE `cat-file --batch` FOR EVERYTHING (the plan's KTD16). A note's group is in its markers, so the
// notes need their CONTENT, not just a heading - and one batch over every blob the shape needs is
// the same cost as the title-only batch would have been. On-baseline documents are read from the
// BASELINE's blob, off-baseline ones from the copy the first sorted live carrier holds (the same
// choice `docs show` makes), so a shape built on a checkout behind the baseline is not wrong.
//
// No git except that one batch, no printing, no process.exit: findings only.

import { blobContents } from './git.mjs'
import { NOTES_DIR } from './repo.mjs'
import { INFLIGHT_IMPACT_ORDER, classifyNote, titleOf } from './inflight-tags.mjs'

/** A directory's own rules file is not one of the documents an area holds - the index's guard, ported. */
export const DIRECTORY_DOCS_RE = /\/(AGENTS|CLAUDE|README)\./
/** Both extensions, because the plan contract allows an artifact to be `.html`. */
export const DOCUMENT_RE = /\.(md|html)$/

/** The word an agent types for an area: the directory's last segment - `inflight`, `solutions`, `plans`. */
const areaKey = (dir) => dir.split('/').pop()

/** The in-flight groups that are not impacts, in the index's order around the impact list. */
/**
 * The label for each non-impact group. EXPORTED because `rank` renders the same groups: a second
 * copy of this taxonomy had already drifted in four of its five entries before it was noticed.
 */
export const INFLIGHT_GROUPS = {
    registers: 'registers - standing documents, consult before choosing work',
    feature: 'feature - proposed, no consequence attached',
    unmatched: 'unmatched - no group claimed them: inflight-type or inflight-impact missing or misspelt',
    closed: 'closed or blocked - carries a state that is neither deferred nor parked; delete or migrate',
    deferred: 'deferred - decided, not now; all non-deferred work happens first',
}

/** Every in-flight group key in presentation order. */
const INFLIGHT_GROUP_ORDER = ['registers', ...INFLIGHT_IMPACT_ORDER, 'feature', 'unmatched', 'closed', 'deferred']

const impactRank = (impact) => {
    const i = INFLIGHT_IMPACT_ORDER.indexOf(impact)
    // No impact sorts after every known one, matching the index's "registers with no impact come last".
    return i < 0 ? INFLIGHT_IMPACT_ORDER.length : i
}

/**
 * Which in-flight group a note belongs to - the session index's rules, in the order it applies
 * them. A register is grouped as a register whatever its impact; a note with a state is closed
 * unless the state says deferred or parked; an open note goes under its impact, or under `feature`
 * when it is a feature with none, or is reported as unmatched so a typo'd tag is a finding rather
 * than a silent absence.
 */
export function inflightGroupOf(note) {
    if (note.deferred) return 'deferred'
    if (!note.open) return 'closed'
    if (note.type === 'register') return INFLIGHT_IMPACT_ORDER.includes(note.impact) || note.impact === '' ? 'registers' : 'unmatched'
    if (INFLIGHT_IMPACT_ORDER.includes(note.impact)) return note.impact
    if (note.type === 'feature' && note.impact === '') return 'feature'
    return 'unmatched'
}

const groupInflight = (docs) => {
    const buckets = new Map(INFLIGHT_GROUP_ORDER.map((k) => [k, []]))
    for (const d of docs) buckets.get(inflightGroupOf(d.note)).push(d)
    // Registers and deferred notes are ordered by the same impact scale as open work, so one
    // ordering principle governs the whole shape; within a rank, by path, as the index sorts.
    const byImpactThenPath = (a, b) => impactRank(a.note.impact) - impactRank(b.note.impact) || a.path.localeCompare(b.path)
    const byPath = (a, b) => a.path.localeCompare(b.path)
    return INFLIGHT_GROUP_ORDER.map((key) => ({
        key,
        label: INFLIGHT_GROUPS[key] ?? key,
        docs: buckets.get(key).sort(key === 'registers' || key === 'deferred' ? byImpactThenPath : byPath),
    }))
}

/** Solutions: the category directory, alphabetically; a file with no category is `uncategorised`. */
const groupSolutions = (docs, dir) => {
    const buckets = new Map()
    for (const d of docs) {
        const rest = d.path.slice(dir.length + 1).split('/')
        const key = rest.length > 1 ? rest[0] : 'uncategorised'
        if (!buckets.has(key)) buckets.set(key, [])
        buckets.get(key).push(d)
    }
    return [...buckets.keys()].sort().map((key) => ({ key, label: key, docs: buckets.get(key).sort((a, b) => a.path.localeCompare(b.path)) }))
}

/** Plans: the year-month of the filename's leading date, newest first; undated ones last. */
const groupPlans = (docs, dir) => {
    const buckets = new Map()
    for (const d of docs) {
        const name = d.path.slice(dir.length + 1)
        const m = /^(\d{4}-\d{2})-\d{2}/.exec(name)
        const key = m ? m[1] : 'undated'
        if (!buckets.has(key)) buckets.set(key, [])
        buckets.get(key).push(d)
    }
    const keys = [...buckets.keys()].filter((k) => k !== 'undated').sort().reverse()
    if (buckets.has('undated')) keys.push('undated')
    return keys.map((key) => ({ key, label: key, docs: buckets.get(key).sort((a, b) => b.path.localeCompare(a.path)) }))
}

/** An area this file has no rule for groups by its first subdirectory - never by nothing. */
const GROUPERS = { inflight: groupInflight, solutions: groupSolutions, plans: groupPlans }

/**
 * @param {{index: object, stranded: object[], areas?: {dir: string, name: string}[]}} opts
 *   `index` from `corpusIndex()` over the areas; `stranded` from `stranded(index)` over the same.
 * @returns {{ok: true, baseline: string, refs: {total: number, live: number, archival: number},
 *            documents: number, offBaseline: number,
 *            areas: {key: string, dir: string, name: string, documents: number, offBaseline: number,
 *                    groups: {key: string, label: string, documents: number, offBaseline: number, docs: object[]}[]}[]}
 *          | {ok: false, reason: string}}
 *   each doc is `{path, title, offBaseline, ref, note?}` - `ref` names the ref its content was read
 *   from, and `note` is the classification for documents under the notes area.
 */
export function docsShape({ index, stranded, areas = index.areas }) {
    // path -> the first sorted live ref carrying an unlanded version: the copy the shape reads.
    const offBaselineRef = new Map()
    for (const cluster of stranded) {
        if (cluster.preserved) continue
        const ref = [...cluster.liveRefs].sort()[0]
        for (const p of cluster.paths) offBaselineRef.set(p, ref)
    }

    const wanted = [] // {path, ref, blob, offBaseline}
    for (const [path, versions] of index.byPath) {
        if (!DOCUMENT_RE.test(path) || DIRECTORY_DOCS_RE.test(path)) continue
        const onBaseline = index.basePaths.has(path)
        const ref = onBaseline ? index.baseline : offBaselineRef.get(path)
        if (!ref) continue // closed, renamed, or preserved only - not a document of the corpus
        let blob = null
        for (const [b, refs] of versions) if (refs.includes(ref)) { blob = b; break }
        if (blob === null) continue // the cluster named a ref this path is not on: nothing to read
        wanted.push({ path, ref, blob, offBaseline: !onBaseline })
    }

    const batch = blobContents(wanted.map((w) => w.blob))
    // A failed batch cannot classify a single note, and a shape with every note "unmatched" would
    // be a confident wrong answer - so this is could-not-run, not an empty corpus.
    if (!batch.ok) return { ok: false, reason: 'cannot read the documents - `git cat-file --batch` failed' }

    const perArea = new Map(areas.map((a) => [a.dir, []]))
    for (const w of wanted) {
        const area = areas.find((a) => w.path.startsWith(`${a.dir}/`))
        if (!area) continue
        const text = batch.contents.get(w.blob) ?? ''
        const doc = { path: w.path, ref: w.ref, offBaseline: w.offBaseline, title: titleOf(text, w.path) }
        if (area.dir === NOTES_DIR) doc.note = classifyNote(text, w.path)
        perArea.get(area.dir).push(doc)
    }

    const count = (docs) => ({ documents: docs.length, offBaseline: docs.filter((d) => d.offBaseline).length })
    const shaped = areas.map((a) => {
        const key = areaKey(a.dir)
        const docs = perArea.get(a.dir)
        const grouper = GROUPERS[key] ?? groupSolutions
        const groups = grouper(docs, a.dir).map((g) => ({ ...g, ...count(g.docs) }))
        return { key, dir: a.dir, name: a.name, ...count(docs), groups }
    })
    const all = shaped.reduce((n, a) => n + a.documents, 0)
    const off = shaped.reduce((n, a) => n + a.offBaseline, 0)
    return {
        ok: true,
        baseline: index.baseline,
        refs: {
            total: index.refs.length,
            live: index.refs.filter((r) => !r.archival).length,
            archival: index.refs.filter((r) => r.archival).length,
        },
        documents: all,
        offBaseline: off,
        areas: shaped,
    }
}
