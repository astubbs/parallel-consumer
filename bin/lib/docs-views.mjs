// Copyright (C) 2026 Antony Stubbs and contributors
//
// VIEWS FOR THE DOCUMENT CONTEXT QUERY - the `docs` command family and the two hooks that deliver
// its answers: the divergence header at both tiers, the frame every injected block wears, the
// document line the prompt hook and `docs for-branch` share, the page `docs show` prints, the
// corpus shape bare `docs` and `docs list` walk, and the session index. Reads what `drift`,
// `docsShape`, `stranded` and `matchDocs` return and produces strings; runs no git, reads no file,
// decides no exit code - the same contract as bin/lib/views.mjs, which holds the rest of the tool's
// renderers and the two helpers (`plural`, `formatWarnings`) this family shares with them.
//
// SPLIT OUT OF views.mjs when the docs family outgrew the file it shared with `formatDrift` and the
// codecov views: every renderer here belongs to one query, and the hooks import this file alone,
// so a hook's firing path loads what it renders and nothing else.

import { INFLIGHT_IMPACT_ORDER } from './inflight-tags.mjs'
import { largestFirst } from './notes.mjs'
import { addedSizeText, formatWarnings, plural } from './views.mjs'

// --- The divergence header. ---------------------------------------------------------------------
//
// `formatDivergenceHeader` is the header every delivery of the context query shows before a
// document - the read-time hook (summary tier, one line), `docs show` and `docs header` (full tier,
// a box). It is named for what it is rather than `formatHeader`, because prior-art.mjs already
// exports a `formatHeader` and bin/inflight.mjs imports it; two exports of one name across two
// libraries is an alias at every call site and a wrong pick waiting to happen.
//
// DIVERGENCE IS THE ONLY CLAIM. Nothing here says a version is "newer" - it says how much the
// version added, and what: headings, else its first added line. Evidence, and the command for more.

const copyStateText = (d) => {
    const a = d.at
    if (!a) return null
    const size = (s) => addedSizeText(s, 'copy')
    switch (a.state) {
        case 'baseline': return `this copy is the baseline's version (${d.baseline})`
        case 'behind': return `this copy is a version ${d.baseline} once held - it is BEHIND, not divergent`
        case 'own-divergent': return `this copy is ${a.ref}'s OWN divergent version${size(a.added)}`
        case 'branch-only': return `this copy is on NO baseline ref - branch-only${size(a.added)}`
        case 'absent': return `${a.ref} does not carry this path`
        default: return `copy state unknown (${a.state})`
    }
}

const scopeText = (d) => `${d.refsTotal} refs searched (${d.liveRefsTotal} live, ${d.archivalRefsTotal} archival)`

/**
 * HOW MANY DIVERGENT VERSIONS THE FULL HEADER DETAILS, largest first - and therefore how many
 * `drift` needs to spend branch facts and a preview on. One constant for both: a header that
 * showed three while the query previewed five paid for two nobody saw, and a query previewing
 * fewer than the header shows would print bare rows.
 */
export const HEADER_TOP = 3

/** The `adds:` line names at most this many added headings; the rest is a count, like every list here. */
const ADDS_SHOWN = 5

/**
 * @param {object} d the result of `drift()` at either tier
 * @param {{tier?: 'summary'|'full', top?: number, warnings?: {id: string, lines: string[]}[], uncommitted?: boolean}} [opts]
 *   `uncommitted` is the hook's finding that the working-tree file differs from the committed blob
 *   the query described (R24); the query cannot know it, so the caller says so and this prints it.
 */
export function formatDivergenceHeader(d, { tier = 'summary', top = HEADER_TOP, warnings = [], uncommitted = false } = {}) {
    if (d.ok === false) return `${d.path}: could not answer - ${d.reason}`
    const rest = `bin/inflight.mjs note drift ${d.path}`
    const edited = uncommitted ? ' - the working-tree file has UNCOMMITTED edits; this describes the committed version' : ''
    if (!d.found) {
        return `${d.path}: at that path on none of ${scopeText(d)}`
            + (d.at?.state === 'branch-only' ? ` - only ${d.at.ref} holds it, uncommitted to any ref` : '')
            + edited
    }
    const liveCarrying = d.divergent.reduce((n, c) => n + c.liveRefs.length, 0)
    const preserved = (d.preserved ?? []).length
    const preservedText = preserved === 0 ? '' : `; ${preserved} preserved (${[...new Set(d.preserved.flatMap((p) => p.kinds))].join(', ')} only)`
    const counts = d.onBaseline
        ? `${plural(d.divergent.length, 'divergent version')} on ${plural(liveCarrying, 'live ref')}`
        : `on NO baseline ref (branch-only); ${plural(d.liveRefsCarrying, 'live ref')} carry it, ${plural(d.divergent.length, 'version')}`

    if (tier === 'summary') {
        return `${d.path}: ${counts}; ${scopeText(d)}; ${copyStateText(d) ?? 'copy state not asked'}${preservedText}${edited}`
    }

    const out = [`=== divergence: ${d.path} ===`]
    const warn = formatWarnings(warnings)
    if (warn) out.push(warn.trimEnd())
    out.push(`  ${counts} carry content ${d.baseline} has NEVER held; ${scopeText(d)}`)
    const state = copyStateText(d)
    if (state) out.push(`  ${state}`)
    if (edited) out.push(`  ${edited.slice(3)}`)
    if (preserved > 0) {
        out.push(`  preserved, not in flight: ${plural(preserved, 'version')} held only by ${[...new Set(d.preserved.flatMap((p) => p.kinds))].join(', ')} refs - `
            + d.preserved.map((p) => p.refs.join(', ')).join('; '))
    }
    const shown = largestFirst(d.divergent).slice(0, top)
    if (shown.length > 0) out.push(`  largest ${shown.length === 1 ? 'version' : `${shown.length} versions`}, by what each added:`)
    for (const c of shown) {
        const sizeText = addedSizeText(c.added, 'header')
        const branches = (c.branches ?? []).filter((b) => c.liveRefs.includes(b.ref))
        const named = branches.slice(0, 3).map((b) => `${b.ref}${b.pr ? ` [astubbs/parallel-consumer#${b.pr.number} ${b.pr.state}]` : ''}`)
        const more = c.liveRefs.length - named.length
        out.push(`    ${sizeText.padEnd(14)} ${named.join(', ')}${more > 0 ? ` and ${plural(more, 'more ref')}` : ''}`)
        if (c.preview) {
            const headings = c.preview.headings
            if (headings.length > 0) {
                const named = headings.slice(0, ADDS_SHOWN).map((h) => `"${h}"`).join(', ')
                const rest = headings.length - ADDS_SHOWN
                out.push(`        adds: ${named}${rest > 0 ? ` and ${rest} more` : ''}`)
            } else if (c.preview.firstLine !== null) out.push(`        adds: "${c.preview.firstLine}" (no heading added)`)
            else out.push('        adds: nothing visible in a line diff')
        }
    }
    if (d.divergent.length > shown.length) out.push(`    ... and ${d.divergent.length - shown.length} more`)
    out.push(`  the rest: ${rest}`)
    return out.join('\n')
}

/**
 * THE FRAME EVERY INJECTED BLOCK WEARS (the plan's KTD9): a fixed source label first, so an agent
 * can tell a fresh signal from a repeat by its first line, and the command that prints more LAST,
 * so it always knows the next thing to run. Four sources, one renderer, so they cannot drift.
 */
const SOURCE_LABELS = {
    header: (path) => `docs context: divergence header for ${path}`,
    terms: (terms) => `docs context: prompt terms ${[].concat(terms).join(', ')}`,
    branch: () => 'docs context: branch facts',
    index: () => 'docs context: session index',
}

export function sourceFrame(kind, subject, body, moreCommand) {
    const toLabel = SOURCE_LABELS[kind]
    // An unknown kind is a programming error in the caller, and printing it is the right failure:
    // a silent fallback label would defeat the one job the label has.
    const label = toLabel ? toLabel(subject) : `docs context: UNKNOWN SOURCE ${kind}`
    return [label, body.trimEnd(), `more: ${moreCommand}`].join('\n')
}

/**
 * ONE DOCUMENT LINE IN AN INJECTED BLOCK. The prompt-terms hook and `docs for-branch` print the
 * same line for the same hit, so an agent learns one shape: `- <title>  <path>  (<marks>)`. The
 * marks are the two facts the query exists for - the path exists off the baseline only, else a
 * live ref carries a version the baseline has never held. On the baseline and undivergent: no mark.
 */
export function formatDocHit(h) {
    const marks = []
    if (!h.onBaseline) marks.push('off baseline')
    else if (h.divergent) marks.push('divergent elsewhere')
    const title = h.title ?? '(no title)'
    return `- ${title}  ${h.path}${marks.length ? `  (${marks.join(', ')})` : ''}`
}

/**
 * THE BODY OF A MATCH BLOCK - the count line, one `formatDocHit` line per shown hit, and the `+N
 * more` tail. The prompt hook and `docs for-branch` render the same `matchDocs` result and once
 * built this line by line each; one renderer, so the two blocks cannot drift apart in wording.
 *
 * @param {object} m the `matchDocs` result
 * @param {object[]} shown the hits to print - the caller's selection, because the hook drops what
 *   the session has already seen and the command drops nothing
 * @param {{label: string, more: number}} opts `label` is the noun after "name" (`this term`,
 *   `these terms`, `them`); `more` is the caller's count of hits not shown, which the hook and the
 *   command compute differently for the reason above
 */
export function formatMatchBody(m, shown, { label, more }) {
    return [
        `${m.hits.length + m.truncated} document(s) across ${m.refsSearched} live ref(s) name ${label}; ${shown.length} shown:`,
        ...shown.map(formatDocHit),
        ...(more > 0 ? [`+${more} more`] : []),
    ].join('\n')
}

/** Terms as shell words for a "more" command an agent will paste: bare when safe, single-quoted otherwise. */
export const termsAsArgv = (terms) => terms
    .map((t) => (/^[A-Za-z0-9_./#-]+$/.test(t) ? t : `'${t.replaceAll("'", "'\\''")}'`))
    .join(' ')

/**
 * THE PAGE `docs show` PRINTS - and, with no body, exactly what `docs header` prints. One renderer
 * for both, so the two commands cannot disagree about the same file: the hook names `docs header`
 * as its "more" command, and a header there that differed from the one above the document would be
 * two answers to one question.
 *
 * THE FIRST LINE NAMES THE REF SHOWN (the plan's KTD11), before the frame. The body that follows
 * reads identically whichever branch it came from, so a page that does not open with "from
 * adds-heading" is read as the file in the working tree - which is the stale-copy incident with a
 * tool badge on it. `ref` null means no live ref carries the path: the page then says which
 * archival refs hold it and how to ask for one, and shows no body, because a tag is where this
 * repository parks work before a re-cut and presenting it as the document is presenting preserved
 * history as live.
 *
 * The "more" command is the next thing an agent would actually run: the largest divergent version
 * this page is not already showing - the one the preview above it put first - else the full drift.
 *
 * @param {object} d the result of `drift()` at the full tier, found
 * @param {{ref: string|null, warnings?: object[], archivalCarriers?: string[], body?: string|null}} opts
 */
export function formatDocsShow(d, { ref, warnings = [], archivalCarriers = [], body = null } = {}) {
    const from = ref === null
        ? `${d.path}: on NO live ref - held only by archival refs ${archivalCarriers.join(', ')}; show one with --ref`
        : `${d.path} from ${ref}` + (ref === d.baseline ? ' - the baseline'
            : d.onBaseline ? ` - NOT the baseline; ${d.baseline} has its own copy`
                : ` - ${d.baseline} does not carry this path`)
    const other = ref === null ? null : largestFirst(d.divergent).find((c) => !c.liveRefs.includes(ref))
    const next = ref === null ? `bin/inflight.mjs docs show ${d.path} --ref ${archivalCarriers[0]}`
        : other ? `bin/inflight.mjs docs show ${d.path} --ref ${other.liveRefs[0]}`
            : `bin/inflight.mjs note drift ${d.path}`
    const out = [from, sourceFrame('header', d.path, formatDivergenceHeader(d, { tier: 'full', warnings }), next)]
    // The separator names path AND ref again, because the page can be long and the body is what
    // gets copied out of it.
    if (body !== null) out.push('', `--- ${d.path} @ ${ref} ---`, body.trimEnd())
    return out.join('\n')
}

// --- The corpus shape - bare `docs`, and the levels `docs list` walks. ---------------------------
//
// EVERY LEVEL PRINTS THE NEXT LEVEL'S COMMANDS (the plan's R14). An agent reaches a document from
// the bare call by copying what it was shown and nothing else: the area row carries its `docs list
// <area>`, each group row carries its `docs list <area> <group>`, and each document carries its
// `docs show <path>`. A count without the command beside it is a fact the reader has to turn into
// a command by hand, which is the step that gets skipped.

const TOOL = 'bin/inflight.mjs'
const offText = (n) => (n > 0 ? `, ${n} only off the baseline` : '')
/** The ref set and the baseline, the way every scope statement in this family spells them. */
const refsText = (shape) => `${shape.refs.total} refs (${shape.refs.live} live, ${shape.refs.archival} archival); baseline ${shape.baseline}.`
const scopeLine = (shape) => `searched ${refsText(shape)} Read from the refs, never the working tree.`

/** One area's heading and its non-empty groups, each with the command that lists it. */
const areaBlock = (area, { allGroups = false } = {}) => {
    const out = [`${area.name}  ${area.dir}/  ${plural(area.documents, 'document')}${offText(area.offBaseline)}`
        + `    ${TOOL} docs list ${area.key}`]
    const groups = area.groups.filter((g) => allGroups || g.documents > 0)
    const width = Math.max(1, ...groups.map((g) => g.key.length))
    for (const g of groups) {
        out.push(`  ${g.key.padEnd(width)}  ${String(g.documents).padStart(4)}${g.offBaseline > 0 ? ` (${g.offBaseline} off)` : '        '}`
            + `    ${TOOL} docs list ${area.key} ${g.key}`)
    }
    if (groups.length === 0) out.push('  (no documents)')
    return out
}

/**
 * @param {object} shape from `docsShape()`
 * @param {{warnings?: object[], failures?: Record<string, {reason: string, time: string}>,
 *          commands?: {path: string, summary: string, when: string}[]}} opts
 *   `commands` are the `docs` subcommands as the front door registers them - summary and `when`
 *   verbatim, so the guide cannot say something help does not.
 */
export function formatDocsShape(shape, { warnings = [], failures = {}, commands = [] } = {}) {
    const out = []
    const warn = formatWarnings(warnings)
    if (warn) out.push(warn.trimEnd(), '')
    out.push(`docs corpus: ${plural(shape.documents, 'document')} across ${shape.areas.length} areas`
        + `${offText(shape.offBaseline)} - on live branches that have not merged, where no working-tree read reaches them.`, '')
    for (const area of shape.areas) out.push(...areaBlock(area), '')

    if (commands.length > 0) {
        const width = Math.max(...commands.map((c) => c.path.length))
        out.push('Commands:')
        for (const c of commands) {
            out.push(`  ${c.path.padEnd(width)}  ${c.summary}`, `  ${' '.repeat(width)}  when: ${c.when}`)
        }
        out.push('')
    }
    out.push(...failureLines(failures))
    out.push(scopeLine(shape))
    return out.join('\n')
}

/**
 * ONE LINE PER RECORDED FAILURE (R26), followed by a blank line when there were any. A delivery
 * that fails open prints nothing to the agent it failed, so this notice is the only place the
 * failure exists outside the cache file - and it is printed by BOTH bare `docs` and the session
 * index, from one helper, so the two cannot word the same record differently.
 */
function failureLines(failures) {
    const out = []
    for (const [delivery, f] of Object.entries(failures)) {
        out.push(`DELIVERY FAILED: ${delivery} - ${f.reason} (${f.time}). It fails open, so nothing else showed this; `
            + 'a later success of the same delivery clears it.')
    }
    if (Object.keys(failures).length > 0) out.push('')
    return out
}

/**
 * One level of the shape: the areas (no area given, or an unknown one), one area's groups, or the
 * documents of one group - the leaf, where each line carries its `docs show` command.
 *
 * An unknown name is a result, not an error: the valid names are the answer, and every one is
 * printed as the command that would have worked.
 */
export function formatDocsList(shape, { area = null, group = null } = {}) {
    const found = area === null ? null : shape.areas.find((a) => a.key === area)
    if (!found) {
        const out = [area === null ? 'give an area to list:' : `no area named '${area}' - the areas are:`]
        for (const a of shape.areas) {
            out.push(`  ${a.key.padEnd(10)} ${a.name}, ${plural(a.documents, 'document')}    ${TOOL} docs list ${a.key}`)
        }
        return [...out, '', scopeLine(shape)].join('\n')
    }
    if (group === null) return [...areaBlock(found), '', scopeLine(shape)].join('\n')

    const g = found.groups.find((x) => x.key === group)
    if (!g) {
        const out = [`no group named '${group}' in ${found.key} - the groups are:`, ...areaBlock(found, { allGroups: true }).slice(1)]
        return [...out, '', scopeLine(shape)].join('\n')
    }
    const out = [`${found.name} / ${g.label}  ${plural(g.documents, 'document')}${offText(g.offBaseline)}`]
    if (g.docs.length === 0) out.push(`  none in ${found.key} ${g.key}`)
    for (const d of g.docs) {
        const type = d.note?.type ? `[${d.note.type}] ` : ''
        // The state is the reason a closed or deferred note sits where it does, and the index
        // prints it for the same reason: a disposition without its why reads as an abandonment.
        const state = d.note?.state && !d.note.open ? `  _${d.note.state}_` : ''
        out.push(`  - ${type}${d.title}  ${d.path}${d.offBaseline ? `  (off baseline - on ${d.ref})` : ''}${state}`)
        out.push(`        ${TOOL} docs show ${d.path}`)
    }
    return [...out, '', scopeLine(shape)].join('\n')
}

// --- The session index - `docs index`, what the session-start hook injects for the three areas. --
//
// THE HEADINGS ARE THE ONES THE BASH HOOK PRINTED, VERBATIM. The index moved here from
// .claude/hooks/inject-recorded-knowledge.sh (the plan's KTD8), and an agent that learned to
// `grep '^## crash'` or `sed -n '/^# Open work/,/^# /p'` over the injected text keeps working only
// if the text it greps for did not move. The equivalence check in bin/test-check-agent-hooks.sh
// runs the pre-migration hook at a pinned commit and asserts every title it listed is listed here;
// these headings are its positive control.
//
// CORPUS-SCOPED, WHICH IS WHAT THE MOVE BOUGHT (R17). The hook read the working tree, so it listed
// what the current branch carried and apologised for the rest with a count. This renders
// `docsShape`, which reads the refs: on-baseline documents from the baseline's blob, off-baseline
// ones from the first live ref carrying them. The on-baseline part keeps the hook's grouping; the
// off-baseline part is new, and it is GROUPED BY THE BRANCH SET CARRYING IT (R18), as `stranded`
// clusters it, because a workstream's forty notes on one branch are one fact, not forty lines.
//
// THE CAP IS ON THE NEW PART ONLY. The on-baseline listing is exactly as long as the hook's was and
// is never cut - the failure the hook exists to fix is not knowing a document EXISTS, and a cap on
// the part every session already paid for would reintroduce it. The off-baseline groups are taken
// largest first until `maxLines` is spent; the rest of each area collapses to a count and the
// command that lists them, so the omission is visible and costs one line.

const INDEX_TOOL_MORE = `${TOOL} docs`

/** The hook's area order, which is not `DOC_AREAS`'s: solved first, then work, then the plans. */
const INDEX_AREA_ORDER = ['solutions', 'inflight', 'plans']

/**
 * A cluster's branch names: local and remote-tracking copies of one branch are one name, and the
 * `backup/` branches this repository pushes before a force-push sort LAST - they are live refs
 * (refs/heads, not refs/backup), so they belong in the set, but a label that opens with three of
 * them hides the branch a reader could actually go and continue.
 */
const branchNames = (liveRefs) => [...new Set(liveRefs.map((r) => r.replace(/^origin\//, '')))]
    .sort((a, b) => Number(a.startsWith('backup/')) - Number(b.startsWith('backup/')) || a.localeCompare(b))
const branchSetLabel = (names) => (names.length > 3
    ? `${names.slice(0, 3).join(', ')} and ${names.length - 3} more`
    : names.join(', '))

const planStem = (path) => path.replace(/^docs\/plans\//, '').replace(/\.(md|html)$/, '')
const stemsLine = (docs) => docs.map((d) => planStem(d.path)).join(', ')

/** The disposition a note line carries after its title: the impact, or the state that closed it. */
const noteTail = (d) => {
    if (!d.note) return ''
    if (d.note.state && !d.note.open) return `  _${d.note.state}_`
    return d.note.impact ? `  _${d.note.impact}_` : ''
}

/** One document as a line of the index, in the shape the hook gave that area's lines. */
const INDEX_LINE = {
    solutions: (d) => `- ${d.title}  \`${d.path}\``,
    inflight: (d) => `- [${d.note?.type || 'untyped'}] ${d.title}${noteTail(d)}`,
    plans: (d) => `- ${planStem(d.path)}`,
}

/** The on-baseline half of one area, as the hook rendered it. */
const ON_BASELINE = {
    solutions: (area, docs) => {
        const out = []
        for (const g of area.groups) {
            const mine = docs.filter((d) => g.docs.includes(d))
            if (mine.length === 0) continue
            out.push(`## ${g.key}`, ...mine.map(INDEX_LINE.solutions), '')
        }
        return out
    },
    inflight: (area, docs) => {
        const inGroup = (key) => area.groups.find((g) => g.key === key).docs.filter((d) => docs.includes(d))
        const out = [
            '# Registers - standing documents, consult before choosing work', '',
            'Consulted, never completed. Read these before picking up anything below.', '',
            // Path as well as title: a register is something you go and OPEN.
            ...inGroup('registers').map((d) => `- ${d.title}  \`${d.path}\``), '',
            '# Open work - what it costs you to not know', '',
            'One file per item under `docs/inflight/`, grouped by impact across every type.', '',
        ]
        for (const g of area.groups) {
            if (!INFLIGHT_IMPACT_ORDER.includes(g.key)) continue
            const mine = inGroup(g.key)
            if (mine.length === 0) continue
            out.push(`## ${g.key}`, ...mine.map((d) => `- [${d.note.type}] ${d.title}`), '')
        }
        const feature = inGroup('feature')
        if (feature.length > 0) out.push('## feature - proposed, no consequence attached', ...feature.map((d) => `- [${d.note.type}] ${d.title}`), '')
        // Listed by name rather than counted: an unmatched note is a bug in its tags and the fix
        // needs to know which file.
        const unmatched = inGroup('unmatched')
        if (unmatched.length > 0) {
            out.push('## unmatched - no group claimed them', '', 'Their `inflight-type` or `inflight-impact` is missing or misspelt:',
                ...unmatched.map((d) => `- ${d.title}  \`${d.path}\``), '')
        }
        const closed = inGroup('closed')
        if (closed.length > 0) {
            out.push('# Not shown above - closed or blocked', '',
                'Listed rather than counted: a number cannot tell you a note fell here by accident. Delete or migrate them.', '',
                ...closed.map((d) => `- ${d.title}  _${d.note.state}_  \`${d.path}\``), '')
        }
        const deferred = inGroup('deferred')
        if (deferred.length > 0) {
            out.push('# Deferred - decided, not now', '',
                'All non-deferred work happens first. Running out of open work above is the trigger to re-read this.', '',
                ...deferred.map((d) => `- [${d.note.impact || 'no impact'}] ${d.title}  _${d.note.state}_`), '')
        }
        return out
    },
    plans: (area, docs) => {
        const out = ['# Dated plans and investigations', '', '`docs/plans/` - the method that settled a question of this shape before:']
        let any = false
        for (const g of area.groups) {
            const mine = docs.filter((d) => g.docs.includes(d))
            if (mine.length === 0) continue
            any = true
            out.push('', `## ${g.key}`, stemsLine(mine))
        }
        if (!any) out.push('(none)')
        out.push('')
        return out
    },
}

const OFF_BASELINE_HEADING = {
    solutions: '# Solved only on branches - grouped by the branch set carrying them, largest first',
    inflight: '# In flight only on branches - grouped by the branch set carrying them, largest first',
    plans: '# Plans only on branches - grouped by the branch set carrying them, largest first',
}

/**
 * The off-baseline documents of one area as branch-set groups: `{names, docs}`, largest first.
 * Two `stranded` clusters whose ref sets differ only by a remote-tracking copy name the same
 * branches, and are one group here.
 */
function branchSetGroups(docs, clusters, currentBranch = null) {
    const setOf = new Map() // path -> branch-set key
    const namesOf = new Map()
    for (const c of clusters) {
        if (c.preserved) continue
        const names = branchNames(c.liveRefs)
        const key = names.join(' ')
        namesOf.set(key, names)
        for (const p of c.paths) setOf.set(p, key)
    }
    const groups = new Map()
    for (const d of docs) {
        const key = setOf.get(d.path)
        if (key === undefined) continue // a document `docsShape` read from a ref no live cluster names: nothing to group it under
        if (!groups.has(key)) groups.set(key, { names: namesOf.get(key), docs: [] })
        groups.get(key).docs.push(d)
    }
    // THE CHECKED-OUT BRANCH'S OWN GROUP FIRST, and marked, whatever its size. The working-tree
    // scan this replaced always listed the current branch's notes; a corpus-scoped index that
    // dropped them under the cap would list every other workstream's notes and not yours.
    for (const g of groups.values()) g.pinned = currentBranch !== null && g.names.includes(currentBranch)
    return [...groups.values()].sort((a, b) => Number(b.pinned) - Number(a.pinned)
        || b.docs.length - a.docs.length || a.names.join(' ').localeCompare(b.names.join(' ')))
}

/**
 * @param {object} shape from `docsShape()`
 * @param {{clusters: object[], maxLines?: number, currentBranch?: string|null, warnings?: object[],
 *          failures?: Record<string, {reason: string, time: string}>}} opts
 *   `clusters` from `stranded()` over the same index the shape was built on - the branch sets.
 *   `maxLines` bounds the off-baseline groups across the whole index, never the on-baseline listing.
 *   `currentBranch` pins the group carrying the checked-out branch's own documents ahead of the cap.
 */
export function formatDocsIndex(shape, { clusters, maxLines = 400, currentBranch = null, warnings = [], failures = {} } = {}) {
    const out = []
    const warn = formatWarnings(warnings)
    if (warn) out.push(warn.trimEnd(), '')
    out.push(...failureLines(failures))
    out.push(`Corpus-scoped: ${plural(shape.documents, 'document')} across ${shape.areas.map((a) => `${a.dir}/`).join(', ')} on `
        + `${refsText(shape)} `
        + `${shape.offBaseline} exist only on live branches that have not merged - no working-tree read reaches them, `
        + 'and they are listed under the branches carrying them. Titles are read from the refs, never the working tree: '
        + 'a document this checkout has edited is shown as the baseline holds it. What this cannot show is a version '
        + `preserved only in an archival ref (a tag, refs/backup) - \`${TOOL} stranded\` names those.`, '')

    // EACH AREA GETS AN EQUAL SHARE OF THE CAP, and what it does not spend rolls to the next. One
    // shared budget in area order let the in-flight area, which holds most of the off-baseline
    // corpus, spend the whole cap and collapse every branch-only plan to one count line - the
    // smallest area paying for the largest.
    const areas = INDEX_AREA_ORDER.map((k) => shape.areas.find((a) => a.key === k)).filter(Boolean)
    const share = Math.floor(maxLines / Math.max(1, areas.length))
    let carry = maxLines - share * areas.length
    for (const area of areas) {
        const allDocs = area.groups.flatMap((g) => g.docs)
        out.push(...ON_BASELINE[area.key](area, allDocs.filter((d) => !d.offBaseline)))

        let budget = share + carry
        carry = 0
        const groups = branchSetGroups(allDocs.filter((d) => d.offBaseline), clusters, currentBranch)
        if (groups.length === 0) { carry = budget; continue }
        out.push(OFF_BASELINE_HEADING[area.key], '')
        let omitted = 0
        let omittedDocs = 0
        for (const g of groups) {
            const heading = `## only on ${branchSetLabel(g.names)}${g.pinned ? ' - YOUR BRANCH' : ''}`
            const lines = area.key === 'plans'
                ? [heading, stemsLine(g.docs), '']
                : [heading, ...g.docs.map(INDEX_LINE[area.key]), '']
            // A group that does not fit is omitted with everything after it in this area: the
            // groups are largest first, so the cap lands on the smallest and the tail stays a tail.
            // The pinned group is never the one omitted, and it spends the budget it uses.
            if (!g.pinned && (omitted > 0 || lines.length > budget)) {
                omitted++
                omittedDocs += g.docs.length
                continue
            }
            budget = Math.max(0, budget - lines.length)
            out.push(...lines)
        }
        if (omitted > 0) {
            out.push(`... ${plural(omitted, 'more branch set')} holding ${plural(omittedDocs, 'document')}, past the ${maxLines}-line cap `
                + `(\`docs index --max-lines <n>\` raises it): ${TOOL} docs list ${area.key}`, '')
        }
        carry = budget
    }
    return sourceFrame('index', null, out.join('\n'), INDEX_TOOL_MORE)
}
