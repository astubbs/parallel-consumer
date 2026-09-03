// Copyright (C) 2026 Antony Stubbs and contributors
//
// VIEWS. Reads what the query libraries return and produces strings; runs no git and decides no exit
// code. Kept apart from the queries so a caller that wants the findings - a hook building a session
// index, a gate deciding whether to fail - never pays to render them.
//
// `formatWarnings` is here rather than beside either caller because bin/lib/git.mjs's
// `freshnessWarnings` has two consumers and they rendered it identically, down to the eleven-space
// continuation indent and the one id that prints NOTE instead of WARNING. That was the second copy
// of a shared primitive appearing on the same branch that wrote the rule against it.

import { INFLIGHT_IMPACT_ORDER } from './inflight-tags.mjs'

const plural = (n, w) => `${n} ${w}${n === 1 ? '' : 's'}`

export function formatWarnings(warnings) {
    if (!warnings.length) return ''
    return `${warnings.map((w) => `  ${w.id === 'head-behind' ? 'NOTE' : 'WARNING'}: ${w.lines.join('\n           ')}`).join('\n')}\n`
}

export function formatFind(hits, query, index) {
    if (hits.length === 0) {
        return `no in-flight note matching /${query}/ on any of ${plural(index.refs.length, 'ref')}.\n`
            + 'Nothing is a result here: this searched every branch tip, not the working tree.'
    }
    const out = [`${plural(hits.length, 'note')} matching /${query}/, across ${plural(index.refs.length, 'ref')}:\n`]
    for (const h of hits) {
        out.push(`  ${h.path}`)
        out.push(h.onBaseline
            ? `      on ${index.baseline}, ${plural(h.versionCount, 'version')} across ${plural(h.refCount, 'ref')}`
            : `      NOT ON ${index.baseline} - ${plural(h.versionCount, 'version')} across ${plural(h.refCount, 'ref')}`)
    }
    out.push('\nNext: bin/inflight.mjs note drift <path>')
    return out.join('\n')
}

export function formatDrift(d) {
    if (d.ok === false) return `prior-art: ${d.reason}`
    if (!d.found) return `no in-flight note at that path on any ref.\nTry: bin/inflight.mjs note find <fuzzy>`

    const cluster = (c, label) => {
        const out = [`  ${c.blob.slice(0, 9)}  ${plural(c.refs.length, 'ref')}  ${label}`]
        if (c.title) out.push(`      "${c.title}"`)
        for (const b of c.branches) {
            const pr = b.pr ? `  [astubbs/parallel-consumer#${b.pr.number} ${b.pr.state}]` : ''
            out.push(`      ${b.ref}${pr}`)
            if (b.themeFrom !== 'branch-name') out.push(`          ${b.theme}   (${b.themeFrom})`)
        }
        if (c.refs.length > c.branches.length) out.push(`      ... and ${c.refs.length - c.branches.length} more`)
        out.push('')
        return out.join('\n')
    }

    const out = [
        `${d.path}`,
        `  ${plural(d.refsCarrying, 'ref')} carry it, of ${d.refsTotal} searched; `
            + `${d.onBaseline ? `present on ${d.baseline}` : `ABSENT from ${d.baseline}`}\n`,
    ]

    if (d.divergent.length === 0) {
        out.push('  NOTHING DIVERGENT. Every other version is one this baseline has already held, so no')
        out.push('  branch is carrying content that would be lost.\n')
    } else {
        out.push(`  ${plural(d.divergent.length, 'version')} carry content ${d.baseline} has NEVER had `
            + `- on ${plural(d.divergent.reduce((n, c) => n + c.refs.length, 0), 'ref')}.`)
        out.push('  Sizes are against each branch\'s MERGE-BASE, so they say what the branch added, not how far')
        out.push(`  ${d.baseline} has moved since.\n`)
        for (const c of d.divergent) {
            const a = c.added
            out.push(cluster(c, !a ? 'differs (no merge-base version to compare against)'
                : a.diffFailed ? 'size UNKNOWN - the diff command failed, which is not "no change"'
                    : a.newFile ? 'added on this branch, after it diverged'
                        : `+${a.added} -${a.removed} since its merge-base`))
        }
    }

    // A version held ONLY by a tag or a refs/backup ref was listed above as if it were a branch,
    // with a merge-base size and a "branch name" nobody can check out. It is preserved on purpose,
    // so it gets its own line - present, because the corpus looks everywhere, and labelled.
    if ((d.preserved ?? []).length > 0) {
        out.push(`  Preserved, not in flight: ${plural(d.preserved.length, 'version')} held only by archival refs `
            + `(${[...new Set(d.preserved.flatMap((p) => p.kinds))].join(', ')}):`)
        for (const p of d.preserved) out.push(`      ${p.blob.slice(0, 9)}  ${p.refs.join(', ')}`)
        out.push('')
    }
    if (d.behind.versions > 0) {
        out.push(`  Not shown: ${plural(d.behind.versions, 'version')} on ${plural(d.behind.refs, 'ref')} `
            + `that ${d.baseline} itself once held - those branches are simply behind, which is`)
        out.push('  not drift and gets worse on its own. Pass --all to see them.')
    }
    return out.join('\n')
}

// --- The document context query, rendered. ------------------------------------------------------
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
    const size = (s) => (!s ? '' : s.diffFailed ? ' (size unknown - the diff failed)'
        : s.newFile ? ' (created on this branch)' : ` (+${s.added} -${s.removed} since its merge-base)`)
    switch (a.state) {
        case 'baseline': return `this copy is the baseline's version (${d.baseline})`
        case 'behind': return `this copy is a version ${d.baseline} once held - it is BEHIND, not divergent`
        case 'own-divergent': return `this copy is ${a.ref}'s OWN divergent version${size(a.added)}`
        case 'branch-only': return `this copy is on NO baseline ref - branch-only${size(a.added)}`
        case 'absent': return `${a.ref} does not carry this path`
        default: return `copy state unknown (${a.state})`
    }
}

/**
 * LARGEST FIRST, by what the version ADDED - the evidence of knowledge, not of recency. A version
 * whose size is unknown sorts last rather than being given a fabricated position. One function,
 * because the header's preview and the "more" command under it must agree on which version is
 * the one to look at next: `drift` returns clusters most-carried first, and a suggestion built on
 * that order pointed at a stale integration branch's copy under a preview naming a different one.
 */
const largestFirst = (divergent) => {
    const size = (c) => (c.added && Number.isInteger(c.added.added) ? c.added.added : -1)
    return [...divergent].sort((a, b) => size(b) - size(a) || b.liveRefs.length - a.liveRefs.length)
}

const scopeText = (d) => `${d.refsTotal} refs searched (${d.liveRefsTotal} live, ${d.archivalRefsTotal} archival)`

/**
 * @param {object} d the result of `drift()` at either tier
 * @param {{tier?: 'summary'|'full', top?: number, warnings?: {id: string, lines: string[]}[], uncommitted?: boolean}} [opts]
 *   `uncommitted` is the hook's finding that the working-tree file differs from the committed blob
 *   the query described (R24); the query cannot know it, so the caller says so and this prints it.
 */
export function formatDivergenceHeader(d, { tier = 'summary', top = 3, warnings = [], uncommitted = false } = {}) {
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
        const a = c.added
        const sizeText = !a ? 'size unknown (no merge-base)' : a.diffFailed ? 'size unknown (diff failed)'
            : a.newFile ? 'created after diverging' : `+${a.added} -${a.removed}`
        const branches = (c.branches ?? []).filter((b) => c.liveRefs.includes(b.ref))
        const named = branches.slice(0, 3).map((b) => `${b.ref}${b.pr ? ` [astubbs/parallel-consumer#${b.pr.number} ${b.pr.state}]` : ''}`)
        const more = c.liveRefs.length - named.length
        out.push(`    ${sizeText.padEnd(14)} ${named.join(', ')}${more > 0 ? ` and ${plural(more, 'more ref')}` : ''}`)
        if (c.preview) {
            if (c.preview.headings.length > 0) out.push(`        adds: ${c.preview.headings.map((h) => `"${h}"`).join(', ')}`)
            else if (c.preview.firstLine !== null) out.push(`        adds: "${c.preview.firstLine}" (no heading added)`)
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
const scopeLine = (shape) => `searched ${shape.refs.total} refs (${shape.refs.live} live, ${shape.refs.archival} archival); `
    + `baseline ${shape.baseline}. Read from the refs, never the working tree.`

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
        + `${shape.refs.total} refs (${shape.refs.live} live, ${shape.refs.archival} archival); baseline ${shape.baseline}. `
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

export function formatStranded(clusters, index) {
    if (clusters.length === 0) return `nothing stranded: every note on every ref has reached ${index.baseline}.`
    const paths = clusters.reduce((n, c) => n + c.paths.length, 0)
    const out = [
        `${plural(paths, 'in-flight note')} exist on a branch and have NEVER reached ${index.baseline},`,
        `grouped into ${plural(clusters.length, 'cluster')} by the set of refs carrying them.`,
        '',
        'Filtered: notes on the baseline now; notes whose blob lives there under another name; and',
        "notes the baseline's history once had - those landed and were `git rm`d when their work closed.",
        '',
    ]
    const preserved = clusters.filter((c) => c.preserved).length
    if (preserved > 0) {
        out.push(`${preserved === 1 ? 'One cluster sits' : `${preserved} clusters sit`} ONLY in an archive - a tag or a \`refs/backup\` ref,`,
            'which is where this repository parks work before a re-cut. Preserved, not lost; marked below.', '')
    }
    for (const c of clusters) {
        // The example ref is a LIVE one where the cluster has any, because an archival ref name is
        // not somewhere a reader can go and continue the work.
        const eg = (c.liveRefs?.length ? c.liveRefs[0] : c.refs[0])
        out.push(`  ${plural(c.paths.length, 'note')} on ${plural(c.refCount, 'ref')} - e.g. ${eg}`
            + (c.preserved ? '   [ARCHIVE ONLY - preserved, not stranded]' : ''))
        for (const p of c.paths.slice(0, 5)) out.push(`      ${p}`)
        if (c.paths.length > 5) out.push(`      ... and ${c.paths.length - 5} more`)
        out.push('')
    }
    return out.join('\n')
}

export function formatBranch(v, gap) {
    if (!v.ok) return `${v.reason}\nTry: git for-each-ref --format='%(refname:short)'`

    const row = (k, val) => `  ${k.padEnd(12)}${val}`
    const out = [v.ref, '']

    out.push(row('PR', v.pr
        ? `astubbs/parallel-consumer#${v.pr.number} ${v.pr.state}  ${v.pr.title}`
        : 'none'))
    out.push(row('pushed', v.isRemote ? '(this IS the remote ref)'
        : v.upstream ? v.upstream
            : 'NOWHERE - this branch exists only on this disk'))
    // Two records, two questions - saying which is which is the point.
    out.push(row('session', v.session ? `${v.session}   (produced it; from a Claude-Session commit trailer)` : 'unknown'))
    if (v.holder) out.push(row('holding', `${v.holder}   (right now; from .worktree-owner, this machine only)`))
    out.push(row('commits', v.containedInBaseline
        ? `none the baseline lacks - fully contained in ${v.baseline}, safe to delete`
        : `${v.commitsOffBaseline} the baseline does not have`))
    out.push(row('notes', v.notesOnly.length === 0
        ? `none the baseline lacks`
        : `${v.notesOnly.length} that ${v.baseline} has never had`))
    for (const p of v.notesOnly.slice(0, 5)) out.push(`                  ${p}`)
    if (v.notesOnly.length > 5) out.push(`                  ... and ${v.notesOnly.length - 5} more`)

    // Each related branch shows its own PR, and says "no PR" rather than leaving a gap - absent and
    // unknown are different facts, and a blank column reads as neither.
    const rel = (r) => `                  ${r.ref.padEnd(52)} ${r.pr
        ? `astubbs/parallel-consumer#${r.pr.number} ${r.pr.state}`
        : 'no PR'}`
    if (v.parents.length) {
        out.push('', row('integrates', `${v.parents.length} branch(es) it fully contains`))
        for (const p of v.parents.slice(0, 10)) out.push(rel(p))
        if (v.parents.length > 10) out.push(`                  ... and ${v.parents.length - 10} more`)
    }
    if (v.children.length) {
        out.push('', row('absorbed by', `${v.children.length} branch(es) that contain it`))
        for (const c of v.children.slice(0, 10)) out.push(rel(c))
    }

    out.push('')
    if (v.mentions.length) out.push(row('tracked', `named in ${v.mentions.join(', ')}`))
    for (const e of v.explainedBy) {
        out.push(row('tracked', `astubbs/parallel-consumer#${e.pr.number} ${e.how}`))
    }
    if (gap) {
        out.push(row('TRACKED', 'NOWHERE - no PR, no branch note, named in no note on the baseline'))
        out.push(row('FIX', gap.remedy))
    } else if (!v.mentions.length && !v.explainedBy.length && v.pr) {
        out.push(row('tracked', `by its PR`))
    }
    return out.join('\n')
}

export function formatCache(status, known) {
    if (!status.exists) return `no cache directory yet at ${status.dir} - nothing has been cached.`
    const age = (ms) => (ms === null ? '-' : ms < 60000 ? `${Math.round(ms / 1000)}s`
        : ms < 3600000 ? `${Math.round(ms / 60000)}m` : `${Math.round(ms / 3600000)}h`)
    const size = (b) => (b < 1024 ? `${b}B` : b < 1048576 ? `${Math.round(b / 1024)}K` : `${(b / 1048576).toFixed(1)}M`)

    const out = [status.dir, '']
    const width = Math.max(...status.entries.map((e) => e.name.length), 12)
    for (const e of status.entries) {
        out.push(`  ${e.name.padEnd(width)}  ${size(e.bytes).padStart(7)}  ${age(e.ageMs).padStart(5)} old`
            + (e.orphan ? '   ORPHAN - nothing reads this any more' : ''))
    }
    const orphans = status.entries.filter((e) => e.orphan)
    out.push('')
    out.push(`  live: ${known.join(', ')}`)
    if (orphans.length) {
        out.push(`  ${orphans.length} orphan(s) holding ${size(orphans.reduce((t, e) => t + e.bytes, 0))}`
            + ` - clear with: bin/inflight.mjs cache clear`)
    }
    // Freshness is read from inside each file, because an mtime can be rewritten by anything that
    // touches it - and a cache that LOOKS fresh is worse than one that admits it is old.
    return out.join('\n')
}

/**
 * The refactor-window report.
 *
 * ONE SILENCE IS ALLOWED AND IT HAS TO BE EARNED. Under `--if-open` this returns the empty string
 * when the signal RAN and found no candidate open - and never when something failed. A hook's
 * correct silence is byte-identical to a broken hook, which `.claude/hooks/inject-branch-context.sh`
 * records as its own hard-won rule ("DEGRADED READS ARE LOUD, NEVER SHORT"), so every failure path
 * below prints. The caller turns a failed run into exit 2; this only decides what is worth saying.
 *
 * A FAILED CANDIDATE STILL LETS ITS PEERS REPORT. That is why the per-candidate `ok` exists, and
 * printing it inline rather than aborting is the whole point: one bad path must not produce the
 * silence that means "go ahead and refactor".
 */
export function formatRefactorWindow(r, { ifOpen = false } = {}) {
    const open = r.candidates.filter((c) => c.ok && c.open)
    const failed = r.candidates.filter((c) => !c.ok)
    if (ifOpen && open.length === 0 && failed.length === 0) return ''

    const out = []
    if (ifOpen) {
        out.push(`refactor-window: ${plural(open.length, 'candidate')} now cheap to decompose.`, '')
    } else {
        // `plural` appends a bare 's', so every noun here has to take one - "live branchs" shipped
        // in the first cut. `ref` is also the vocabulary the freshness warning above it already uses.
        out.push(`refactor-window: ${plural(r.candidates.length, 'candidate')}, `
            + `${plural(r.liveRefs, 'live ref')}, measured against ${r.baseline}.`, '')
    }
    const width = Math.max(...r.candidates.map((c) => c.id.length))

    // ORDERED BY DISTANCE TO OPEN, closest first, because the list answers two questions and config
    // order answered neither: which one can I start now, and which is furthest from ever being
    // startable. With four candidates the reader was left doing largest-over-threshold in their head
    // for each; with a dozen nobody would. An open candidate has a ratio at or below 1 and therefore
    // sorts to the top by construction. A candidate that could not be measured has no distance at
    // all and goes last rather than being given a fabricated position.
    const distance = (c) => (c.largest ? c.largest.churn / c.threshold : 0)
    const ordered = [...r.candidates].sort((a, b) => {
        if (a.ok !== b.ok) return a.ok ? -1 : 1
        return distance(a) - distance(b)
    })
    if (!r.prsKnown) {
        out.push(`  WARNING: ${r.prsReason} - pull requests below are UNKNOWN, not absent.`, '')
    }
    // BOTH ENDS, NAMED. The ordering already puts the nearest first, but the two questions actually
    // asked of this list - which can I start, and which is the worst - are answered by its ends, and
    // a reader should not have to infer that from position. Only when there is a spread to describe.
    const measured = ordered.filter((c) => c.ok)
    if (!ifOpen && measured.length > 1) {
        const near = measured[0]
        const far = measured[measured.length - 1]
        // A measured candidate always has `largest` here: one with none is `ok: false` and is
        // filtered out above, so this cannot dereference null while ranking.
        const say = (c) => (c.open ? `${c.id} (open now)`
            : `${c.id} (${(c.largest.churn / c.threshold).toFixed(1)}x over)`)
        out.push(`  nearest to workable: ${say(near)}`, `  furthest away:       ${say(far)}`, '')
    }

    for (const c of ordered) {
        if (ifOpen && c.ok && !c.open) continue
        if (!c.ok) {
            out.push(`  FAILED  ${c.id}`, `          ${c.reason}`, '')
            continue
        }
        const verdict = c.open ? 'OPEN' : 'BUSY'
        const size = c.largest ? `${c.largest.churn} touched` : 'no divergence'
        // The distance is the actionable half: "3.5x over" says how far off this one is, and
        // "180 to spare" says how much room an open one still has before it closes again.
        const gap = !c.largest ? 'no divergence at all'
            : c.open ? `${c.threshold - c.largest.churn} to spare`
                : `${(c.largest.churn / c.threshold).toFixed(1)}x over`
        out.push(`  ${verdict.padEnd(7)} ${c.id.padEnd(width)}   largest ${size}, threshold ${c.threshold} - ${gap}`)
        if (c.largest) {
            const pr = c.largest.pr ? `PR #${c.largest.pr.number} (${c.largest.pr.state})` : 'no pull request'
            // NAMED SO IT CAN BE ACTED ON. When the window is shut this is the branch to land in
            // order to open it, which is the operator's alternative to waiting.
            out.push(`          on ${c.largest.ref} - ${pr}`)
        }
        out.push(c.open
            ? '          go, or take this entry out of bin/refactor-candidates.json'
            : '          land that branch first, or wait')
        // IS THE PROBLEM GROWING while nobody takes the moment? Derived from git on every run rather
        // than recorded, so it cannot go stale the way docs/refactoring.md's own 1533 did.
        if (c.growth) {
            const g = c.growth
            const trend = g.delta === null ? 'did not exist then'
                : g.delta > 0 ? `up ${g.delta}`
                    : g.delta < 0 ? `DOWN ${-g.delta}`
                        : 'unchanged'
            out.push(`          ${g.now} lines, ${trend} over the last ${g.days} days`)
        }
        // R15. A superset - it also counts branches predating the file - and worth printing anyway,
        // because a path the config was never told about is otherwise indistinguishable from quiet.
        // COUNT LAST, so neither line has a verb to agree with. `plural` inflects the noun and not
        // the verb, so "N live refs carry it" reads "1 live ref carry it" at the boundary - which
        // these two lines reach routinely, one of them on this repository today.
        if (c.unmatchedRefs > 0) {
            out.push(`          live refs carrying it under none of its `
                + `${plural(c.paths.length, 'configured path')}: ${c.unmatchedRefs}`)
        }
        // A ref that carries the file but could not be measured - no merge-base with the baseline is
        // the usual cause. It is neither a divergence nor an absence, and printing it is the only
        // thing standing between "we looked and found nothing" and "we could not look at this bit".
        if (c.unanswerableRefs > 0) {
            out.push('          live refs carrying it that could not be measured '
                + `(no merge-base with the baseline?): ${c.unanswerableRefs}`)
        }
        out.push('')
    }
    return out.join('\n')
}
// --- Codecov. What the recorded test history looks like when a human reads it. -------------------
//
// Every one of these renders the CACHED marker and the TRUNCATED marker when they apply. That is not
// decoration: a stale answer and a bounded answer are both answers a reader would act on differently,
// and bin/lib/codecov.mjs's header explains why a silently-capped list is the failure mode worth
// spending two lines on.

// `2026-09-02 03:24` - minutes, in UTC as the API returns it. The date is here because ordering
// should be readable off the line itself: a reader holding two shas otherwise has to go and look up
// which came first, which is exactly the cross-reference this whole command exists to remove.
const stamp = (iso) => (iso ? String(iso).replace('T', ' ').slice(0, 16) : '                ')

const cacheNote = (v) => (v.cached ? '  (cached - add --fresh to refetch)' : '')
const truncNote = (v) => (v.truncated
    ? '\n\nWARNING: hit the page bound - this is NOT the whole history, so an absence here proves nothing.'
    : '')

export function formatCoverage(v) {
    const t = v.totals
    const out = [`coverage ${t.coverage}% - ${t.hits}/${t.lines} lines across ${plural(t.files ?? 0, 'file')}`]
    if (t.misses !== undefined) out.push(`  ${t.misses} missed, ${t.partials} partial, ${t.branches} branches`)
    if (v.flagsFailed) {
        out.push(`\nPER-FLAG COVERAGE UNAVAILABLE: ${v.flagsFailed}`)
        out.push('That is a failed request, NOT a repository with no flags - the two used to print alike.')
    }
    if (v.flags.length) {
        out.push('\nper flag, ON THE DEFAULT BRANCH:')
        for (const f of v.flags) out.push(`  ${f.flag_name.padEnd(26)} ${Math.round((f.coverage ?? 0) * 100) / 100}%`)
        out.push('\nA flag at 0% here is expected, not a broken upload: only the push-only `build` job')
        out.push('runs on master, and it carries `default`. The per-suite flags are pull_request-only.')
    }
    return out.join('\n')
}

export function formatTimeline(v) {
    if (v.matches.length === 0) {
        return `no test matching /${v.query}/ in ${plural(v.corpus, 'recorded test')}.\n`
            + 'Nothing is a result, but a WEAK one: Codecov only knows tests whose suite has uploaded\n'
            + 'results since that upload was turned on. A test that never ran here has no history.'
    }
    if (v.matches.length > 6) {
        const out = [`${plural(v.matches.length, 'test')} match /${v.query}/ - too many to be one question. Narrow it:\n`]
        for (const m of v.matches.slice(0, 25)) out.push(`  ${m.name}`)
        if (v.matches.length > 25) out.push(`  ... and ${v.matches.length - 25} more`)
        return out.join('\n')
    }
    const out = []
    for (const m of v.matches) {
        out.push(`${m.name}${cacheNote(v)}`)
        for (const o of m.observations) {
            const secs = typeof o.seconds === 'number' ? `${o.seconds.toFixed(1)}s`.padStart(8) : '       -'
            const mark = o.outcome === 'pass' ? ' ' : '!'
            out.push(`  ${mark} ${String(o.outcome).padEnd(8)} ${secs}  ${stamp(o.at)}  ${o.sha}  ${o.branch ?? ''}`)
            if (o.failure) out.push(`      ${String(o.failure).split('\n')[0].slice(0, 100)}`)
        }
        const outcomes = new Set(m.observations.map((o) => o.outcome))
        out.push(outcomes.size > 1
            ? '  -> outcome CHANGED across these commits. Which commit it changed AT is above;'
            + '\n     whether that is a flake or a regression is not something this can tell you.'
            : `  -> ${plural(m.observations.length, 'run')}, all ${[...outcomes][0]}.`)
        out.push('')
    }
    return out.join('\n').trimEnd() + truncNote(v)
}

export function formatFlakes(v) {
    if (v.candidates.length === 0) {
        // truncNote ON THE NEGATIVE PATH TOO. It was only appended to the non-empty return, so a
        // walk that hit the page bound and happened to find no varying outcome printed a clean
        // negative with no hint that older history was never read - the one result someone might
        // act on by REMOVING a quarantine.
        return `no test has been recorded with more than one outcome.${cacheNote(v)}\n`
            + 'That is not "no flakes": it is no flake VISIBLE in the uploaded history, which starts\n'
            + 'when the test-results upload was turned on and covers only suites that upload.'
            + truncNote(v)
    }
    const out = [`${plural(v.candidates.length, 'test')} recorded with more than one outcome:${cacheNote(v)}\n`]
    for (const c of v.candidates) {
        out.push(`  ${c.name}`)
        const span = [c.observations[c.observations.length - 1], c.observations[0]].map((o) => stamp(o.at))
        // THREE MARKERS, NOT TWO, and the BRANCH beside each. A skip rendered as `X` was
        // indistinguishable from a failure, so the line showed more markers than the run count it
        // sat under and overstated the evidence. And the default query spans every branch, so
        // printing only outcome+sha made one master pass plus one PR-branch failure look exactly
        // like a master-state flake - which is the distinction docs/quarantined-tests.md turns on.
        const mark = (o) => (o.outcome === 'pass' ? '.' : o.outcome === 'skip' ? 's' : 'X')
        out.push(`      ${c.failures} non-pass of ${plural(c.runs, 'run')}, ${span[0]} to ${span[1]}`
            + `\n      ${c.observations.map((o) => `${mark(o)}${o.sha}@${o.branch ?? '?'}`).join(' ')}`
            + '\n      . pass · X non-pass · s skipped (skips are excluded from the run count above)')
    }
    out.push('\nCANDIDATES, not a verdict. The same evidence fits a real regression, which is why')
    out.push('docs/quarantined-tests.md will not quarantine on a rate. Next: inflight codecov test <name>')
    return out.join('\n') + truncNote(v)
}

export function formatSlowest(v) {
    const out = [`slowest of ${plural(v.tests, 'recorded test')}`
        + ` (${Math.round(v.totalSeconds)}s total):${cacheNote(v)}\n`]
    for (const r of v.rows) {
        out.push(`  ${`${r.seconds.toFixed(1)}s`.padStart(8)}  ${r.name}`
            + (r.flags.length ? `  [${r.flags.join(',')}]` : ''))
    }
    out.push('\nWall-clock on a shared runner, NOT a benchmark - see bin/lib/codecov.mjs. Never feed')
    out.push('this to a throughput comparison; that is what bin/check-throughput-regression.mjs is for.')
    return out.join('\n') + truncNote(v)
}
