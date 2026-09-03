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
    // LARGEST FIRST, by what the version ADDED - the evidence of knowledge, not of recency. A version
    // whose size is unknown sorts last rather than being given a fabricated position.
    const size = (c) => (c.added && Number.isInteger(c.added.added) ? c.added.added : -1)
    const shown = [...d.divergent].sort((a, b) => size(b) - size(a) || b.liveRefs.length - a.liveRefs.length).slice(0, top)
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
