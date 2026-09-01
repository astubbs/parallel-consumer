// Copyright (C) 2026 Antony Stubbs and contributors
//
// VIEWS OVER THE NOTE CORPUS. Reads what bin/lib/notes.mjs returns and produces strings; runs no git
// and decides no exit code. Kept apart from the queries so a caller that wants the findings - a hook
// building a session index, a gate deciding whether to fail - never pays to render them.

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
            out.push(cluster(c, !a ? 'differs'
                : a.newFile ? 'added on this branch, after it diverged'
                    : `+${a.added} -${a.removed} since its merge-base`))
        }
    }

    if (d.behind.versions > 0) {
        out.push(`  Not shown: ${plural(d.behind.versions, 'version')} on ${plural(d.behind.refs, 'ref')} `
            + `that ${d.baseline} itself once held - those branches are simply behind, which is`)
        out.push('  not drift and gets worse on its own. Pass --all to see them.')
    }
    return out.join('\n')
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
    for (const c of clusters) {
        out.push(`  ${plural(c.paths.length, 'note')} on ${plural(c.refCount, 'ref')} - e.g. ${c.refs[0]}`)
        for (const p of c.paths.slice(0, 5)) out.push(`      ${p}`)
        if (c.paths.length > 5) out.push(`      ... and ${c.paths.length - 5} more`)
        out.push('')
    }
    return out.join('\n')
}
