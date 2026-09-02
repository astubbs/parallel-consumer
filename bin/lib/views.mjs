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

export function formatBranch(v, gap) {
    if (!v.ok) return `${v.reason}\nTry: git for-each-ref --format='%(refname:short)' refs/heads refs/remotes/origin`

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
