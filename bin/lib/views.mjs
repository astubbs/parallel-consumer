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
        return `no test has been recorded with more than one outcome.${cacheNote(v)}\n`
            + 'That is not "no flakes": it is no flake VISIBLE in the uploaded history, which starts\n'
            + 'when the test-results upload was turned on and covers only suites that upload.'
    }
    const out = [`${plural(v.candidates.length, 'test')} recorded with more than one outcome:${cacheNote(v)}\n`]
    for (const c of v.candidates) {
        out.push(`  ${c.name}`)
        const span = [c.observations[c.observations.length - 1], c.observations[0]].map((o) => stamp(o.at))
        out.push(`      ${c.failures} non-pass of ${plural(c.runs, 'run')}, ${span[0]} to ${span[1]}`
            + `\n      ${c.observations.map((o) => `${o.outcome === 'pass' ? '.' : 'X'}${o.sha}`).join(' ')}`)
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
