// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE PRIOR-ART SEARCH, as a library. `bin/inflight.mjs prior-art` is its only front end.
//
// Runs the AGENTS.md "Before you investigate anything" checks over EVERY REF, not the working tree.
//
// WHY THIS EXISTS. Those checks were written as working-tree greps - `ls docs/plans/`,
// `grep -rl <mechanism> docs/solutions/`, `ls docs/inflight/`. In this repo most of the knowledge
// base lives on unmerged branches: measured 2026-09-01, 580 of the 901 documents under `docs/`
// across every ref exist ONLY on branches that have not merged. So a session on master cannot see
// two thirds of its own prior art, runs every check, gets "nothing", and reasons from a false
// negative - which is worse than not looking, because it carries the authority of a completed check.
//
// Worked incident: docs/solutions/workflow-issues/prior-art-lives-on-branches-2026-09-01.md.
//
// WHAT "NOTHING" MEANS HERE. Every section prints the size of the corpus it searched, and the caller
// returns 2 if it could not search at all. A search whose empty output is indistinguishable from a
// search that never ran is the failure in
// docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md. "No hits across
// 443 refs" is a result; a blank line is not.
//
// NOT A GATE. "No prior art found" is a successful run, nothing in CI depends on it, and neither
// this file nor its front door takes a `check-` prefix - bin/AGENTS.md grants that prefix to the
// review agent by pattern, and a grant is not something to acquire by accident.
//
// WHY IT IS A LIBRARY AND NOT A SCRIPT. It was `bin/prior-art.mjs`, a second front door beside the
// gates. One front door is the point of bin/inflight.mjs: an agent that has to know which of N
// scripts to reach for is an agent that reaches for `git grep` instead.
//
// IT RETURNS A RESULT, NOT AN EXIT CODE, AND NOT TEXT. The first cut of this split returned 0 or 2
// and printed as it went - a shell script in Node's clothing. Three things follow from returning the
// findings instead, and the third is the reason:
//
//   - The process boundary exists in exactly one place, bin/inflight.mjs, which is the only file
//     that may call process.exit. A library that exits has decided something that is not its to
//     decide.
//   - The self-test asserts on structure rather than scraping stdout. A test that greps its own
//     output cannot tell a missing hit from a changed heading.
//   - EVERY FEATURE QUEUED BEHIND THIS ONE NEEDS THE FINDINGS, NOT THE TEXT. Comparing a note's
//     state across branch tips, grouping hits by ref cluster rather than by path, searching headings
//     only - each is a different view over the same `hits`, and none of them is reachable from a
//     formatted page. Format is a consumer of the result, not the result.
//
// `format()` renders one for a terminal; `onSection` streams sections as they complete, because a
// 438-ref search that prints nothing until it finishes reads as a hang.

import { baseline as baselineRef, exec, freshnessWarnings, lines, refTips } from './git.mjs'
import { formatWarnings } from './views.mjs'
import { DOC_AREAS, REPO } from './repo.mjs'


/**
 * The jq filter that matches a search pattern against a GitHub item's title and body.
 *
 * EXPORTED SO IT CAN BE TESTED. It was a closure inside `priorArt`, which is why the escaping bug
 * below could only be found by reading it rather than by running it.
 *
 * `JSON.stringify` for the pattern, never hand-rolled quoting: a jq string literal IS a JSON string,
 * so this is the only escaping that is complete. The previous version escaped `"` and left `\`
 * alone - CodeQL flagged it, and it bites on the first example this tool's own usage text gives.
 * Terms are documented as extended regexes, and `\b` is ALSO a valid JSON escape, for backspace. So
 * `prior-art '\bRetryQueue\b'` asked GitHub for a literal backspace character, matched nothing, and
 * reported "nothing" - a false negative manufactured by the tool built to prevent false negatives,
 * with no error anywhere.
 */
export function jqFilter(pattern, shape) {
    return `.[] | select((.title + " " + (.body // "")) | test(${JSON.stringify(pattern)}; "i")) | ${shape}`
}

export const summary = 'search plans, solutions, notes, commits and GitHub across EVERY ref'

export const usage = `Usage: bin/inflight.mjs prior-art [--headings] [--by-ref] <term> [<term>...]

Terms are case-insensitive extended regexes, OR-ed together. Grep the MECHANISM, never the symptom
- the class, the lock, the option, the exception, the log line. A failing test's name is the weakest
search term available.

  bin/inflight.mjs prior-art isTransactionCommittingInProgress acquireCommitLock
  bin/inflight.mjs prior-art RetryQueue writeLock

--headings matches only markdown headings, and shows the heading TEXT rather than just the path. A
document's headings are its own table of contents, so this answers "has anyone WRITTEN ABOUT X",
where the default answers "does X appear anywhere". Measured: 8812 hits become 2066 headings, for
the same cost. Reach for it first on a broad term.

--by-ref groups the hits by the SET OF REFS carrying them instead of listing one line per path. Use
it when a term returns dozens of paths: identical ref-sets mean one branch, and the per-path view
cannot say that. A cluster whose refs are all gone is a dead branch, not prior art.`

/**
 * @typedef {{path: string, refs: string[], onBaseline: boolean}} Hit
 * @typedef {{n: string, heading: string, pathspec: string, hits: Hit[]}} Section
 * @typedef {{id: string, lines: string[]}} Warning
 *
 * @typedef {object} PriorArtResult
 * @property {boolean} ok        false only when the search could not run at all
 * @property {string} [reason]   why not, when ok is false
 * @property {string} pattern
 * @property {string} baseline
 * @property {number} refsSearched  printed everywhere, so "nothing" is a size and not a blank line
 * @property {Warning[]} warnings
 * @property {Section[]} sections
 * @property {{term: string, entries: string[]}[]} commits
 * @property {{ran: boolean, skipped?: string, lists: {heading: string, note?: string, entries: string[], failed: boolean}[]}} github
 *
 * @param {string[]} terms case-insensitive extended regexes, OR-ed together
 * @param {{onSection?: (s: Section, r: PriorArtResult) => void, github?: boolean, headings?: boolean}} [opts]
 *   `headings: true` matches only markdown HEADINGS, and returns the heading TEXT rather than just
 *   the paths. A document's headings are its own table of contents - what it is *about*, as opposed
 *   to every place a word happens to appear - so this is the mode for "has anyone written about X",
 *   where the body-text mode answers "does X appear anywhere". Measured on this repo: one term went
 *   from 8812 ref:path hits to 2066 heading lines, at the same cost.
 *   `github: false` keeps the search entirely local - for a caller that only wants the tree, and for
 *   the self-test, which must not depend on a rate limit shared with every parallel session here.
 * @returns {PriorArtResult}
 */
export function priorArt(terms, opts = {}) {
    /** @type {PriorArtResult} */
    const result = {
        ok: false, pattern: terms.join('|'), baseline: '', refsSearched: 0,
        warnings: [], sections: [], commits: [], github: { ran: false, lists: [] },
    }
    const cannot = (reason) => ({ ...result, ok: false, reason })

    if (terms.length === 0) return cannot('no search terms given')

    // Anything the caller passes is already a regex, so a term containing `|` or parens composes.
    const pattern = result.pattern
    // A markdown heading, then the caller's pattern anywhere on that line. POSIX class rather than
    // `\s`, because git grep -E is ERE and does not read the PCRE shorthand.
    const grepPattern = opts.headings ? `^#{1,6}[[:space:]].*(${pattern})` : pattern

    // Local branches plus origin's, minus the symbolic HEAD which duplicates whatever it points at.
    // Deliberately NOT `--all`: that pulls in tags and refs/stash, which add noise without adding docs.
    const tips = refTips()
    if (!tips.ok) return cannot('cannot list refs - is this a git repository?')
    const refs = tips.tips.map((r) => r.ref)
    if (refs.length === 0) return cannot('no branch refs found - nothing to search')

    const baseline = baselineRef()
    if (!baseline) return cannot('neither origin/master nor master resolves - no baseline to compare against')

    result.baseline = baseline
    result.refsSearched = refs.length
    result.warnings = freshnessWarnings(baseline, refs.length)

    // ------------------------------------------------------------------------------------------------
    // Documents, across every ref. A hit records EVERY ref carrying it, not just the first: which
    // refs carry a path is the finding, not decoration. `onBaseline` is the flag that matters - a
    // path absent from the baseline is prior art the working tree cannot show you.
    // ------------------------------------------------------------------------------------------------
    // THE LAST SECTION EXCLUDES THE AREAS EXPLICITLY. It was `docs/*.md`, which reads as "markdown
    // directly under docs/" and is not what git does: `*` in a pathspec crosses `/` under wildmatch,
    // so every plan, solution and note matched section 4 as well as its own, and the section headed
    // "Everything else" was in fact "everything, again". Found by running --by-ref against this very
    // file's history, where one note surfaced as two paths in a single cluster.
    //
    // DERIVED FROM `DOC_AREAS`, NOT LISTED HERE. The three areas were a private table in this file,
    // and the context query, the docs shape and the session index all need the same three rows -
    // the REPO defect, one row wider. The numbering and headings are byte-identical to the table
    // this replaced; the self-test pins them, because a reader has learned to scan for them.
    const SECTIONS = [
        ...DOC_AREAS.map((a, i) => [String(i + 1), `${a.name} - ${a.dir}/`, [`${a.dir}/`]]),
        [String(DOC_AREAS.length + 1), 'Everything else under docs/', [
            'docs/', ...DOC_AREAS.map((a) => `:(exclude)${a.dir}/`)]],
    ]
    for (const [n, heading, pathspec] of SECTIONS) {
        // git grep exits 1 for "no match" and >1 for a real error; only the latter is a problem.
        // Without `-l` in headings mode, because the heading TEXT is the answer there, not the path.
        const grepArgs = opts.headings
            ? ['grep', '-i', '-E', grepPattern, ...refs, '--', ...pathspec]
            : ['grep', '-l', '-i', '-E', grepPattern, ...refs, '--', ...pathspec]
        const res = exec('git', grepArgs)
        if (!res.ok && res.status > 1) {
            return cannot(`git grep failed (status ${res.status}) on ${pathspec.join(' ')} - results are NOT trustworthy`)
        }
        // ref:path (-l), or ref:path:heading. A path can contain ':' only pathologically; split on
        // the first, and in headings mode split once more to separate the matched line.
        const byPath = new Map()
        const headingsByPath = new Map()
        for (const hit of lines(res.out)) {
            const i = hit.indexOf(':')
            if (i < 0) continue
            const ref = hit.slice(0, i)
            let path = hit.slice(i + 1)
            if (opts.headings) {
                const j = path.indexOf(':')
                if (j < 0) continue
                const text = path.slice(j + 1).trim()
                path = path.slice(0, j)
                if (!headingsByPath.has(path)) headingsByPath.set(path, new Set())
                headingsByPath.get(path).add(text)
            }
            if (!byPath.has(path)) byPath.set(path, [])
            byPath.get(path).push(ref)
        }
        const section = {
            n, heading, pathspec,
            hits: [...byPath.keys()].sort().map((path) => ({
                path,
                refs: [...new Set(byPath.get(path))],
                onBaseline: byPath.get(path).includes(baseline),
                headings: [...(headingsByPath.get(path) ?? [])].sort(),
            })),
        }
        result.sections.push(section)
        opts.onSection?.(section, result)
    }

    // ------------------------------------------------------------------------------------------------
    // Code history. A mechanism that was added and later removed leaves no trace in any tree - only in
    // commits - so a tree search cannot find the experiment that already tried what you are proposing.
    // ------------------------------------------------------------------------------------------------
    for (const term of terms) {
        const res = exec('git', ['log', '--all', '--format=%h %ad %s', '--date=short', `-S${term}`])
        // A FAILED pickaxe is not an EMPTY pickaxe. This loop used to read only `res.out`, so a git
        // that died - a pathological repack, an OOM kill, a term git could not handle - produced no
        // entries and rendered as "nothing", byte-identical to a real zero-hit search. That is the
        // exact failure this file's header forbids, one section below where it forbids it.
        if (!res.ok) {
            result.commits.push({ term, entries: [], failed: true })
            continue
        }
        const entries = lines(res.out).slice(0, 15)
        if (entries.length > 0) result.commits.push({ term, entries })
    }

    // ------------------------------------------------------------------------------------------------
    // GitHub. Optional - a missing or unauthenticated gh must not look like "no prior art", which is
    // why `ran` is a field rather than an empty list.
    //
    // Every call names the repo. `gh` resolves a bare command against `upstream` in this fork, and a
    // merged-PR search that silently answers for confluentinc reads exactly like "no prior art" - see
    // docs/solutions/workflow-issues/compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md.
    // ------------------------------------------------------------------------------------------------
    result.ok = true
    if (opts.github === false) {
        result.github.skipped = 'not requested'
        return result
    }
    if (!exec('gh', ['--version']).ok) {
        result.github.skipped = 'gh not installed'
        return result
    }
    if (!exec('gh', ['auth', 'status']).ok) {
        result.github.skipped = 'gh is not authenticated'
        return result
    }
    result.github.ran = true

    const jqSelect = (shape) => jqFilter(pattern, shape)

    const ghList = (heading, note, args) => {
        const res = exec('gh', args)
        result.github.lists.push({ heading, note, entries: lines(res.out), failed: !res.ok })
    }

    ghList('6. Open PRs whose title or body matches (collision check)', undefined,
        ['pr', 'list', '-R', REPO, '--state', 'open', '--limit', '200', '--json', 'number,title,body',
            '--jq', jqSelect('"  #\\(.number) \\(.title)"')])

    ghList('7. MERGED PRs whose title or body matches',
        '(the PR that already solved something in your file is, by definition, merged)',
        ['pr', 'list', '-R', REPO, '--state', 'merged', '--limit', '200', '--json', 'number,title,body',
            '--jq', jqSelect('"  #\\(.number) \\(.title)"')])

    ghList('8. Issues, --state all (fork issues and upstream-mirror ones)',
        "(read the upstream original, not the mirror's summary)",
        ['issue', 'list', '-R', REPO, '--state', 'all', '--limit', '400', '--json', 'number,title,body,state',
            '--jq', jqSelect('"  #\\(.number) [\\(.state)] \\(.title)"')])

    return result
}

// ================================================================================================
// VIEWS OVER THE RESULT. Everything below reads `PriorArtResult` and returns a string; none of it
// runs git, and none of it decides an exit code. That separation is the point of returning findings:
// `refClusters` below is a genuinely different answer to the same search, and it is twenty lines
// precisely because the search handed back the data rather than a page of text.
// ================================================================================================

/** The header every view shares - what was searched, and how much of it. */
export function formatHeader(r) {
    const head = `prior-art: searching ${r.refsSearched} refs for /${r.pattern}/i  (baseline: ${r.baseline})\n`
    const warnings = formatWarnings(r.warnings)
    return warnings ? `${head}\n${warnings}` : head
}

/** One section, per path. The default view, and the one streamed as sections complete. */
export function formatSection(section, r) {
    const out = [`=== ${section.n}. ${section.heading} ===`]
    if (section.hits.length === 0) {
        out.push(`  nothing, across ${r.refsSearched} refs\n`)
        return out.join('\n')
    }
    for (const h of section.hits) {
        out.push(`  ${h.path}`)
        for (const text of h.headings ?? []) out.push(`      ${text}`)
        out.push(h.onBaseline
            ? `      on ${r.baseline}`
            : `      NOT ON ${r.baseline} - e.g. ${h.refs[0]} (${h.refs.length} refs)`)
    }
    out.push('')
    return out.join('\n')
}

/**
 * Group every hit by the SET OF REFS carrying it.
 *
 * WHY THIS EXISTS, and it is the first thing the result object bought. Dogfooding the per-path view
 * on a term that survived only on one abandoned branch printed sixty-five lines, one per note, each
 * saying "NOT ON origin/master" - and a reader had to compare sixty-five ref names by eye to reach
 * the actual finding, which was "this is one dead branch". Identical ref-sets are one event in the
 * repository's history; listing their members separately hides that behind their own volume.
 *
 * @returns {{refs: string[], paths: string[], onBaseline: boolean}[]} largest cluster first
 */
export function refClusters(r) {
    const byKey = new Map()
    // Deduplicated across sections: a cluster is a statement about the repository, not about how
    // this tool happens to have partitioned its search, and counting one note twice overstates the
    // finding. The overlapping-pathspec bug that made this bite is fixed above; the guard stays,
    // because a future section is free to overlap deliberately.
    const seen = new Set()
    for (const section of r.sections) {
        for (const h of section.hits) {
            if (seen.has(h.path)) continue
            seen.add(h.path)
            const refs = [...h.refs].sort()
            const key = refs.join(' ')
            if (!byKey.has(key)) byKey.set(key, { refs, paths: [], onBaseline: h.onBaseline })
            byKey.get(key).paths.push(h.path)
        }
    }
    return [...byKey.values()].sort((a, b) => b.paths.length - a.paths.length)
}

export function formatByRef(r) {
    const clusters = refClusters(r)
    if (clusters.length === 0) return `=== hits grouped by ref-set ===\n  nothing, across ${r.refsSearched} refs\n`
    const paths = clusters.reduce((n, c) => n + c.paths.length, 0)
    const out = [`=== hits grouped by ref-set - ${clusters.length} cluster(s) over ${paths} path(s) ===\n`]
    for (const c of clusters) {
        const where = c.onBaseline
            ? `on ${r.baseline}`
            : `NOT ON ${r.baseline} - ${c.refs.length} ref(s): ${c.refs.slice(0, 3).join(', ')}${c.refs.length > 3 ? ', ...' : ''}`
        out.push(`  ${c.paths.length} path(s), ${where}`)
        for (const p of c.paths) out.push(`      ${p}`)
        out.push('')
    }
    return out.join('\n')
}

/** Commits and GitHub - identical in both views, because neither is per-path. */
export function formatTail(r) {
    const out = ['=== 5. Commits that added or removed the term (git log --all -S) ===']
    if (r.commits.length === 0) out.push(`  nothing, across every ref`)
    for (const c of r.commits) {
        out.push(`  -- ${c.term}`)
        if (c.failed) out.push('  (query failed - treat as UNKNOWN, not as nothing)')
        for (const e of c.entries) out.push(`  ${e}`)
    }
    out.push('')

    if (!r.github.ran) {
        out.push(`=== 6-8. GitHub checks SKIPPED - ${r.github.skipped} ===`)
        out.push('  These are NOT "nothing found". Run the gh checks in AGENTS.md by hand.')
        out.push('')
        return out.join('\n')
    }
    for (const l of r.github.lists) {
        out.push(`=== ${l.heading} ===`)
        if (l.note) out.push(`    ${l.note}`)
        if (l.failed) out.push('  (query failed - treat as UNKNOWN, not as nothing)')
        else if (l.entries.length === 0) out.push('  nothing')
        else out.push(...l.entries)
        out.push('')
    }
    return out.join('\n')
}

/**
 * The whole run as one page. `byRef` picks the view; everything else is shared. Kept separate from
 * priorArt() so a caller that wants only the findings never pays to render them.
 */
export function format(r, { byRef = false } = {}) {
    if (!r.ok) return `prior-art: ${r.reason}`
    const middle = byRef ? [formatByRef(r)] : r.sections.map((s) => formatSection(s, r))
    return [formatHeader(r), ...middle, formatTail(r)].join('\n')
}
