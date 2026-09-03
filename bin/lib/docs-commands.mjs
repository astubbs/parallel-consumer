// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE `docs` COMMAND FAMILY - the bodies behind the registry rows bin/inflight.mjs keeps, and the
// summary and usage text each row imports, the way bin/lib/prior-art.mjs supplies its own. The
// front door holds only what is REGISTRY: the name, the `when` line that places a command among the
// others, and the row order. Everything a command does when run lives here, so the self-test can
// drive a body without the dispatcher and the front door stays a table.
//
// One query, three deliveries (the plan's Key Decisions): the read-time hook, the prompt hook and
// these commands all render the same `drift`, `docsShape` and `matchDocs` results through
// bin/lib/docs-views.mjs. What is here is the part between argv and the renderer - argument
// parsing, ref selection, the failure record - and nothing a hook would also need.
//
// THE FAILURE RECORD IS THESE COMMANDS' OWN JOB (the plan's KTD13). The session-start hook prints
// nothing for a block whose command cannot run - silence is that block's common, legitimate state -
// so without the record a broken delivery is indistinguishable from a branch nothing names. `docs
// index` and `docs for-branch` write it under the names below and clear it on their next success;
// bare `docs` prints whatever is recorded.
//
// Returns `{ok, reason?, note?}` and emits through the caller's `emit`; never prints, never exits,
// never decides a code. bin/inflight.mjs owns the process boundary, and with it the channels: what
// is emitted is the answer, on stdout; a `note` is a sentence for whoever ran the command by hand,
// which the front door prints to stderr so a hook capturing stdout never injects it.

import { INVALIDATING_WARNINGS, baseline, blobContents, blobsForPath, exec, freshnessWarnings, refTips } from './git.mjs'
import { clearDeliveryFailure, deliveryFailures, recordDeliveryFailure } from './cache.mjs'
import { corpusIndex, drift, prsByBranch, stranded } from './notes.mjs'
import { DOC_AREAS } from './repo.mjs'
import { docsShape } from './docs-shape.mjs'
import { matchDocs, termsFromBranch } from './terms.mjs'
import { formatWarnings } from './views.mjs'
import {
    HEADER_TOP, formatDivergenceHeader, formatDocsIndex, formatDocsList, formatDocsShape, formatDocsShow, formatMatchBody,
    sourceFrame, termsAsArgv,
} from './docs-views.mjs'

/** The name the session index records its failures under - what bare `docs` shows. */
const INDEX_DELIVERY = 'session index'
/** The same for the branch-facts block the session hook injects after the index (the plan's R11). */
const BRANCH_DELIVERY = 'branch facts'
const DEFAULT_INDEX_MAX_LINES = 400

/** Under one of the three corpus areas - the only paths the divergence query is defined for. */
const inCorpus = (path) => DOC_AREAS.some((a) => path.startsWith(`${a.dir}/`))

// --- The text the registry imports. -------------------------------------------------------------

export const docsSummary = 'the docs corpus across every ref - its shape, one level of it, a document with its divergence header, the header alone, or what names your branch'
export const docsUsage = `Usage: bin/inflight.mjs docs                                  the corpus shape, and the guide
       bin/inflight.mjs docs list <area> [<group>]              one level of it, with the next level's commands
       bin/inflight.mjs docs show <path> [--ref <ref>] [--header-only]
       bin/inflight.mjs docs header <path> [--ref <ref>]
       bin/inflight.mjs docs for-branch [<ref>]                 what names the branch, its PR, its issues

The docs corpus - docs/inflight/, docs/solutions/, docs/plans/ - read across EVERY ref, never the
working tree. Bare \`docs\` prints each area, its groups and their counts, how many documents exist
only off the baseline, the subcommands with when to use each, and a notice for any delivery of the
context query that has a recorded failure. Every level prints the commands for the next, so the
walk from here to one document is copy and paste.

  list    the areas; one area's groups; or one group's documents, each with its \`docs show\`
  show    one document with its divergence header, from the right ref
  header  the header alone - what the read-time hook shows, in full
  index   the session-start index for the three areas - every title, corpus-scoped
  for-branch  the documents naming a branch, its cached PR and its issue numbers - the block after the index`

export const listSummary = "one level of the corpus shape - the areas, one area's groups, or one group's documents with their docs show commands"
export const listUsage = `Usage: bin/inflight.mjs docs list <area> [<group>]

Areas are the corpus directories by their last segment: inflight, solutions, plans. Groups are what
the session index groups by - a solution's category directory, an in-flight note's impact (plus
registers, feature, unmatched, closed and deferred), a plan's year-month - and the area level lists
them with the command for each. The leaf lists every document as a title, its path, whether it
exists only off the baseline (and on which ref), and the \`docs show\` command that prints it.

An unknown area or group is not an error: the valid names are printed, each as a command, exit 0.

  bin/inflight.mjs docs list inflight
  bin/inflight.mjs docs list inflight crash
  bin/inflight.mjs docs list solutions test-flakiness
  bin/inflight.mjs docs list plans 2026-09`

export const showSummary = 'one document with its full divergence header, from the baseline or the first live ref carrying it'
export const showUsage = `Usage: bin/inflight.mjs docs show <path> [--ref <ref>] [--header-only]

Prints the header, then the document from ONE ref, and the first line names which: the baseline when
it carries the path, else the first carrying live ref in sorted order. Archival refs - tags,
refs/backup - are never chosen by default; they are reported as preserved, and --ref reaches them.

The header is the full tier of the query the read-time hook runs at the summary tier: how many
distinct divergent versions exist on live refs, which branches and PRs carry the largest, what each
added - headings, else its first added line - and which ref set was searched. Divergence is the only
claim it makes; nothing here says a version is newer.

--ref <ref>     show that ref's copy (and describe THAT copy's state in the header)
--header-only   the header alone - the same text as \`docs header\`

  bin/inflight.mjs docs show docs/inflight/bug-857-family.md
  bin/inflight.mjs docs show docs/inflight/bug-857-family.md --ref origin/feats/hasten-micro-mvp`

export const headerSummary = 'the full divergence header for one document, without the document'
export const headerUsage = `Usage: bin/inflight.mjs docs header <path> [--ref <ref>]

Exactly what \`docs show --header-only\` prints: which ref the header describes, how many distinct
divergent versions live refs carry and which branches and PRs hold the largest, what each added,
anything preserved only in archival refs, and the ref set searched. An agent on a host without
hooks runs this before acting on a document; with hooks, this is the "more" the read-time line
points at.

  bin/inflight.mjs docs header docs/inflight/bug-857-family.md`

export const indexSummary = 'the session-start index for the three areas - every title, grouped as the hook groups them, corpus-scoped'
export const indexUsage = `Usage: bin/inflight.mjs docs index [--max-lines <n>]

What .claude/hooks/inject-recorded-knowledge.sh injects at session start for docs/solutions,
docs/inflight and docs/plans - rendered from the refs, not the working tree, so it lists the whole
corpus: on-baseline documents under the groups the index has always used (solutions by category,
in-flight notes by the cost-of-not-knowing order with registers first and deferred last, plans by
month), and documents that exist ONLY off the baseline under the branch set carrying them, as
\`stranded\` clusters them, largest first.

--max-lines <n>   the cap on the off-baseline groups across the whole index (default ${DEFAULT_INDEX_MAX_LINES});
                  the on-baseline listing is never cut. Past the cap, the rest of an area collapses
                  to a count and the \`docs list\` command that lists it.

Cost: one index build, the same as bare \`docs\` - several seconds here, every call, no cache.

  bin/inflight.mjs docs index
  bin/inflight.mjs docs index --max-lines 100`

export const forBranchSummary = 'the documents across every live ref that name a branch - its slug, its issue number, its cached PR number and title - as the session hook injects them'
export const forBranchUsage = `Usage: bin/inflight.mjs docs for-branch [<ref>]

The branch-facts block .claude/hooks/inject-recorded-knowledge.sh injects after the session index.
Terms come from the branch name (\`fix/857-commit-lock\` gives \`#857\` and \`commit-lock\`; the kind
prefix is dropped) and, when the tool's PR cache holds the branch, its PR number and the identifiers
in its title. The cache is the only source: this never calls gh, so on a fresh cache the branch name
is all it has, and the first body line says which terms were used and whether a PR was known. One
\`git grep\` over the live refs - the same query as the prompt-terms hook, the same document lines
and the same marks.

<ref> defaults to the checked-out branch. Stdout carries the block or NOTHING - the hook captures it
and injects whatever it gets, so its silence is the answer. On the baseline - master, origin/master,
a detached head - one line saying there is nothing to look up goes to stderr, exit 0. When the terms
match nothing, the coverage - the terms, the ref count, and that an empty result is not proof - goes
to stderr; exit 0.

  bin/inflight.mjs docs for-branch
  bin/inflight.mjs docs for-branch fix/857-commit-lock`

// --- The bodies. --------------------------------------------------------------------------------

/**
 * The index, its stranded clusters and the shape over both - what bare `docs` and every `docs
 * list` level share. One build per call and no cache, per the plan's KTD5: the three-area index
 * measures about five seconds here and the budget is eight, so the cost is stated in the usage
 * rather than hidden behind a file that would go stale.
 */
export function corpusShape() {
    const index = corpusIndex()
    if (!index.ok) return { ok: false, reason: index.reason }
    const clusters = stranded(index)
    const shape = docsShape({ index, stranded: clusters })
    if (!shape.ok) return shape
    return { ok: true, shape, stranded: clusters, warnings: freshnessWarnings(index.baseline, index.refs.length) }
}

/**
 * Bare `docs` - THE MAP (the plan's R13): the shape, the guide, and the failure notice, the one
 * place a fail-open delivery's breakage is visible. `commands` are the subcommands as the front
 * door registers them - summary and `when` verbatim - so the guide cannot say what help does not.
 */
export function showCorpus(emit, commands) {
    const built = corpusShape()
    if (!built.ok) return { ok: false, reason: `docs: ${built.reason}` }
    emit(formatDocsShape(built.shape, { warnings: built.warnings, failures: deliveryFailures(), commands }))
    return { ok: true }
}

export function listDocs(args, emit) {
    const [area = null, group = null, ...extra] = args
    if (extra.length > 0) return { ok: false, reason: `docs list: takes an area and at most one group, not '${args.join(' ')}'` }
    const built = corpusShape()
    if (!built.ok) return { ok: false, reason: `docs list: ${built.reason}` }
    // ONLY THE WARNINGS THAT VOID THE ANSWER. A list level is one step of a walk whose entry
    // point, bare `docs`, printed the full set; re-printing a stale-fetch note at every step
    // buried the levels under their own preamble. Guarded, because `emit('')` is a newline.
    const warn = formatWarnings(built.warnings.filter((w) => INVALIDATING_WARNINGS.has(w.id)))
    if (warn) emit(warn)
    emit(formatDocsList(built.shape, { area, group }))
    return { ok: true }
}

/**
 * `docs show` - and `docs header`, which is this with `--header-only` appended.
 *
 * WHICH REF IS SHOWN (the plan's KTD11): the baseline when it carries the path, else the first
 * carrying LIVE ref in sorted order, and `--ref` overrides both. Archival refs - tags, refs/backup -
 * are never chosen by default: a tag is where this repository parks work before a re-cut, and a
 * document served from one is preserved history wearing the look of the live copy. The selection
 * asks git the narrow question itself (one `cat-file --batch-check` over every ref) rather than
 * reading the carriers off the drift clusters, because those list behind-only refs only under
 * `--all` and a note absent from the baseline now may still have versions the baseline once held.
 *
 * The header is the full tier of the same query the read-time hook runs at the summary tier, so
 * what an agent sees on read and what it asks for here cannot count different versions. The refs,
 * the baseline and the blob lookup resolved for the selection are handed to `drift`, which would
 * otherwise ask git the same three questions again.
 */
export function showDocument(args, emit) {
    const KNOWN = new Set(['--ref', '--header-only'])
    const unknown = args.filter((a) => a.startsWith('--') && !KNOWN.has(a))
    if (unknown.length) return { ok: false, reason: `docs show: unknown option(s): ${unknown.join(', ')} - known: --ref <ref>, --header-only` }
    const refAt = args.indexOf('--ref')
    // -1 is a sentinel, not an index - bin/inflight.mjs's cvOpts carries the incident.
    const refValueAt = refAt >= 0 ? refAt + 1 : -1
    const requested = refAt >= 0 ? args[refValueAt] : null
    if (refAt >= 0 && (requested === undefined || requested.startsWith('--'))) return { ok: false, reason: 'docs show: --ref needs a ref after it' }
    const headerOnly = args.includes('--header-only')
    const path = args.filter((a, i) => !a.startsWith('--') && i !== refValueAt)[0]?.replace(/^\.\//, '')
    if (!path) return { ok: false, reason: 'docs show: give a document path (see: note find, prior-art)' }
    if (!inCorpus(path)) {
        emit(`${path} is outside the areas this command covers - ${DOC_AREAS.map((a) => `${a.dir}/`).join(', ')} - `
            + 'so nothing was searched and no divergence is claimed for it')
        return { ok: true }
    }

    const tips = refTips()
    if (!tips.ok) return { ok: false, reason: 'docs show: cannot list refs - is this a git repository?' }
    const base = baseline()
    if (!base) return { ok: false, reason: 'docs show: neither origin/master nor master resolves - no baseline to compare against' }
    const warnings = freshnessWarnings(base, tips.tips.length)
    const lookup = blobsForPath(tips.tips.map((t) => t.ref), path)
    if (!lookup.ok) return { ok: false, reason: `docs show: cannot read ${path} across refs - the object lookup failed` }
    const liveSet = new Set(tips.tips.filter((t) => !t.archival).map((t) => t.ref))
    const carriers = [...lookup.blobs.keys()]
    const liveCarriers = carriers.filter((r) => liveSet.has(r)).sort()
    const archivalCarriers = carriers.filter((r) => !liveSet.has(r)).sort()
    const selected = requested ?? (lookup.blobs.has(base) ? base : liveCarriers[0] ?? null)
    let blob = selected === null ? null : lookup.blobs.get(selected) ?? null
    if (requested !== null && blob === null) {
        // Not a tip - a sha, HEAD, an ancestor - but still something git can read at that path.
        const one = blobsForPath([requested], path)
        blob = one.ok ? one.blobs.get(requested) ?? null : null
        if (blob === null) {
            // A count and a few names, not the list: on the most-shared note that list is several
            // hundred refs long, and an error nobody can read is an error nobody acts on.
            const sample = liveCarriers.slice(0, 5).join(', ') + (liveCarriers.length > 5 ? ` and ${liveCarriers.length - 5} more` : '')
            const held = carriers.length === 0 ? 'no ref carries it'
                : `${carriers.length} refs carry it (${liveCarriers.length} live, ${archivalCarriers.length} archival)${sample ? `, e.g. ${sample}` : ''}`
            return { ok: false, reason: `docs show: ${requested} does not carry ${path} - ${held}` }
        }
    }

    const prs = prsByBranch()
    // gh being unavailable is not "these branches have no PR" - the same line note drift prints.
    // INSIDE the header box, never before the page: the first line of `docs show` names the ref
    // shown, and the checks pin that. Emitting the warning first was invisible on a machine with an
    // authenticated gh and broke the first-line contract on CI, where there is none.
    if (!prs.ok) warnings.push({ id: 'gh-unavailable', lines: [`${prs.reason} - PR facts in the header are UNKNOWN, not absent.`] })
    const d = drift(path, {
        prs: prs.map, at: selected === null ? null : { ref: selected, blob },
        tips: tips.tips, base, lookup, previewLimit: HEADER_TOP,
    })
    if (d.ok === false) return { ok: false, reason: `docs show: ${d.reason}` }
    if (!d.found) {
        // Names the ref set it covered, so an absence reads as a search that ran and not as proof.
        emit(formatDivergenceHeader(d, { tier: 'full', warnings }))
        emit('Nothing is a result here: this searched every ref, not the working tree. Try: bin/inflight.mjs note find <fuzzy>')
        return { ok: true }
    }
    let body = null
    if (!headerOnly && selected !== null) {
        const contents = blobContents([blob])
        if (!contents.ok || !contents.contents.has(blob)) return { ok: false, reason: `docs show: cannot read ${selected}:${path}` }
        body = contents.contents.get(blob)
    }
    emit(formatDocsShow(d, { ref: selected, warnings, archivalCarriers, body }))
    return { ok: true }
}

/** `docs header`: the page with no body - one renderer, so the two channels cannot disagree. */
export const showHeader = (args, emit) => showDocument([...args, '--header-only'], emit)

export function indexDocs(args, emit) {
    let maxLines = DEFAULT_INDEX_MAX_LINES
    for (let i = 0; i < args.length; i++) {
        if (args[i] !== '--max-lines') return { ok: false, reason: `docs index: unknown argument '${args[i]}'` }
        const n = Number(args[++i])
        if (!Number.isInteger(n) || n < 0) return { ok: false, reason: `docs index: --max-lines wants a whole number, not '${args[i]}'` }
        maxLines = n
    }
    const built = corpusShape()
    // Recorded, because the hook that calls this fails open: without the record a session
    // whose index never rendered looks like one with nothing to list.
    if (!built.ok) {
        recordDeliveryFailure(INDEX_DELIVERY, built.reason)
        return { ok: false, reason: `docs index: ${built.reason}` }
    }
    clearDeliveryFailure(INDEX_DELIVERY)
    // The branch checked out, so its own notes are never the ones the cap drops: they are
    // what the working-tree scan this replaced always listed. Detached HEAD names no
    // branch and pins nothing, which is right - a detached checkout is on no workstream.
    const head = exec('git', ['rev-parse', '--abbrev-ref', 'HEAD'])
    const currentBranch = head.ok && head.out.trim() !== 'HEAD' ? head.out.trim() : null
    emit(formatDocsIndex(built.shape, {
        clusters: built.stranded, maxLines, currentBranch, warnings: built.warnings, failures: deliveryFailures(),
    }))
    return { ok: true }
}

export function docsForBranch(args, emit) {
    if (args.length > 1 || args.some((a) => a.startsWith('--'))) {
        return { ok: false, reason: `docs for-branch: takes one ref at most, not '${args.join(' ')}'` }
    }
    const cannot = (reason) => {
        recordDeliveryFailure(BRANCH_DELIVERY, reason)
        return { ok: false, reason: `docs for-branch: ${reason}` }
    }
    let ref = args[0] ?? null
    if (ref === null) {
        const head = exec('git', ['rev-parse', '--abbrev-ref', 'HEAD'])
        if (!head.ok) return cannot('cannot resolve HEAD - is this a git repository?')
        ref = head.out.trim()
    }
    // `rev-parse --abbrev-ref` answers `HEAD` for a detached head: on no workstream, so
    // nothing to look up - the same reading `docs index` makes when it pins nothing.
    // NOTHING TO SHOW IS A `note`, NEVER AN EMIT. Everything emitted here is the block the
    // session hook captures from stdout and injects verbatim, so a sentence for the person who
    // ran this by hand - the baseline, no identifier, no match - goes back as `note` for the front
    // door to print on stderr. While the baseline case was emitted, every session on master
    // opened with "master is on the baseline - nothing to look up".
    const name = ref.replace(/^origin\//, '')
    if (ref === 'HEAD' || name === 'master' || ref === baseline()) {
        return { ok: true, note: `docs for-branch: ${ref} is on the baseline - nothing to look up` }
    }
    // Cache or nothing: this runs at every session start, where a gh call is both over the
    // budget and a draw on the rate limit every parallel session shares.
    const prs = prsByBranch({ network: false })
    const pr = prs.map.get(name) ?? null
    const source = pr ? `${ref} and its PR #${pr.number}` : `${ref} (no PR in the cache)`
    const terms = termsFromBranch(ref, { prs: prs.map })
    // Silence, not a failure record: a branch named by plain words carries no identifier,
    // and the coverage line still says what was looked at, for whoever asked.
    if (terms.length === 0) {
        return { ok: true, note: `docs for-branch: ${source} yields no identifier-shaped term - nothing searched` }
    }
    const m = matchDocs(terms)
    if (!m.ok) return cannot(m.reason)
    clearDeliveryFailure(BRANCH_DELIVERY)
    // Only the marked hits are shown, as the prompt hook shows them; the unmarked tail past
    // MARK_LIMIT and the body-capped rest are counted, and the "more" command lists them.
    const marked = m.hits.filter((h) => h.onBaseline !== null)
    const more = m.hits.length - marked.length + m.truncated
    if (marked.length === 0) {
        return {
            ok: true,
            note: `docs for-branch: terms from ${source} - ${terms.join(', ')} - name no document across `
                + `${m.refsSearched} live ref(s). Not proof of absence: headings alone never justify an empty result, `
                + 'and a cold PR cache means the title was never searched for.',
        }
    }
    const body = [`terms from ${source}: ${terms.join(', ')}`, formatMatchBody(m, marked, { label: 'them', more })].join('\n')
    emit(sourceFrame('branch', ref, body, `bin/inflight.mjs prior-art --headings ${termsAsArgv(terms)}`))
    return { ok: true }
}
