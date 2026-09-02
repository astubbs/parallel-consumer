// Copyright (C) 2026 Antony Stubbs and contributors
//
// BRANCHES, and what relates them - from the commit graph, exactly, with no heuristic.
//
// The question this answers is "what IS this branch": who owns it, whether anyone but this disk has
// a copy, what it carries that the baseline does not, and - the part no other view gives you - what
// else in the repository relates to it.
//
// RELATEDNESS IS CONTAINMENT, and containment is a set-membership test. Branch A is a PARENT of B
// when A's tip is among B's commits off the baseline: B has all of A's work. One
// `rev-list <ref> ^<baseline>` per ref builds the whole map - measured 2026-09-02 at 1.9s across 436
// refs and 27,775 commits - after which every relationship is answered from memory rather than by
// forking git once per pair, which at this ref count would be ~190,000 processes.
//
// WHY IT MATTERS RATHER THAN BEING A CURIOSITY. `origin/feats/ks-streams-reconciled` has no PR and
// reads as an orphan. The graph says it fully contains eight `ks-streams` siblings: it is an
// integration branch, and that is WHY it has no PR of its own. A tracking-gap detector without this
// would report it as untracked work and be wrong.
//
// No process.exit, no printing: bin/inflight.mjs owns the process boundary.

import { cacheRead, cacheWrite } from './cache.mjs'

const REPO = 'astubbs/parallel-consumer'
import { baseline, exec, lines, refTips, treeEntries } from './git.mjs'
import { NOTES_DIR } from './notes.mjs'

/**
 * Every ref, its tip, its upstream, and the commits it carries that the baseline does not.
 *
 * @returns {{ok: boolean, reason?: string, baseline: string,
 *            refs: {ref: string, sha: string, upstream: string}[],
 *            own: Map<string, Set<string>>}}
 */
export function commitGraph() {
    const base = baseline()
    if (!base) return { ok: false, reason: 'neither origin/master nor master resolves - no baseline' }
    const tips = refTips()
    if (!tips.ok) return { ok: false, reason: 'cannot list refs - is this a git repository?' }
    if (tips.tips.length === 0) return { ok: false, reason: 'no branch refs found - nothing to relate' }

    // Upstream in the same pass; a ref with none exists only where it is stored.
    const upstreams = new Map()
    for (const l of lines(exec('git', ['for-each-ref', '--format=%(refname:short)\t%(upstream:short)',
        'refs/heads']).out)) {
        const [ref, up] = l.split('\t')
        upstreams.set(ref, up ?? '')
    }

    const own = new Map()
    for (const { ref } of tips.tips) {
        own.set(ref, new Set(lines(exec('git', ['rev-list', ref, `^${base}`]).out)))
    }
    return {
        ok: true,
        baseline: base,
        refs: tips.tips.map((t) => ({ ...t, upstream: upstreams.get(t.ref) ?? '' })),
        own,
    }
}

/**
 * What this branch integrates, and what integrates it.
 *
 * `parents` are branches whose tip this one already contains - the work it has absorbed.
 * `children` are branches that contain this one's tip.
 */
export function relatives(graph, ref) {
    const mine = graph.own.get(ref)
    if (!mine) return { parents: [], children: [] }
    const tipOf = new Map(graph.refs.map((r) => [r.ref, r.sha]))
    const myTip = tipOf.get(ref)
    const parents = []
    const children = []
    for (const other of graph.refs) {
        if (other.ref === ref) continue
        if (mine.has(other.sha)) parents.push(other.ref)
        if (graph.own.get(other.ref)?.has(myTip)) children.push(other.ref)
    }
    return { parents: parents.sort(), children: children.sort() }
}

/**
 * Who owns this branch, from the two independent records - and they answer different questions.
 *
 * The `Claude-Session` COMMIT TRAILER is durable, travels with the branch, and resolves from any
 * clone: it says which session PRODUCED the work. The `.worktree-owner` MARKER is local and
 * uncommitted: it says who is holding that worktree RIGHT NOW. Reporting either as "the owner"
 * without saying which one it is would conflate a fact about history with a fact about this machine.
 *
 * Measured 2026-09-02: 1035 commits carry a session trailer, across 63 distinct sessions.
 */
export function ownership(ref) {
    const body = exec('git', ['log', '-40', '--format=%b', ref]).out
    const session = body.match(/Claude-Session:\s*\S*?(session_[A-Za-z0-9]+)/)?.[1] ?? null

    // The marker lives in whichever worktree holds this branch, and is never committed.
    let holder = null
    const wt = exec('git', ['worktree', 'list', '--porcelain']).out
    const blocks = wt.split('\n\n')
    for (const b of blocks) {
        if (!b.includes(`branch refs/heads/${ref}`)) continue
        const dir = b.match(/^worktree (.+)$/m)?.[1]
        if (!dir) continue
        const marker = exec('cat', [`${dir}/.worktree-owner`])
        if (marker.ok) holder = marker.out.match(/^owner:\s*(.+)$/m)?.[1]?.trim() ?? null
    }
    return { session, holder }
}

/**
 * Everything one branch is, in one record.
 *
 * `notesOnly` is the count that matters for loss: note paths this branch carries that the baseline
 * does not have at all. Two ls-trees rather than the whole corpus, because this is one branch.
 */
/**
 * THE BASELINE MOMENT, looked up rather than stored.
 *
 * Antony's design, and better than the two alternatives it replaced - a committed marker per orphan,
 * or a generated snapshot the tool diffs against. Both store state that goes stale; this derives it.
 *
 * The moment the tool arrived on the baseline is the moment tracking became expected. A branch whose
 * own history predates it was cut when nothing asked, and reporting it every run would make the
 * detector noise nobody reads - roughly sixty branches here. A branch cut afterwards has no excuse.
 *
 * The set shrinks on its own as old branches land or die, and there is no snapshot file to rot,
 * which is the failure docs/todo-index.md is this repository's cautionary tale for.
 *
 * @returns {number|null} epoch ms, or null when the tool has not reached the baseline yet
 */
export function baselineMoment(base) {
    const added = lines(exec('git', ['log', base, '--diff-filter=A', '--format=%ct', '--', 'bin/inflight.mjs']).out)
    const oldest = added[added.length - 1]
    return oldest ? Number(oldest) * 1000 : null
}

export function branchView(graph, ref, prs) {
    const meta = graph.refs.find((r) => r.ref === ref)
    if (!meta) return { ref, ok: false, reason: `no such ref: ${ref}` }

    const basePaths = new Set(treeEntries(graph.baseline, NOTES_DIR).entries.map((e) => e.path))
    const notesOnly = treeEntries(ref, NOTES_DIR).entries
        .map((e) => e.path)
        .filter((p) => !basePaths.has(p))
        .sort()

    // A branch is "mentioned" when any note on the baseline names it - the record that survives the
    // branch. Its own notes naming it would be circular, so this asks the baseline only.
    const mentions = lines(exec('git', ['grep', '-l', '-F', ref, graph.baseline, '--', `${NOTES_DIR}/`]).out)
        .map((l) => l.slice(l.indexOf(':') + 1))

    // A PR EXPLAINS BRANCHES OTHER THAN ITS OWN HEAD, and missing that produced a false positive:
    // astubbs/parallel-consumer#271 bases on `feats/ks-streams-reconciled` and names it in its body,
    // so that branch was documented all along while the detector called it tracked nowhere. A base
    // ref is exact; a body mention is textual and weaker, so they are reported separately.
    const bare = ref.replace(/^origin\//, '')
    const explainedBy = []
    for (const [head, pr] of prs) {
        if (head === bare) continue
        // A base ref IS a branch, so this is exact and costs a few bytes in the bulk PR fetch.
        if (pr.baseRefName === bare) explainedBy.push({ pr, how: 'bases on it' })
    }

    // A NOTE THAT PLAUSIBLY OWNS THIS BRANCH, before proposing a new file. The remedy told me to
    // write branch-feats-ks-streams-reconciled.md while branch-ks-streams-workstream.md already
    // owned that workstream - a second file for one item is what docs/inflight/AGENTS.md forbids.
    // Matched on the branch's distinctive tokens rather than the whole slug, since a workstream note
    // is named for the workstream and not for any one of its branches.
    const tokens = bare.split(/[^A-Za-z0-9]+/)
    // Listed then filtered in JS: a `branch-*.md` PATHSPEC returns nothing here, while the plain
    // directory returns all thirteen - ls-tree's glob handling is not what the shell teaches you to
    // expect, and a pathspec that silently matches nothing is the shape of every quiet miss above.
    const existing = lines(exec('git', ['ls-tree', '-r', '--name-only', graph.baseline,
        '--', `${NOTES_DIR}/`]).out).filter((f) => f.includes('/branch-'))
    // One long token is enough - `ks-streams-workstream` is named for the workstream, not for any
    // one of its branches, so requiring two shared tokens found nothing. Two short ones also count.
    const candidateNotes = existing.filter((f) => {
        const shared = tokens.filter((t) => t.length >= 3 && f.includes(t))
        return shared.some((t) => t.length >= 6) || shared.length >= 2
    })

    // Cut before the tool arrived on the baseline, so nothing asked for a note at the time.
    const moment = baselineMoment(graph.baseline)
    const firstCommit = lines(exec('git', ['log', ref, `^${graph.baseline}`, '--format=%ct']).out).pop()
    const predatesBaseline = moment !== null && firstCommit !== undefined
        && Number(firstCommit) * 1000 < moment
    const slug = ref.replace(/^origin\//, '').replace(/[^A-Za-z0-9]+/g, '-')

    return {
        ref,
        ok: true,
        baseline: graph.baseline,
        isRemote: ref.startsWith('origin/'),
        upstream: meta.upstream,
        pr: prs.get(ref.replace(/^origin\//, '')) ?? null,
        commitsOffBaseline: graph.own.get(ref)?.size ?? 0,
        containedInBaseline: (graph.own.get(ref)?.size ?? 0) === 0,
        // Related branches carry their OWN PR state, because "what integrates this" is only half an
        // answer if you then have to look each one up by hand - and a branch with no PR is a
        // different fact from one whose PR is unknown, so it is spelled out rather than left blank.
        ...(() => {
            const rel = relatives(graph, ref)
            const withPr = (name) => ({ ref: name, pr: prs.get(name.replace(/^origin\//, '')) ?? null })
            return { parents: rel.parents.map(withPr), children: rel.children.map(withPr) }
        })(),
        ...ownership(ref),
        notesOnly,
        mentions,
        explainedBy,
        candidateNotes,
        predatesBaseline,
        baselineKnown: baselineMoment(graph.baseline) !== null,
        slug,
    }
}

/**
 * ASK GITHUB ABOUT ONE BRANCH, only when everything local came up empty.
 *
 * Antony's design, and it replaces fetching every PR body: that took the bulk response from 56K to
 * 2.3MB to answer a question about the rare branch that looks untracked. This asks GitHub's search
 * API for one name instead - measured at 0.94s - and only on a miss.
 *
 * The result is cached locally, but the REAL cache is the fix: the agent writes the tracking note,
 * the note merges, and every later run answers from the tree without asking GitHub at all. A
 * self-eliminating query is better than a warm cache.
 *
 * Returns `{ok, prs}` - `ok: false` means GitHub could not answer, which is not the same as "no PR
 * mentions this branch" and must never be rendered as though it were.
 */
export function prSearch(ref, { cache = true } = {}) {
    const bare = ref.replace(/^origin\//, '')
    const key = `search:${bare}`
    const hit = cache ? cacheRead('pr-search.json', { key, maxAgeMs: 6 * 60 * 60 * 1000 }) : null
    if (hit) return { ok: true, cached: true, prs: hit }

    const res = exec('gh', ['search', 'prs', '--repo', REPO, bare,
        '--json', 'number,title,state', '--limit', '5'])
    if (!res.ok) return { ok: false, prs: [] }
    let rows = []
    try { rows = JSON.parse(res.out) } catch { return { ok: false, prs: [] } }
    if (cache) cacheWrite('pr-search.json', rows, key)
    return { ok: true, cached: false, prs: rows }
}

/**
 * THE TRACKING GAP, and the remedy for it.
 *
 * A branch with no PR, no `docs/inflight/branch-*.md`, and no mention anywhere in the baseline's
 * notes is invisible to every check this repository runs - `gh` cannot see it, CI cannot see it,
 * another clone cannot see it. That is how `bin/prior-art.mjs` came to sit unpushed and unrecorded
 * with 226 lines of tooling in it, found only by a hand-written ref sweep.
 *
 * INTEGRATION BRANCHES ARE NOT ORPHANS. A branch that fully contains other branches is explained by
 * the graph, and reporting it as untracked would be wrong - so it is reported as what it is.
 *
 * Returns a remedy STRING, not a flag: a report gets skimmed, an instruction gets acted on.
 */
export function trackingGap(view) {
    if (view.pr) return null
    if (view.containedInBaseline) return null // already landed; nothing to lose
    if (view.mentions.length > 0) return null
    if (view.explainedBy.length > 0) return null // another PR bases on it or names it
    // Point at the note that plausibly already owns this branch rather than inventing a filename.
    const write = view.candidateNotes.length > 0
        ? `record it in ${view.candidateNotes[0]}, which already covers this workstream`
        : `write docs/inflight/branch-${view.slug}.md saying what it is`

    if (view.parents.length > 0) {
        return { kind: 'integration', remedy: `integration branch for ${view.parents.length} others `
            + `- ${write}, so it does not read as an orphan` }
    }
    const unpushed = !view.isRemote && !view.upstream
    // Grandfathered rather than silenced: still reported, but as backlog rather than as a new gap,
    // so the detector's loud channel stays for branches cut after tracking became expected.
    if (view.predatesBaseline) {
        return { kind: 'pre-baseline', remedy: `predates the tool reaching ${view.baseline}, so it is `
            + `backlog rather than a new gap - triage when convenient, or write `
            + write }
    }
    return {
        kind: unpushed ? 'unpushed-and-untracked' : 'untracked',
        remedy: unpushed
            ? `NOT PUSHED and named nowhere. Push it, or ${write}`
            : `named nowhere. Open a PR, or ${write}`,
    }
}
