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
        slug,
    }
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
    if (view.parents.length > 0) {
        return { kind: 'integration', remedy: `integration branch for ${view.parents.length} others `
            + `- record that in docs/inflight/branch-${view.slug}.md so it does not read as an orphan` }
    }
    const unpushed = !view.isRemote && !view.upstream
    return {
        kind: unpushed ? 'unpushed-and-untracked' : 'untracked',
        remedy: unpushed
            ? `NOT PUSHED and named nowhere. Push it, or write docs/inflight/branch-${view.slug}.md saying what it is`
            : `named nowhere. Open a PR, or write docs/inflight/branch-${view.slug}.md saying what it is`,
    }
}
