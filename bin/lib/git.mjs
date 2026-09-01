// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE SINGLE HOME of git access for bin/lib/*.mjs - the same move bin/lib/inflight-tags.sh makes for
// the tag vocabulary: source this, do not copy from it. That file's header names the failure this
// prevents, having watched it happen: two consumers carried private copies, each annotated "these
// WILL drift", and drift in a shared primitive is the worst class this system has, because both
// copies keep working while they disagree.
//
// NOTHING HERE INTERPRETS GIT. Every function is a thin wrapper over plumbing - for-each-ref,
// cat-file, ls-tree, rev-parse - and the only work done in JavaScript is grouping the output into a
// Map. Where git offers an exact answer and a heuristic one, this takes the exact one: blob identity
// over `--find-renames`, never `--follow`.
//
// A containment helper was written here and removed unused. The knowledge it carried is real and is
// recorded in docs/inflight/ci-inflight-absorbs-the-query-half.md instead - `git branch -d` and
// `git cherry` both answer a different question than they appear to - because a function nobody
// calls is documentation with a maintenance cost, not a mechanism.
//
// IT NEVER CALLS process.exit AND NEVER PRINTS. bin/inflight.mjs owns the process boundary;
// bin/test-inflight.mjs asserts that no library under bin/lib/ contains a process exit at all.

import { execFileSync } from 'node:child_process'
import { statSync } from 'node:fs'

/** Run a command; return {ok, out, status}. Never throws - callers decide what a failure means. */
export function exec(cmd, args, opts = {}) {
    try {
        return {
            ok: true,
            out: execFileSync(cmd, args, { encoding: 'utf8', maxBuffer: 256 * 1024 * 1024, ...opts }),
            status: 0,
        }
    } catch (e) {
        return { ok: false, out: e.stdout ?? '', status: e.status ?? -1 }
    }
}

export const lines = (s) => s.split('\n').filter((l) => l.length > 0)

/**
 * Local branches plus origin's, with their tip SHAs.
 *
 * Deliberately NOT `--all`: that pulls in tags and refs/stash, which add noise without adding
 * documents. The symbolic HEAD is dropped because it duplicates whatever it points at.
 *
 * THE TIP SHA IS THE CACHE KEY for everything downstream. A ref that has not moved cannot have
 * changed its contents, so it is an exact key rather than a heuristic one - which is what makes a
 * time-based TTL the wrong instrument here.
 *
 * @returns {{ref: string, sha: string}[]}
 */
export function refTips() {
    const res = exec('git', ['for-each-ref', '--format=%(objectname) %(refname:short)', 'refs/heads', 'refs/remotes/origin'])
    if (!res.ok) return []
    return lines(res.out)
        .map((l) => ({ sha: l.slice(0, l.indexOf(' ')), ref: l.slice(l.indexOf(' ') + 1) }))
        .filter((r) => !r.ref.endsWith('/HEAD'))
}

/**
 * The ref every comparison is made against, so "not on the mainline" is a statement about a real ref
 * rather than about whatever happens to be checked out.
 */
export function baseline() {
    return exec('git', ['rev-parse', '--verify', '--quiet', 'origin/master']).ok ? 'origin/master' : 'master'
}

/**
 * Blob SHA of one path on many refs, in ONE process.
 *
 * `git cat-file --batch-check` is the whole trick: the obvious loop forks git once per ref, which at
 * this repo's ref count is 435 processes. Measured 2026-09-01, this is 31ms for 435 refs.
 *
 * A ref where the path does not exist comes back `missing` and is simply absent from the Map -
 * "this branch does not carry that note" is a finding, not an error.
 *
 * @returns {Map<string, string>} ref -> blob SHA, for refs that carry the path
 */
export function blobsForPath(refs, path) {
    // EACH QUERY CARRIES ITS OWN REF as a trailing `%(rest)` token, so every output line says which
    // ref it answers for. The alternative is pairing output to input by position, which is correct
    // today and silently wrong the first time anything filters or reorders. Verified against git's
    // actual output: a hit prints `<sha> <rest>`, a miss prints `<spec> missing` and drops the rest -
    // so a miss is detected on the second field, never by trying to parse the ref back out.
    const query = refs.map((r) => `${r}:${path} ${r}`).join('\n')
    const res = exec('git', ['cat-file', '--batch-check=%(objectname) %(rest)'], { input: `${query}\n` })
    const out = new Map()
    for (const line of lines(res.out)) {
        const cut = line.indexOf(' ')
        if (cut < 0) continue
        const first = line.slice(0, cut)
        const rest = line.slice(cut + 1)
        if (rest === 'missing' || rest === '') continue // absence is a finding, recorded by omission
        out.set(rest, first)
    }
    return out
}

/** Every (blob, path) pair under a pathspec on one ref. */
export function treeEntries(ref, pathspec) {
    const res = exec('git', ['ls-tree', '-r', ref, '--', pathspec])
    if (!res.ok) return []
    return lines(res.out).map((l) => {
        const [meta, path] = l.split('\t')
        const cols = meta.split(/\s+/)
        return { blob: cols[2], path }
    }).filter((e) => e.blob && e.path)
}

/** Line-level size of the difference between two blobs, without checking anything out. */
export function blobDiffStat(a, b) {
    if (a === b) return { added: 0, removed: 0, identical: true }
    const res = exec('git', ['diff', '--numstat', a, b])
    if (!res.ok) return { added: null, removed: null, identical: false }
    const first = lines(res.out)[0]
    if (!first) return { added: 0, removed: 0, identical: true }
    const [added, removed] = first.split('\t')
    return { added: Number(added), removed: Number(removed), identical: false }
}

/**
 * Reasons the answers below may be stale, as data.
 *
 * A complete search of a stale corpus is still a false negative, and it reads exactly like a
 * complete search of a current one. Both incidents behind these are real: a session worked from the
 * main checkout while master advanced 151 commits underneath it, and every working-tree read
 * answered for that snapshot without saying so.
 *
 * @returns {{id: string, lines: string[]}[]}
 */
export function freshnessWarnings(base, refCount) {
    const warnings = []
    const warn = (id, ...text) => warnings.push({ id, lines: text })

    const gitDir = exec('git', ['rev-parse', '--git-dir']).out.trim()
    const commonDir = exec('git', ['rev-parse', '--git-common-dir']).out.trim()
    if (gitDir === commonDir) {
        warn('main-checkout',
            'this is the MAIN CHECKOUT, which AGENTS.md says never to work in - several',
            'sessions share it, so its HEAD can move between two of your own commands.',
            'Cut a worktree: git worktree add .claude/worktrees/<name> -b <branch> origin/master')
    }
    if (exec('git', ['rev-parse', '--is-shallow-repository']).out.trim() === 'true') {
        warn('shallow',
            'SHALLOW clone - any commit search covers only the fetched depth.',
            'Run: git fetch --unshallow')
    }
    try {
        const ageSeconds = (Date.now() - statSync(`${commonDir}/FETCH_HEAD`).mtimeMs) / 1000
        if (ageSeconds > 3600) {
            warn('stale-fetch',
                `last fetch was ${Math.floor(ageSeconds / 3600)}h ago, so '${base}' and the ${refCount} refs`,
                "are that stale. Run 'git fetch origin' and re-run.")
        }
    } catch {
        warn('never-fetched', "no FETCH_HEAD - this clone may never have fetched. Run 'git fetch origin'.")
    }
    const behind = Number(exec('git', ['rev-list', '--count', `HEAD..${base}`]).out.trim() || '0')
    if (behind > 0) {
        warn('head-behind',
            `your HEAD is ${behind} commit(s) behind ${base}. Anything answered from the REFS is`,
            `unaffected - but anything you read out of the working tree is ${behind} commits old.`,
            "AGENTS.md: 'Read the commits you inherit'.")
    }
    return warnings
}
