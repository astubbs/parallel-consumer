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

import { perfRecord } from './perf.mjs'
import { readFileSync, statSync } from 'node:fs'

/** Run a command; return {ok, out, status}. Never throws - callers decide what a failure means. */
export function exec(cmd, args, opts = {}) {
    // Timed on both paths: a command that FAILS still cost its time, and the failures are exactly
    // where an unexpected cost hides - a retry loop, a command dying slowly on a huge input.
    const t0 = Date.now()
    try {
        const out = execFileSync(cmd, args, { encoding: 'utf8', maxBuffer: 256 * 1024 * 1024, ...opts })
        perfRecord(`${cmd} ${args[0] ?? ''}`.trim(), Date.now() - t0)
        return { ok: true, out, status: 0 }
    } catch (e) {
        perfRecord(`${cmd} ${args[0] ?? ''}`.trim(), Date.now() - t0)
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
    // `{ok}` RATHER THAN AN EMPTY ARRAY. This returned `[]` on failure, so "this is not a git
    // repository" and "this repository has no branches" were the same answer - and three commands
    // rendered it as a clean empty result and exited 0, printing "nothing, across 0 refs" over a
    // search that never ran. Reproduced outside a git repo before it was fixed.
    if (!res.ok) return { ok: false, tips: [] }
    return {
        ok: true,
        tips: lines(res.out)
            .map((l) => ({ sha: l.slice(0, l.indexOf(' ')), ref: l.slice(l.indexOf(' ') + 1) }))
            .filter((r) => !r.ref.endsWith('/HEAD')),
    }
}

/**
 * The ref every comparison is made against, so "not on the mainline" is a statement about a real ref
 * rather than about whatever happens to be checked out.
 */
export function baseline() {
    for (const ref of ['origin/master', 'master']) {
        if (exec('git', ['rev-parse', '--verify', '--quiet', ref]).ok) return ref
    }
    // BOTH CANDIDATES FAILED, so there is no baseline to compare against. The fallback used to be the
    // bare string 'master' with no check, which resolves to nothing in a shallow or single-ref clone -
    // and every consumer then read an empty tree as "the baseline carries nothing", reporting every
    // note on every branch as stranded and every version as divergent, with no warning anywhere.
    // Returning null forces the caller to say so instead of answering confidently and wrongly.
    return null
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
 * `{ok, blobs}`, for the reason refTips and treeEntries carry a flag: a failed cat-file produced an
 * empty Map, and drift read that as "no note at that path on ANY ref" - the same silent-miss shape
 * as the two P0s this file already fixed at the enumeration layer, one function further down.
 *
 * @returns {{ok: boolean, blobs: Map<string, string>}} ref -> blob SHA, for refs that carry the path
 */
export function blobsForPath(refs, path) {
    // EACH QUERY CARRIES ITS OWN REF as a trailing `%(rest)` token, so every output line says which
    // ref it answers for. The alternative is pairing output to input by position, which is correct
    // today and silently wrong the first time anything filters or reorders. Verified against git's
    // actual output: a hit prints `<sha> <rest>`, a miss prints `<spec> missing` and drops the rest -
    // so a miss is detected on the second field, never by trying to parse the ref back out.
    // A PATH CONTAINING WHITESPACE CANNOT CARRY A `%(rest)` TOKEN. git splits the input line at its
    // FIRST space to find the object spec, so `<ref>:<path with space> <ref>` asks about a truncated
    // path and answers `missing` for every ref - a note invisible to every command, with no error.
    // git emits exactly one output line per input line, in order, so positional pairing is exact;
    // the length is asserted below rather than assumed, which is the whole reason it is safe.
    const selfDescribing = !/\s/.test(path)
    const query = refs.map((r) => (selfDescribing ? `${r}:${path} ${r}` : `${r}:${path}`)).join('\n')
    const res = exec('git', ['cat-file', '--batch-check=%(objectname) %(rest)'], { input: `${query}\n` })
    const outLines = lines(res.out)
    const out = new Map()
    if (!res.ok) return { ok: false, blobs: out }

    if (!selfDescribing) {
        // A short answer is not a set of misses - it means git stopped early.
        if (outLines.length !== refs.length) return { ok: false, blobs: out }
        outLines.forEach((line, i) => {
            const [sha, second] = [line.slice(0, line.indexOf(' ')), line.slice(line.indexOf(' ') + 1)]
            if (line.indexOf(' ') > 0 && second !== 'missing') out.set(refs[i], sha)
        })
        return { ok: true, blobs: out }
    }

    for (const line of outLines) {
        const cut = line.indexOf(' ')
        if (cut < 0) continue
        const first = line.slice(0, cut)
        const rest = line.slice(cut + 1)
        // A HIT IS IDENTIFIED BY ITS OBJECT NAME, not by the second field not saying "missing".
        // The miss line is `<spec> missing`, so testing the second field mis-read a genuine hit on
        // a branch literally named `missing` as an absence, silently dropping that ref's version.
        // A 40-hex object name cannot be produced by the miss format, so this is unambiguous.
        if (!/^[0-9a-f]{40}$/.test(first) || rest === '') continue
        out.set(rest, first)
    }
    return { ok: true, blobs: out }
}

/** Every (blob, path) pair under a pathspec on one ref. */
export function treeEntries(ref, pathspec) {
    // `-z`, because git C-QUOTES a path containing non-ASCII or special characters unless told
    // otherwise - wrapping it in quotes with octal escapes. Splitting the default output on tab
    // would hand back that quoted string as if it were the path, corrupting the entry everywhere
    // downstream with nothing to surface it. NUL-terminated records cannot be quoted at all.
    //
    // `{ok, entries}` rather than a bare array, for the same reason refTips carries a flag: a
    // failed ls-tree returned `[]`, which corpusIndex read as "this branch carries no notes". If
    // the failing ref were the BASELINE, basePaths would empty and every landed note would report
    // as stranded - a plumbing failure feeding a headline number, unreported.
    const res = exec('git', ['ls-tree', '-r', '-z', ref, '--', pathspec])
    if (!res.ok) return { ok: false, entries: [] }
    const entries = res.out.split('\0').filter((r) => r.length > 0).map((rec) => {
        const [meta, path] = rec.split('\t')
        const cols = meta.split(/\s+/)
        return { blob: cols[2], path }
    }).filter((e) => e.blob && e.path)
    return { ok: true, entries }
}

/** Line-level size of the difference between two blobs, without checking anything out. */
export function blobDiffStat(a, b) {
    if (a === b) return { added: 0, removed: 0, identical: true }
    const res = exec('git', ['diff', '--numstat', a, b])
    // Distinguishable from the newFile sentinel: without `diffFailed`, the view saw a truthy object
    // with no `newFile` and rendered `+null -null since its merge-base`.
    if (!res.ok) return { added: null, removed: null, identical: false, diffFailed: true }
    const first = lines(res.out)[0]
    if (!first) return { added: 0, removed: 0, identical: true }
    const [added, removed] = first.split('\t')
    return { added: Number(added), removed: Number(removed), identical: false }
}

/**
 * When the last fetch was, and HOW MUCH OF THE CORPUS it covered.
 *
 * FETCH_HEAD's mtime dates a fetch of ANY width, so `git fetch origin master` - one ref of 292 -
 * resets the freshness clock without refreshing anything else, and the staleness warning below goes
 * quiet over a corpus exactly as stale as it was. Measured 2026-09-02: mtime forced to 2020, one
 * single-ref fetch, mtime now. The file also LISTS what that fetch brought, one line per ref, so
 * the width is readable rather than guessable - and a full fetch lists every ref it covered even
 * when none of them moved.
 *
 * @returns {{at: number, refs: number|null, source: string}|null}
 */
function lastFetch(commonDir) {
    try {
        const refs = lines(readFileSync(`${commonDir}/FETCH_HEAD`, 'utf8')).length
        return { at: statSync(`${commonDir}/FETCH_HEAD`).mtimeMs, refs, source: 'FETCH_HEAD' }
    } catch { /* no FETCH_HEAD - fall through, it is not evidence of never having fetched */ }
    // A FRESH CLONE HAS NO FETCH_HEAD, and was told "this clone may never have fetched" - the
    // opposite error, and the one that reads as most alarming on the newest corpus obtainable.
    // `packed-refs` is written by the clone itself, so its mtime dates the refs actually held.
    try {
        return { at: statSync(`${commonDir}/packed-refs`).mtimeMs, refs: null, source: 'packed-refs' }
    } catch { return null }
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

    if (!base) {
        warn('no-baseline', 'neither origin/master nor master resolves in this checkout, so there is',
            'NO baseline to compare against. Every answer below is unreliable - a shallow or',
            'single-ref clone is the usual cause. Run: git fetch origin master')
        return warnings
    }

    const gitDir = exec('git', ['rev-parse', '--git-dir']).out.trim()
    const commonDir = exec('git', ['rev-parse', '--git-common-dir']).out.trim()
    if (!gitDir || !commonDir) {
        // Both empty compare EQUAL, which used to print a confident "this is the MAIN CHECKOUT".
        // A git that cannot answer `rev-parse` is not a diagnosis, it is a missing answer.
        warn('git-unreadable', 'git could not answer `rev-parse --git-dir`; freshness below is UNKNOWN,',
            'not clean. Everything this run reports may be answering for the wrong tree.')
    } else if (gitDir === commonDir) {
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
    const last = lastFetch(commonDir)
    if (!last) {
        warn('never-fetched', 'no FETCH_HEAD and no packed-refs - this clone may never have fetched.',
            "Run 'git fetch origin'.")
    } else {
        const ageSeconds = (Date.now() - last.at) / 1000
        if (ageSeconds > 3600) {
            warn('stale-fetch',
                `last fetch was ${Math.floor(ageSeconds / 3600)}h ago (by ${last.source}), so '${base}'`,
                `and the ${refCount} refs are that stale. Run 'git fetch origin' and re-run.`)
        }
        // WIDTH, NOT JUST AGE. A fetch narrower than a quarter of the corpus dates that ref and
        // nothing else, so its recency says nothing about the refs this search actually reads.
        if (last.refs !== null && last.refs * 4 < refCount) {
            warn('narrow-fetch',
                `the last fetch covered ${last.refs} ref(s) against ${refCount} in this search, so its`,
                'timestamp dates THOSE refs only - the rest are as old as they were, and the age above',
                "is measuring the wrong thing. Run 'git fetch origin' for the whole set.")
        }
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
