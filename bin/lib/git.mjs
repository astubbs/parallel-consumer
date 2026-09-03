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
import { readFileSync, readdirSync, statSync } from 'node:fs'

/**
 * Run a command; return {ok, out, err, status}. Never throws - callers decide what a failure means.
 *
 * STDERR IS CAPTURED, NOT INHERITED. execFileSync forwards a child's stderr to the parent's unless
 * told otherwise, so every `fatal: not a git repository` git printed reached whoever ran the
 * library - and for a hook, whose stderr the harness shows the user on some paths, that turned a
 * fail-open silence into three lines of git noise (caught by bin/test-check-docs-hooks.mjs's
 * forced-failure case). The text is returned as `err` rather than dropped: the reason a command
 * failed is exactly what a caller rendering "could not answer" wants to say.
 */
export function exec(cmd, args, opts = {}) {
    // Timed on both paths: a command that FAILS still cost its time, and the failures are exactly
    // where an unexpected cost hides - a retry loop, a command dying slowly on a huge input.
    const t0 = Date.now()
    try {
        const out = execFileSync(cmd, args, {
            encoding: 'utf8', maxBuffer: 256 * 1024 * 1024, stdio: ['pipe', 'pipe', 'pipe'], ...opts,
        })
        perfRecord(`${cmd} ${args[0] ?? ''}`.trim(), Date.now() - t0)
        return { ok: true, out, err: '', status: 0 }
    } catch (e) {
        perfRecord(`${cmd} ${args[0] ?? ''}`.trim(), Date.now() - t0)
        return { ok: false, out: e.stdout ?? '', err: String(e.stderr ?? ''), status: e.status ?? -1 }
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
    // LOOK EVERYWHERE. Nothing was ever blacklisted - this listed `refs/heads` and
    // `refs/remotes/origin` and simply stopped there, so 64 tags and 44 `refs/backup` refs were
    // outside the corpus while the help text said "every branch tip". Measured 2026-09-02: 12 of
    // those tags point at commits reachable from nothing else, named `backup/pre-recut-324` and
    // `recut-baseline-342` - which is exactly where this repository preserves work before a re-cut,
    // so the excluded space was a LIKELY home for stranded knowledge rather than an unlikely one.
    //
    // `*objectname` IS THE DEREFERENCED TARGET, empty for anything but an annotated tag. Without it
    // an annotated tag contributes its TAG object's sha, which is not a commit, and every read of
    // that "tip" fails in a way that looks like an empty branch.
    const res = exec('git', ['for-each-ref',
        '--format=%(objectname)\t%(*objectname)\t%(refname)\t%(refname:short)'])
    // `{ok}` RATHER THAN AN EMPTY ARRAY. This returned `[]` on failure, so "this is not a git
    // repository" and "this repository has no branches" were the same answer - and three commands
    // rendered it as a clean empty result and exited 0, printing "nothing, across 0 refs" over a
    // search that never ran. Reproduced outside a git repo before it was fixed.
    if (!res.ok) return { ok: false, tips: [] }
    return {
        ok: true,
        tips: lines(res.out)
            .map((l) => l.split('\t'))
            // `refs/remotes/<name>/HEAD` is a symbolic pointer at another ref in this same list, so
            // it contributes a duplicate tip under a name that reads like a branch. `refs/stash` is
            // this checkout's scratch, not a line of work, and its second parent is the index.
            .filter(([, , full]) => !full.endsWith('/HEAD') && full !== 'refs/stash')
            .map(([sha, deref, full, short]) => ({ sha: deref || sha, ref: short, full, ...refKind(full) })),
    }
}

/**
 * WHERE A REF LIVES, because "found in a tag" and "found on a branch" are different findings.
 *
 * Widening the corpus without this would report preserved history as in-flight work: a note held
 * only by `refs/backup/pre-rename-merge/...` is not stranded, it is archived on purpose, and a
 * remedy telling someone to go rescue it is wrong. So the corpus looks everywhere and the ANSWER
 * carries the distinction, rather than the enumeration deciding it in advance.
 */
export function refKind(full) {
    if (full.startsWith('refs/heads/')) return { kind: 'local', archival: false }
    if (full.startsWith('refs/remotes/origin/')) return { kind: 'remote', archival: false }
    if (full.startsWith('refs/remotes/')) return { kind: 'other-remote', archival: false }
    if (full.startsWith('refs/tags/')) return { kind: 'tag', archival: true }
    // refs/backup/**, refs/notes/**, and anything else a person or a script parked outside the
    // usual spaces. Unknown is treated as archival deliberately: over-reporting preserved work as
    // live is the error that sends someone to rescue something nobody lost.
    return { kind: 'archive', archival: true }
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

/**
 * Every (blob, path) pair under one pathspec - or several - on one ref.
 *
 * SEVERAL PATHSPECS IN ONE CALL, because the cost of an index is the fork count: the corpus index
 * runs this once per ref, and widening it from one docs area to three as three calls would have
 * tripled a 1.3s pass to answer the same question.
 */
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
    const specs = Array.isArray(pathspec) ? pathspec : [pathspec]
    const res = exec('git', ['ls-tree', '-r', '-z', ref, '--', ...specs])
    if (!res.ok) return { ok: false, entries: [] }
    const entries = res.out.split('\0').filter((r) => r.length > 0).map((rec) => {
        const [meta, path] = rec.split('\t')
        const cols = meta.split(/\s+/)
        return { blob: cols[2], path }
    }).filter((e) => e.blob && e.path)
    return { ok: true, entries }
}

/**
 * The CONTENT of many blobs, in ONE process - `git cat-file --batch`, the full-content sibling of
 * the `--batch-check` that `blobsForPath` already relies on.
 *
 * WHY A BATCH. Titles were read with one `cat-file -p` per blob, memoised per process. That is fine
 * for `note drift`, which asks for a few dozen, and not for an index over three docs areas across
 * every ref, where every off-baseline document needs its title and the session-start budget has no
 * room for a fork per document. The plan's KTD16 makes this the only way a title is read.
 *
 * PARSED IN BYTES, NOT CHARACTERS. Each record is `<sha> <type> <size>\n`, then exactly `size`
 * BYTES, then `\n`. A note with a non-ASCII character is longer in bytes than in characters, so
 * slicing a decoded string by `size` walks off the end of one record into the header of the next -
 * and every title after that point is garbage that looks like a title. The subprocess is therefore
 * read as a Buffer and cut by byte offsets; decoding happens per blob, after the cut.
 *
 * A missing blob comes back `<sha> missing` and is simply absent from the map. `{ok}` for the
 * reason every batch here carries it: a failed cat-file and an empty request both produce an
 * empty map, and only one of them is an answer.
 *
 * @returns {{ok: boolean, contents: Map<string, string>}} blob -> its content, as utf8
 */
export function blobContents(blobs) {
    const contents = new Map()
    if (blobs.length === 0) return { ok: true, contents }
    // `encoding: null` is how execFileSync is asked for a Buffer. The documented alias `'buffer'`
    // throws "Unknown encoding: buffer" on Node 25, and `exec` reports a throw as a failed command -
    // so the first cut of this returned an empty map that read as "no blob has a title".
    const res = exec('git', ['cat-file', '--batch'], { input: `${blobs.join('\n')}\n`, encoding: null })
    if (!res.ok) return { ok: false, contents }
    const buf = Buffer.isBuffer(res.out) ? res.out : Buffer.from(res.out ?? '')
    let at = 0
    while (at < buf.length) {
        const eol = buf.indexOf(0x0a, at)
        if (eol < 0) break
        const header = buf.subarray(at, eol).toString('utf8').split(' ')
        at = eol + 1
        // `<sha> missing` has two fields; a hit has three, the last being the byte size.
        if (header.length < 3) continue
        const size = Number(header[2])
        if (!Number.isInteger(size)) return { ok: false, contents }
        contents.set(header[0], buf.subarray(at, at + size).toString('utf8'))
        at += size + 1 // the record's trailing newline
    }
    return { ok: true, contents }
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
 * The lines blob `b` ADDS over blob `a`, in order - the raw material for "what does this version
 * add", which is the evidence the divergence header shows instead of calling anything "newer".
 *
 * `a` may be null, meaning there is nothing to diff against - the branch created the file after it
 * diverged - and then every line of `b` is an addition. That is one `cat-file -p` rather than a diff
 * against the empty blob, whose object name depends on the repository's hash algorithm.
 *
 * `git diff <blob> <blob>` prints a unified diff of the two objects with no working tree involved.
 * The `+++ b/...` header line starts with the same character as an added line, so added lines are
 * taken only from inside hunks - after the first `@@` - rather than by excluding a `+++` prefix,
 * which would also drop a content line that happens to begin `++`.
 *
 * @returns {{ok: boolean, lines: string[]}}
 */
export function blobDiffAddedLines(a, b) {
    if (a === b) return { ok: true, lines: [] }
    if (a === null) {
        const whole = exec('git', ['cat-file', '-p', b])
        return whole.ok ? { ok: true, lines: lines(whole.out) } : { ok: false, lines: [] }
    }
    const res = exec('git', ['diff', a, b])
    if (!res.ok) return { ok: false, lines: [] }
    const added = []
    let inHunk = false
    for (const l of res.out.split('\n')) {
        if (l.startsWith('@@')) inHunk = true
        else if (inHunk && l.startsWith('+')) added.push(l.slice(1))
    }
    return { ok: true, lines: added }
}

/**
 * The merge-base commit for each ref against `base`.
 *
 * `git merge-base` takes one pair and has no batch form, so this is a fork per ref and there is no
 * arrangement that avoids it. It is here rather than in a caller because it is plumbing with no
 * interpretation, and because the alternative is what this file's header was written about: the
 * private copy that works until it disagrees.
 */
export function mergeBases(base, refs) {
    const byRef = new Map()
    for (const ref of refs) {
        const mb = exec('git', ['merge-base', base, ref]).out.trim()
        if (mb) byRef.set(ref, mb)
    }
    return byRef
}

/**
 * The blob at `path` in each of those merge-base commits, in ONE subprocess.
 *
 * The obvious spelling is a `git rev-parse --verify <mb>:<path>` per ref, and that is what the first
 * two callers here did - about 3,500 forks for four candidates over 437 refs, most of a `refactor-window`
 * run. A commit SHA is a perfectly good left-hand side for `<rev>:<path>`, so `blobsForPath` already
 * batches this; it just had to be asked.
 *
 * Keyed by merge-base SHA, not by ref: branches share fork points heavily, so the distinct set is a
 * fraction of the ref count and the batch collapses accordingly. Look a ref's answer up through its
 * own entry in the map this was built from.
 */
export function mergeBaseBlobs(mbByRef, path) {
    // `{ok, blobs}`, NOT the bare map. Returning `.blobs` threw away exactly the flag `blobsForPath`
    // carries a flag for: a failed `cat-file` produces an empty map, and both consumers then read
    // that as "the path is absent at the merge-base" - which `note drift` renders as a note this
    // branch created, and `refactor-window` renders as a whole-file divergence. Two different silent
    // wrong answers from one dropped boolean.
    return blobsForPath([...new Set(mbByRef.values())], path)
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
    // FETCH_HEAD IS PER-WORKTREE; THE REFS IT UPDATES ARE SHARED. Reading only the common dir's
    // copy answered with the MAIN CHECKOUT's last fetch - the one place AGENTS.md says never to
    // work - so every worktree, which is everywhere work actually happens, was told about someone
    // else's fetch. Measured 2026-09-02: a fetch in this worktree, and the check still reported the
    // main checkout's, four hours older. Because a fetch in ANY worktree refreshes the shared refs
    // this search reads, the answer is the NEWEST across all of them, not this one's.
    const candidates = [`${commonDir}/FETCH_HEAD`]
    try {
        for (const w of readdirSync(`${commonDir}/worktrees`)) candidates.push(`${commonDir}/worktrees/${w}/FETCH_HEAD`)
    } catch { /* no worktrees dir - a plain clone, and the common dir copy is the only one */ }
    let newest = null
    for (const f of candidates) {
        try {
            const at = statSync(f).mtimeMs
            if (!newest || at > newest.at) newest = { at, file: f }
        } catch { /* this worktree has never fetched; another may have */ }
    }
    if (newest) {
        return { at: newest.at, refs: lines(readFileSync(newest.file, 'utf8')).length, source: 'FETCH_HEAD' }
    }
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
