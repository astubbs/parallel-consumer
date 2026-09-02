// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE IN-FLIGHT NOTE CORPUS, across every ref rather than the working tree.
//
// docs/inflight/AGENTS.md's contract is that a note travels on the branch that produced it, and stops
// being true when that branch does. That is the property this repo chose over a shared store, and it
// has a cost nobody was paying: THE WORKING TREE IS NOT THE CORPUS. Measured 2026-09-01, 401 of the
// 566 in-flight note paths that exist on some ref are absent from origin/master, so `ls docs/inflight/`
// answers for a third of what is open.
//
// THREE QUESTIONS, ONE PASS. Every command here reads the same index, so the second and third cost
// nothing once the first has run:
//
//   find      which note is this, and where does it live
//   drift     how does one note differ across branch tips, and what is each branch
//   stranded  knowledge that will be lost if nobody acts
//
// THE RENAME SUBTRACTION IS NOT OPTIONAL, and it is why `stranded` is not just a set difference. Of
// those 401 absent paths, most are not stranded at all - master renamed them (`next-` to `core-`,
// `parked-` to `ci-`) and the old name lingers on branches cut before the rename. A blob living at
// two paths IS a rename, exactly, with no similarity heuristic and no `--follow`: measured, it finds
// them across 435 refs in about a second. Reporting the raw difference would mean reporting ~390
// false positives, and a tool that returns 401 hits is a tool an agent stops reading - the same
// failure that made `prior-art --by-ref` necessary.
//
// THERE IS NO DISK CACHE FOR GIT DATA, and removing the one that was here is why `note drift` is
// fast. Git is already the cache: `ls-tree` and `cat-file` read packed objects, and the tip SHAs
// that would key a cache are themselves a git read. The cache that existed cost a read/write layer,
// a 2.5MB file per key, a staleness class, and it shipped one real bug - orphaned snapshots, 7.4MB
// in a single session - to make a 1.3s command take 59ms.
//
// It was also hiding a design mistake. `drift` asks about ONE path, and it was building the WHOLE
// corpus (one `ls-tree` per ref, 436 forks) to answer it, then caching the result to make that
// affordable. Asking git the narrow question directly - `cat-file --batch-check` over `<ref>:<path>`
// - is 60ms cold, which is the cached path's speed with none of its machinery.
//
// `corpusIndex` remains for the two questions that genuinely span every note (`find`, `stranded`)
// and simply pays its 1.3s. The ONE cache left is `prsByBranch`, because that one crosses the
// network and shares a rate limit with every parallel session here.
//
// No process.exit, no printing: bin/inflight.mjs owns the process boundary.


import { cacheRead, cacheWrite } from './cache.mjs'
import { baseline, blobDiffStat, blobsForPath, exec, lines, refTips, treeEntries } from './git.mjs'

export const NOTES_DIR = 'docs/inflight'
const REPO = 'astubbs/parallel-consumer'
/**
 * PR state moves without any ref moving, so this one key is time-based - and bounded, not trusted.
 *
 * TWENTY-FOUR HOURS, not thirty minutes. Thirty was chosen when nothing kept the cache current, so
 * the TTL was the only correction mechanism and had to be short. It no longer is: `inflight cache
 * pr <n>` folds a single PR in for the cost of one `gh pr view`, and the PostToolUse hook runs it
 * the moment a PR is created here. With the writers being the people working in this repository,
 * and each write updating the cache as it happens, the TTL is a backstop for drift from OUTSIDE -
 * someone closing a PR in the web UI - rather than the primary path.
 */
const PR_CACHE_TTL_MS = 24 * 60 * 60 * 1000

/**
 * Every (blob, path) under docs/inflight/ on every ref, plus the derived indexes.
 *
 * @returns {{
 *   baseline: string, refs: {ref: string, sha: string}[],
 *   byPath: Map<string, Map<string, string[]>>,   // path -> blob -> refs carrying that version
 *   byRef: Map<string, {blob: string, path: string}[]>, // ref -> what it carries; the inverse, built once
 *   blobPaths: Map<string, string[]>,             // blob -> every path it has ever lived at
 *   basePaths: Set<string>,                       // paths present on the baseline
 *   baseBlobs: Set<string>,                       // blobs present on the baseline, at any path
 *   baseEverPaths: Set<string>,                   // every path the baseline's HISTORY has held - the stranded filter
 * }}
 */
export function corpusIndex() {
    const { ok, tips: refs } = refTips()
    const base = baseline()
    // A failed ref enumeration is not an empty repository, and a missing baseline is not an empty
    // baseline. Both used to render as a confident "nothing found" and exit 0.
    if (!ok) return { ok: false, reason: 'cannot list refs - is this a git repository?' }
    if (refs.length === 0) return { ok: false, reason: 'no branch refs found - nothing to search' }
    if (!base) return { ok: false, reason: 'neither origin/master nor master resolves - no baseline to compare against' }
    // AGGREGATED, not swallowed. A single ref's ls-tree failing used to read as "that branch
    // carries no notes"; if it were the baseline, every landed note would have reported as stranded.
    const unreadable = []
    const entries = refs.map(({ ref }) => {
        const t = treeEntries(ref, NOTES_DIR)
        if (!t.ok) unreadable.push(ref)
        return [ref, t.entries.map((e) => [e.blob, e.path])]
    })
    if (unreadable.includes(base)) {
        return { ok: false, reason: `cannot read ${base}'s notes - every comparison would be against an empty baseline` }
    }

    const byPath = new Map()
    const blobPaths = new Map()
    // INVERTED ONCE, because branchFacts needs "what does this ref carry" and scanning byPath for it
    // is O(corpus) per branch - measured ~3ms a call over 26,539 rows, and `note drift` asks up to
    // six times per cluster. Built here so every consumer shares the one pass.
    const byRef = new Map()
    for (const [ref, pairs] of entries) {
        byRef.set(ref, pairs.map(([blob, path]) => ({ blob, path })))
        for (const [blob, path] of pairs) {
            if (!byPath.has(path)) byPath.set(path, new Map())
            const versions = byPath.get(path)
            if (!versions.has(blob)) versions.set(blob, [])
            versions.get(blob).push(ref)

            if (!blobPaths.has(blob)) blobPaths.set(blob, new Set())
            blobPaths.get(blob).add(path)
        }
    }

    // FROM `entries`, NOT A SECOND ls-tree. The baseline is always one of the refs above, so forking
    // again for it cost a live git process on every cache hit - 67ms where the cache promised none.
    const basePaths = new Set()
    const baseBlobs = new Set()
    for (const e of byRef.get(base) ?? []) {
        basePaths.add(e.path)
        baseBlobs.add(e.blob)
    }

    // EVERY PATH THE BASELINE HAS EVER CARRIED, from its history rather than its tree - one `git log`,
    // measured at 22ms. This is the discriminator that actually works, and finding that out cost a
    // wrong prediction: blob equality was expected to explain most absent paths as renames and
    // explained ONE, because a rename almost always carries an edit and the blob then differs. What
    // separates "never landed" from "landed and was cleaned up" is that this repo `git rm`s a note
    // when its work lands (docs/inflight/AGENTS.md), so a path the baseline once had and no longer
    // has is a CLOSED item, not a stranded one.
    const baseEverPaths = new Set(lines(
        exec('git', ['log', base, '--diff-filter=AD', '--name-status', '--format=', '--', `${NOTES_DIR}/`]).out,
    ).map((l) => l.split('\t')[1]).filter(Boolean))

    return {
        ok: true, unreadableRefs: unreadable,
        baseline: base, refs, byPath, byRef, basePaths, baseBlobs, baseEverPaths,
        blobPaths: new Map([...blobPaths].map(([b, s]) => [b, [...s]])),
    }
}

/** headRefName -> {number, title, state}. One gh call for every ref, never one per branch. */
export function prsByBranch({ cache = true } = {}) {
    // KEYED ON THE FIELD SET, so widening it cannot serve a cached answer that lacks the new
    // field. Adding `baseRefName` did exactly that: the code read it, the cache had never stored it,
    // and every branch silently looked unexplained until the TTL expired.
    const shape = 'headRefName,baseRefName,number,title,state'
    const cached = cache ? cacheRead('prs.json', { key: shape, maxAgeMs: PR_CACHE_TTL_MS }) : null
    if (cached) return { ok: true, cached: true, map: new Map(cached) }
    // Naming the repo is not optional: `gh` resolves a bare command against `upstream` in this fork,
    // and an answer for confluentinc reads exactly like "this branch has no PR".
    const res = exec('gh', ['pr', 'list', '-R', REPO, '--state', 'all', '--limit', '500',
        // NOT `body`: adding it took this response from 56K to 2.3MB, for data used on the rare
        // branch that looks untracked. `baseRefName` is a few bytes and answers the common case
        // exactly. The body question is asked per-branch, on a miss, by prSearch below.
        '--json', 'headRefName,baseRefName,number,title,state'])
    // UNAVAILABLE IS NOT "NO PR", and saying so needs a shape that can carry the difference. This
    // returned a bare Map, so an unauthenticated or rate-limited `gh` was indistinguishable from a
    // branch that genuinely has no PR - and the caller silently fell through to guessing a theme
    // from a note title. prior-art already reports its own gh skip loudly; this now can too.
    if (!res.ok) return { ok: false, reason: 'gh unavailable or unauthenticated', map: new Map() }
    let rows = []
    try {
        rows = JSON.parse(res.out)
    } catch {
        return { ok: false, reason: 'gh returned output that is not JSON', map: new Map() }
    }
    const pairs = rows.map((r) => [r.headRefName, {
        number: r.number, title: r.title, state: r.state,
        // Carried because a PR EXPLAINS branches other than its own head: its base IS a branch, by
        // definition, and costs a few bytes to know.
        baseRefName: r.baseRefName,
    }])
    if (cache) cacheWrite('prs.json', pairs, shape)
    return { ok: true, cached: false, map: new Map(pairs) }
}

/**
 * The first `# ` heading of a blob - a note's own title, read without checking anything out.
 *
 * Memoised for the process, which is always safe: a blob SHA names its content, so the answer cannot
 * change. Without it the same title was re-forked once per branch that happened to carry the same
 * note - `note drift` on a busy note spent 361ms of 527ms in `sys`, almost all of it forking.
 */
const titleCache = new Map()
export function blobTitle(blob) {
    if (titleCache.has(blob)) return titleCache.get(blob)
    const res = exec('git', ['cat-file', '-p', blob])
    let title = null
    if (res.ok) for (const l of lines(res.out)) if (l.startsWith('# ')) { title = l.slice(2).trim(); break }
    titleCache.set(blob, title)
    return title
}

/**
 * Every blob the baseline has EVER held at this path, from its history.
 *
 * THIS IS THE NOISE FILTER, and it is the difference between a tool and a wall of text. A long-lived
 * note is edited on the baseline constantly, so every branch cut before those edits is "different" -
 * and gets more different every day, without anyone doing anything. That is not drift. Reporting it
 * buries the answer: for the fork's most-edited note, 198 of the 274 carrying refs are behind and
 * nothing else.
 *
 * A branch's version is PURELY BEHIND when its blob is one the baseline itself once had. A version
 * the baseline has never held at this path is content someone added on a branch - the only kind of
 * difference worth a reader's attention, because it is the only kind that will be LOST.
 *
 * Two batched calls, measured at 17ms: rev-list the commits touching the path, then one cat-file
 * over `<commit>:<path>`. No per-ref `merge-base`, which would be one fork per ref.
 */
export function baselineHistoryBlobs(base, path) {
    // `--full-history` because plain rev-list PRUNES a merge parent's contribution when the merge is
    // TREESAME to the other parent, so a blob the baseline briefly held on a merged-away side is
    // absent - and the doc above claims "EVER held". The pruning errs safe (such a version is called
    // divergent rather than behind) but the claim was false, and completeness is the point here.
    const commits = lines(exec('git', ['rev-list', '--full-history', base, '--', path]).out)
    if (commits.length === 0) return new Set()
    const res = exec('git', ['cat-file', '--batch-check=%(objectname)'],
        { input: `${commits.map((c) => `${c}:${path}`).join('\n')}\n` })
    return new Set(lines(res.out).filter((l) => !l.includes('missing')))
}

/**
 * What this branch ADDED to the note since it diverged - not how far it has fallen behind.
 *
 * Diffing a branch's version against the baseline's CURRENT version answers the wrong question: it
 * reports the baseline's own additions as the branch's deletions. One real case read `+22 -1150`,
 * where the 22 is the finding and the 1150 is a thousand lines the baseline added afterwards.
 *
 * Against the merge-base, only the branch's own edits remain - "the newer commits since the common
 * ancestor". One `merge-base` fork per divergent cluster, which is affordable only because the
 * history filter already removed the branches that are merely behind.
 */
export function addedSinceMergeBase(base, ref, path, blob) {
    const mb = exec('git', ['merge-base', base, ref]).out.trim()
    if (!mb) return null
    const at = exec('git', ['rev-parse', '--verify', '--quiet', `${mb}:${path}`]).out.trim()
    if (!at) return { added: null, removed: null, newFile: true } // the branch created it after diverging
    return blobDiffStat(at, blob)
}

/** The baseline's own note paths - one ls-tree, memoised, used only by the theme fallback. */
const baselinePathsCache = new Map()
function baselineNotePaths(base) {
    if (!baselinePathsCache.has(base)) {
        baselinePathsCache.set(base, new Set(treeEntries(base, NOTES_DIR).entries.map((e) => e.path)))
    }
    return baselinePathsCache.get(base)
}

/**
 * What IS this branch, in facts only.
 *
 * The cascade is the PR title, else the title of a note this branch carries and the baseline does
 * not, else the branch name. Every step is a lookup. A summarised "theme" was the first design and
 * was dropped deliberately - it is the one field that cannot be reproduced and has to be verified by
 * the reader, which defeats the point of a guided command.
 *
 * THE SECOND STEP IS LAZY, and that is what lets `drift` avoid building the whole corpus. It costs
 * one `ls-tree` for this ref, and only when the ref has no PR and is actually being displayed - at
 * most `maxBranchesPerCluster` per cluster, rather than one per ref in the repository.
 */
export function branchFacts(ref, prs, base) {
    const pr = prs.get(ref)
    if (pr) return { ref, pr, theme: pr.title, themeFrom: 'pr-title' }

    const onBase = baselineNotePaths(base)
    const own = treeEntries(ref, NOTES_DIR).entries
        .filter((e) => !onBase.has(e.path))
        .sort((a, b) => a.path.localeCompare(b.path))
    for (const o of own) {
        const title = blobTitle(o.blob)
        if (title) return { ref, pr: null, theme: title, themeFrom: `note:${o.path}`, ownNotes: own.length }
    }
    return { ref, pr: null, theme: ref, themeFrom: 'branch-name' }
}

/** Fuzzy path lookup over every note that has ever existed on any ref. */
export function findNotes(index, query) {
    const needle = query.toLowerCase()
    const out = []
    for (const [path, versions] of index.byPath) {
        if (!path.toLowerCase().includes(needle)) continue
        const refs = new Set()
        for (const rs of versions.values()) for (const r of rs) refs.add(r)
        out.push({
            path,
            onBaseline: index.basePaths.has(path),
            refCount: refs.size,
            versionCount: versions.size,
        })
    }
    return out.sort((a, b) => Number(b.onBaseline) - Number(a.onBaseline) || b.refCount - a.refCount)
}

/**
 * How one note differs across every branch tip - DIVERGENCE ONLY, by default.
 *
 * TAKES A PATH, NOT THE CORPUS. This is a question about ONE file, and answering it used to mean
 * building an index of every note on every ref - 436 `ls-tree` forks - which then had to be cached
 * to be usable. Asking git the narrow question instead is 60ms cold: one `cat-file --batch-check`
 * over `<ref>:<path>` for the versions, one `rev-list` plus one `cat-file` for the history.
 *
 * Clustered by blob, so the diff runs once per DISTINCT VERSION rather than once per ref: 37 rather
 * than 274 for the fork's most-edited note.
 *
 * Then split. A cluster whose blob the baseline once held is a branch that has simply not merged
 * recently - it is behind, it gets further behind every time anyone edits the note, and nobody needs
 * to be told. What is reported is content that exists on a branch and has never existed on the
 * baseline, because that is what is at risk of being lost.
 */
export function drift(path, { prs = new Map(), maxBranchesPerCluster = 6, all = false } = {}) {
    const base = baseline()
    const { ok, tips } = refTips()
    if (!ok) return { path, ok: false, reason: 'cannot list refs - is this a git repository?' }
    // The guard corpusIndex has and this did not: zero refs fell through to an empty blobsForPath
    // and rendered as a confident "no note at that path on any ref".
    if (tips.length === 0) return { path, ok: false, reason: 'no branch refs found - nothing to search' }
    if (!base) return { path, ok: false, reason: 'neither origin/master nor master resolves - no baseline' }
    const refs = tips.map((r) => r.ref)
    const lookup = blobsForPath(refs, path)
    if (!lookup.ok) return { path, ok: false, reason: `cannot read ${path} across refs - the object lookup failed` }
    const blobs = lookup.blobs
    if (blobs.size === 0) return { path, ok: true, found: false }

    const versions = new Map()
    for (const [ref, blob] of blobs) {
        if (!versions.has(blob)) versions.set(blob, [])
        versions.get(blob).push(ref)
    }

    const history = baselineHistoryBlobs(base, path)
    const baseBlob = blobs.get(base) ?? null

    const build = ([blob, refs]) => {
        const sorted = [...refs].sort()
        return {
            blob,
            refs: sorted,
            isBaseline: blob === baseBlob,
            // Against the merge-base of the first carrying ref, so the number is what this branch
            // ADDED rather than how far the baseline has moved since.
            added: blob === baseBlob ? null : addedSinceMergeBase(base, sorted[0], path, blob),
            title: blobTitle(blob),
            branches: sorted.slice(0, maxBranchesPerCluster).map((r) => branchFacts(r, prs, base)),
        }
    }

    const divergent = []
    const behind = []
    for (const entry of versions.entries()) {
        const [blob, refs] = entry
        if (blob === baseBlob) continue
        if (history.has(blob)) behind.push({ blob, refs })
        else divergent.push(entry)
    }

    return {
        path, ok: true, found: true, baseline: base, onBaseline: baseBlob !== null,
        refsCarrying: blobs.size,
        refsTotal: refs.length,
        baselineCluster: baseBlob ? build([baseBlob, versions.get(baseBlob)]) : null,
        divergent: divergent.map(build).sort((a, b) => b.refs.length - a.refs.length),
        behind: {
            versions: behind.length,
            refs: behind.reduce((n, b) => n + b.refs.length, 0),
            clusters: all ? behind.map((b) => build([b.blob, b.refs])) : [],
        },
    }
}

/**
 * Notes that exist on some ref and never reached the baseline - the `stranded-work` impact, detected.
 *
 * THREE FILTERS, in increasing order of what they actually remove. The middle one is here because it
 * was predicted to do most of the work and did almost none, which is worth stating rather than
 * quietly deleting:
 *
 *   1. present on the baseline now      - not stranded, obviously
 *   2. its blob lives on the baseline
 *      under another path               - a rename, proven exactly. Removed 1 of 405.
 *   3. the baseline's HISTORY once had
 *      this path                        - it landed and was `git rm`d when its work closed.
 *                                         Removed 40 more.
 *
 * CLUSTERED BY REF-SET, not listed per path. Hundreds survive the filters, and the head of that
 * list is one workstream's notes sharing one set of refs. An identical
 * ref-set is one event in the repository's history; printing its members separately buries the
 * finding under its own volume, which is the lesson `prior-art --by-ref` already paid for.
 *
 * @returns {{refs: string[], paths: string[], refCount: number}[]} largest cluster first
 */
export function stranded(index) {
    // PER VERSION, NOT PER PATH - and getting this wrong was a real bug, not merely an untested one.
    // Both filters were `versions.some(...)`, so ONE version being finished work excluded the whole
    // path: a branch carrying genuinely new content at a recycled filename was swallowed by another
    // branch still carrying the old, closed content at that same path. That is the exact collision
    // the blob-aware filter was added to catch, reintroduced one level up. A path is stranded when
    // ANY of its versions is content the baseline has never held, and the refs reported are only
    // those carrying such a version.
    //
    // Why the path filter is not enough on its own: filenames get reused. Once master closes and
    // `git rm`s a note, a DIFFERENT note later created at the same path on a branch was silently
    // dropped - the tool that exists to surface work that will be lost, losing it. The short
    // `<category>-<slug>` names make that collision ordinary rather than exotic.
    const survivors = []
    for (const [path, versions] of index.byPath) {
        if (index.basePaths.has(path)) continue
        const heldHere = index.baseEverPaths.has(path)
            ? baselineHistoryBlobs(index.baseline, path)
            : new Set()

        const refs = new Set()
        for (const [blob, carrying] of versions) {
            // This version reached the baseline under another name - a rename, proven exactly.
            if (index.baseBlobs.has(blob)
                && (index.blobPaths.get(blob) ?? []).some((p) => index.basePaths.has(p))) continue
            // The baseline itself once held THIS content at THIS path, then removed it: finished.
            if (heldHere.has(blob)) continue
            for (const r of carrying) refs.add(r)
        }
        if (refs.size === 0) continue
        survivors.push({ path, refs: [...refs].sort() })
    }

    const byKey = new Map()
    for (const s of survivors) {
        const key = s.refs.join(' ')
        if (!byKey.has(key)) byKey.set(key, { refs: s.refs, refCount: s.refs.length, paths: [] })
        byKey.get(key).paths.push(s.path)
    }
    return [...byKey.values()].sort((a, b) => b.paths.length - a.paths.length || b.refCount - a.refCount)
}

/**
 * Fold ONE pull request into the cached set, without refetching the other 284.
 *
 * The reason the TTL could go from thirty minutes to a day: the cache no longer relies on expiring
 * to become correct. A PR created here updates it as it is created, so the window in which the cache
 * can be wrong about our own work is the length of one `gh pr view` rather than the TTL.
 *
 * Returns `{ok, action, pr}` - `added` or `updated` - or `ok: false` with a reason. A refresh that
 * silently did nothing would leave the caller believing the cache is current when it is not, which
 * is the same shape as every other silent miss this tool exists to remove.
 */
export function cachePr(number) {
    const res = exec('gh', ['pr', 'view', String(number), '-R', REPO,
        '--json', 'headRefName,baseRefName,number,title,state'])
    if (!res.ok) return { ok: false, reason: `gh could not read PR #${number}` }
    let row
    try { row = JSON.parse(res.out) } catch { return { ok: false, reason: 'gh returned output that is not JSON' } }
    if (!row?.headRefName) return { ok: false, reason: `PR #${number} has no head branch` }

    const shape = 'headRefName,baseRefName,number,title,state'
    const existing = cacheRead('prs.json', { key: shape, maxAgeMs: PR_CACHE_TTL_MS }) ?? []
    const pairs = existing.filter(([head]) => head !== row.headRefName)
    const action = pairs.length === existing.length ? 'added' : 'updated'
    pairs.push([row.headRefName, {
        number: row.number, title: row.title, state: row.state, baseRefName: row.baseRefName,
    }])
    cacheWrite('prs.json', pairs, shape)
    return { ok: true, action, pr: row, total: pairs.length }
}
