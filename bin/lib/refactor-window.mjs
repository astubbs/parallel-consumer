// Copyright (C) 2026 Antony Stubbs and contributors
//
// IS NOW A GOOD TIME to decompose one of the files this repo has already decided to decompose?
//
// docs/refactoring.md says its entries are to be picked up "when things are quiet". Nothing could
// evaluate that, so the entries aged instead: AbstractParallelEoSStreamProcessor is recorded there
// at 1533 lines and is now 2405. Every author who touched it faced the same undecidable question
// and made the same locally correct choice to add rather than extract.
//
// THE SIGNAL IS THE LARGEST SINGLE DIVERGENCE, NOT THE NUMBER OF BRANCHES. That is the whole
// design and it was chosen against a measurement rather than a hunch: on 2026-09-02 PartitionState
// had dozens of live branches with an open PR diverging from it and the largest of those
// divergences was EIGHT LINES. A count would have called that file blocked with nothing in its
// way. What actually makes a decomposition expensive is one branch that has rewritten half the
// file, so the maximum is the thing to measure - and it is also the thing to report, because the
// operator's alternative to waiting is to go and land that one branch.
//
// A max is immune to two things a count is not, which is a bonus rather than the reason: local and
// remote copies of one branch (`feats/x` and `origin/feats/x`) count twice, and a stack of seven
// branches sharing a base all report the same divergence. Neither moves a maximum.
//
// NOTHING IS REMEMBERED. There is no stored verdict and no comparison against what the last run
// said - a stored answer is a second thing that can be wrong, and repeating is the point. The
// operator asked for a signal that keeps saying so until the work is done or the entry is removed.
// The bulk PR listing IS cached (bin/lib/cache.mjs, 24h) but that is a cache of an INPUT: the
// divergence numbers and the open-or-not verdict are always fresh from the refs. The consequence
// worth knowing is that the PR label can lag by a day, so a just-merged blocker can still be
// named as open.
//
// FAILURE IS PER CANDIDATE. One candidate whose git query fails marks that candidate and leaves
// the others reported. An all-or-nothing flag would let one bad path silence three good answers,
// and the silence would be indistinguishable from "nothing is open" - which is the failure this
// whole feature exists to avoid, reproduced inside it.
//
// RETURNS FINDINGS, DECIDES NOTHING. No exit codes, no output, no process.exit - bin/inflight.mjs
// owns those, and bin/lib/views.mjs renders. Self-test: bin/test-inflight.mjs.

import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { baseline, blobDiffStat, blobsForPath, exec, mergeBaseBlobs, mergeBases, refTips } from './git.mjs'
import { prsByBranch } from './notes.mjs'

/** The shipped list. Overridable so the self-test can point at a fixture. */
export const DEFAULT_CONFIG = join(dirname(fileURLToPath(import.meta.url)), '..', 'refactor-candidates.json')

/**
 * The candidate list, or a REASON - never a throw and never a silent default.
 *
 * A malformed config must not read as "no candidates". That is the same silence a clean run
 * produces when nothing is open, and the two mean opposite things: one says go and refactor, the
 * other says this tool is broken. Every caller here distinguishes them, so this returns the
 * distinction rather than an empty list.
 *
 * STRICT ON ENTRIES, LENIENT ON THE TOP LEVEL. `about` and `threshold_note` exist so the file
 * explains itself - JSON has no comments - and more may be added. An ENTRY missing a field is a
 * different thing: it would silently drop a candidate or default its threshold, so it is refused
 * by name.
 *
 * @returns {{ok: true, candidates: Array<{id: string, paths: string[], threshold: number, hint: string}>}
 *          | {ok: false, reason: string}}
 */
export function loadCandidates(path = DEFAULT_CONFIG) {
    let raw
    try {
        raw = readFileSync(path, 'utf8')
    } catch (e) {
        return { ok: false, reason: `cannot read ${path}: ${e.code || e.message}` }
    }
    let doc
    try {
        doc = JSON.parse(raw)
    } catch (e) {
        return { ok: false, reason: `${path} is not valid JSON: ${e.message}` }
    }
    if (!doc || !Array.isArray(doc.candidates)) return { ok: false, reason: `${path} has no 'candidates' array` }

    const candidates = []
    for (const [i, c] of doc.candidates.entries()) {
        const where = c && typeof c.id === 'string' && c.id ? `candidate '${c.id}'` : `candidate #${i + 1}`
        if (!c || typeof c !== 'object') return { ok: false, reason: `${where} is not an object` }
        if (typeof c.id !== 'string' || !c.id) return { ok: false, reason: `${where} has no 'id'` }
        // A BARE STRING IS REFUSED RATHER THAN WRAPPED. Accepting it would work today and lose the
        // second path the day somebody adds one by editing the string in place - which across this
        // fork's in-flight package rename is the regression the paths list exists to prevent.
        if (!Array.isArray(c.paths) || c.paths.length === 0 || c.paths.some((p) => typeof p !== 'string' || !p)) {
            return { ok: false, reason: `${where} needs a non-empty 'paths' array of strings` }
        }
        if (typeof c.threshold !== 'number' || !Number.isFinite(c.threshold) || c.threshold < 0) {
            return { ok: false, reason: `${where} needs a non-negative numeric 'threshold'` }
        }
        if (typeof c.hint !== 'string' || !c.hint) return { ok: false, reason: `${where} needs a 'hint'` }
        candidates.push({ id: c.id, paths: [...c.paths], threshold: c.threshold, hint: c.hint })
    }
    return { ok: true, candidates }
}

/**
 * The largest divergence any live branch holds against the baseline, per candidate.
 *
 * `prs` is injectable because the self-test forbids network access - the same reason
 * `drift` in bin/lib/notes.mjs takes one. Pass `{ok, map}` in the shape `prsByBranch` returns, so
 * an injected map cannot claim more certainty than a real one.
 *
 * @returns {{ok: false, reason: string} | {ok: true, baseline: string, liveRefs: number,
 *           prsKnown: boolean, prsReason: string|null, candidates: object[]}}
 */
export function refactorWindow({ configPath = DEFAULT_CONFIG, prs = null } = {}) {
    const config = loadCandidates(configPath)
    if (!config.ok) return config

    const tips = refTips()
    if (!tips.ok) return { ok: false, reason: 'cannot list refs - not a git repository?' }
    // ARCHIVAL REFS ARE OUT. refKind already draws this line for the corpus commands, and it is the
    // line that matters here too: a tag or a refs/backup/** copy is preserved work, not a branch
    // somebody is about to merge, so a big divergence parked in one must not close the window.
    const live = tips.tips.filter((t) => !t.archival).map((t) => t.ref)

    // REFUSE THE RUN RATHER THAN ANSWERING IT. `baseline()` returns null in a shallow or single-ref
    // clone and its own comment states the contract this honours: "Returning null forces the caller
    // to say so instead of answering confidently and wrongly." Unchecked, every `git merge-base null
    // <ref>` failed, no ref was measurable, `largest` stayed null for every candidate - and the
    // verdict came back OPEN for all of them. Both hooks would then have told every session that
    // all four candidates were cheap to decompose, from a measurement that never ran. That is this
    // feature's own worst case, inverted from a silence into a confident go.
    const base = baseline()
    if (!base) {
        return { ok: false, reason: 'neither origin/master nor master resolves in this checkout - no baseline to measure against; try: git fetch origin master' }
    }

    const pr = prs || prsByBranch()

    // Once for the whole run: every candidate asks the same question of the same refs.
    const mbByRef = mergeBases(base, live)

    const candidates = config.candidates.map((c) => measure(c, live, base, pr, mbByRef))
    return {
        ok: true,
        baseline: base,
        liveRefs: live.length,
        prsKnown: pr.ok,
        prsReason: pr.ok ? null : pr.reason,
        candidates,
    }
}

/**
 * WHY THIS IS NOT WRITTEN THE OBVIOUS WAY, which is `addedSinceMergeBase` once per (candidate,
 * path, ref). It was, and it took **11.2 seconds** for four candidates - not the 1.55s the plan
 * predicted, because that figure had been measured over only the refs carrying an open PR while the
 * requirement is every live branch. Nearly all of it was forking, in two layers:
 *
 *   - a `git merge-base` per call, asked up to eight times per ref for an answer that does not
 *     depend on the path being examined. Hoisting it to once per ref: 11.2s -> 7.2s.
 *   - a `git rev-parse` per (candidate, path, ref) to find the merge-base version of the file -
 *     about 3,500 forks. Batched through `blobsForPath` (now `mergeBaseBlobs` in git.mjs): 7.2s -> 3.4s.
 *   - `countLines` re-deriving a blob the caller already held, and re-reading it once per ref rather
 *     than once per distinct blob - 208 calls over 24 blobs. Memoised: 3.4s -> **2.3s**.
 *
 * The answers are byte-identical at every step; this is arrangement, not approximation. The one
 * exception was not an optimisation at all: `countLines` also had an off-by-one that reported 492
 * lines for a 491-line file, so two candidates' figures moved by one when it was fixed.
 *
 * What is left is 437 `git merge-base` calls, one per live ref, which does not batch. Measuring
 * against `origin/master` directly instead would remove them and cost about 0.3s total - and would
 * be wrong: it counts what master has gained since the branch forked as though the branch had
 * written it, so a stale branch that never touched the file would hold the window shut.
 * `docs/inflight-tool.md` states that distinction for `note drift`, which measures the same way.
 *
 * The one memo below keys on the PAIR of blobs, which is what the answer actually depends on: refs
 * sharing both a merge-base version and a tip version have the same divergence by definition, and
 * this repo's branch stacks produce that constantly - seven refs at +363/-27 on one file.
 *
 * Process-lifetime only, and that is all it may be: a blob SHA names immutable content, so the memo
 * cannot go stale within a run, and nothing is written to disk between runs. Remembering across
 * runs is the thing this tool is specified not to do.
 */
const diffMemo = new Map()

/**
 * Added lines between this ref's merge-base version of the candidate and `blob`.
 *
 * TAKES EVERY CONFIGURED PATH, not the one path the blob was found under, and that is the whole
 * correction. A candidate lists two paths because this fork's package rename is in flight - they are
 * TWO NAMES FOR ONE FILE, not two files. Resolving the merge-base version under only the path the
 * tip happens to use means a branch that has DONE the rename finds nothing at its merge-base, falls
 * to `countLines`, and scores the entire file as added.
 *
 * Measured on this repository when it did: AbstractParallelEoSStreamProcessor reported +1609 on a rename branch whose
 * real divergence is +92, and named that branch as the one to land - while the true largest, +1047
 * on another branch entirely, went unreported. Three of the four shipped candidates were wrong the
 * same way. The inflation is systematically upward, so it pins a window shut rather than opening one
 * falsely, which is why nothing looked broken.
 *
 * `countLines` survives for the case it was written for: a file that genuinely exists on this ref
 * and at none of the candidate's paths at the merge-base.
 */
function addedForRef(ref, blob, mbByRef, mbBlobsByPath) {
    if (!mbByRef.has(ref)) return null
    const mb = mbByRef.get(ref)
    let at = null
    for (const byMergeBase of mbBlobsByPath) {
        const hit = byMergeBase.get(mb)
        if (hit) { at = hit; break }
    }
    // No configured path resolves at the merge-base: the branch really did create this file.
    if (!at) return countLines(blob)
    if (at === blob) return 0
    const key = `${at} ${blob}`
    if (!diffMemo.has(key)) {
        const d = blobDiffStat(at, blob)
        diffMemo.set(key, d && !d.diffFailed && typeof d.added === 'number' ? d.added : null)
    }
    return diffMemo.get(key)
}

/**
 * IS THIS FILE GETTING BETTER OR WORSE? The window says whether now is a good moment; this says
 * whether the problem is growing while nobody takes the moment.
 *
 * DERIVED, NEVER STORED. The alternative considered was recording a snapshot per run into the config
 * so a series could be read back - rejected because the writer would be a hook firing on every
 * session start and every push, mutating a TRACKED file across a dozen worktrees, and because git
 * already holds every past length exactly. `bin/lib/cache.mjs` makes the same argument for the same
 * reason: git is already the store.
 *
 * THE ANCHOR IS A ROLLING WINDOW, not a recorded date, so there is nothing to keep current. That is
 * the whole point: `docs/refactoring.md` records AbstractParallelEoSStreamProcessor at 1533 lines and it is now 2405, and
 * nothing went red about the gap. A number that recomputes both ends cannot rot that way.
 *
 * EVERY CONFIGURED PATH IS TRIED at the historical commit, for the same reason the divergence
 * measurement tries them all - across the package rename the file lived under a different name six
 * months ago, and asking only today's name would report it as newly created.
 *
 * Answers null rather than guessing when the repository is younger than the window, or when the file
 * existed under none of its names then.
 */
const GROWTH_WINDOW_DAYS = 180

function growth(c, base) {
    const nowBlob = firstBlobAtAnyPath(base, c.paths)
    if (!nowBlob) return null
    const since = new Date(Date.now() - GROWTH_WINDOW_DAYS * 86400000).toISOString().slice(0, 10)
    const then = exec('git', ['rev-list', '-1', `--before=${since}`, base]).out.trim()
    if (!then) return null
    const thenBlob = firstBlobAtAnyPath(then, c.paths)
    const now = countLines(nowBlob)
    if (typeof now !== 'number') return null
    if (!thenBlob) return { days: GROWTH_WINDOW_DAYS, now, then: null, delta: null }
    const before = countLines(thenBlob)
    if (typeof before !== 'number') return null
    return { days: GROWTH_WINDOW_DAYS, now, then: before, delta: now - before }
}

/** The candidate's blob at `rev`, under whichever of its names existed there. */
function firstBlobAtAnyPath(rev, paths) {
    for (const path of paths) {
        // THE THIRD INSTANCE OF ONE DEFECT CLASS, found by the merge-prep sweep for others like it
        // rather than by a test. Dropping `ok` here would report a FAILED lookup as "the file did not
        // exist at that revision", which the view renders as "did not exist then" - a confident wrong
        // fact, in a line whose whole purpose is telling the operator whether the problem is growing.
        // Same shape as the two already fixed in notes.mjs and measure(); `null` means unanswerable
        // and the caller drops the growth line rather than inventing one.
        const found = blobsForPath([rev], path)
        if (!found.ok) return null
        const hit = found.blobs.get(rev)
        if (hit) return hit
    }
    return null
}

/** One candidate's answer, carrying its own ok/reason so a failure here cannot silence its peers. */
function measure(c, live, base, pr, mbByRef) {
    const out = {
        id: c.id, paths: c.paths, threshold: c.threshold, hint: c.hint,
        // The three ref counts are placeholders keeping the shape stable for a consumer; a failure
        // path below returns before they are real, and the view prints only `reason` in that case.
        ok: true, reason: null, largest: null, open: false,
        matchedRefs: 0, unmatchedRefs: live.length, unanswerableRefs: 0,
    }
    // Every path's merge-base version, resolved once for the candidate rather than per path, because
    // the question "what did this branch start from" is about the FILE and not about which of its
    // names the tip uses.
    const mbBlobsByPath = c.paths.map((p) => mergeBaseBlobs(mbByRef, p))
    if (mbBlobsByPath.some((m) => !m.ok)) {
        out.ok = false
        out.reason = `git could not resolve the merge-base version of ${c.id}`
        return out
    }

    const matched = new Set()
    for (const [i, path] of c.paths.entries()) {
        // CHECKED, like its neighbour. This was the one git result in this function whose `ok` was
        // dropped - which AGENTS.md forbids by name, and which read as an oversight beside the
        // "COUNTED, NEVER JUST SKIPPED" rule three lines below. No wrong verdict follows from it (a
        // failed lookup leaves baseBlob undefined, the identical-to-baseline fast path stops firing,
        // and every ref falls through to a real diff that is still correct) - but "still correct by
        // accident" is exactly what the rule exists to stop being invisible.
        const baseFound = blobsForPath([base], path)
        const found = blobsForPath(live, path)
        if (!baseFound.ok || !found.ok) {
            out.ok = false
            out.reason = `git could not answer for ${path}`
            return out
        }
        const baseBlob = baseFound.blobs.get(base)
        for (const [ref, blob] of found.blobs) {
            matched.add(ref)
            // Identical to the baseline is not a divergence, and skipping it here is most of the
            // cost saved: on the largest candidate that is ~190 of ~440 carrying refs never diffed.
            if (blob === baseBlob) continue
            const added = addedForRef(ref, blob, mbByRef, mbBlobsByPath.map((m) => m.blobs))
            // COUNTED, NEVER JUST SKIPPED. A ref is added to `matched` before it is measured, so a
            // bare `continue` here put it in neither the maximum nor the unmatched count - a live
            // ref with an unrelated history carried a 400-line divergence and the report said there
            // was none. Being unable to measure a ref is a fact about the run, not an absence.
            if (typeof added !== 'number') { out.unanswerableRefs++; continue }
            if (!out.largest || added > out.largest.added) {
                const prRow = pr.map.get(ref.replace(/^origin\//, ''))
                out.largest = { added, ref, path: c.paths[i], pr: prRow ? { number: prRow.number, state: prRow.state } : null }
            }
        }
    }
    out.matchedRefs = matched.size
    out.growth = growth(c, base)
    // R15: a live branch carrying the candidate under NONE of the configured paths. Deliberately a
    // superset - it also counts branches that predate the file entirely - because the alternative
    // is deciding which absences are innocent, and the number exists to make a path the config was
    // never told about VISIBLE rather than to be exact.
    out.unmatchedRefs = live.length - matched.size

    // NO REF CARRIES IT AT ALL: the config has gone stale, which is not a quiet tree. The paths list
    // exists to survive the package rename, so the day a spelling is retired this is what happens -
    // and reporting it as OPEN would turn a stale entry into a standing instruction to refactor.
    if (matched.size === 0) {
        out.ok = false
        out.reason = `no live ref carries ${c.id} under any of its configured paths - has one been renamed?`
        return out
    }

    // A MEASUREMENT THAT FOUND NOTHING AND A MEASUREMENT THAT COULD NOT LOOK ARE DIFFERENT ANSWERS.
    // Without the second clause, a candidate whose every carrying ref was unanswerable reports
    // "no divergence, go ahead" - the false pass this whole feature exists to prevent.
    out.open = out.largest === null ? out.unanswerableRefs === 0 : out.largest.added <= c.threshold
    return out
}

/**
 * Line count of a file that exists only on this ref, so a branch-created copy is not scored as zero.
 *
 * `git diff --numstat` against the empty tree would be the tidy answer and is a second subprocess
 * shape for a number this only needs approximately; the blob is already addressable, so read it.
 *
 * TAKES THE BLOB, not a ref and a path. The caller already resolved it, and re-deriving it cost a
 * second `git cat-file --batch-check` fork per call for an answer sitting in the caller's hand.
 *
 * MEMOISED ON THE BLOB, on the same grounds as `diffMemo` and with the same safety: a blob SHA names
 * immutable content, so the memo cannot go stale within a run. Measured: 208 calls over 24 distinct
 * blobs, because branch stacks share content heavily - so seven of every eight reads were re-reading
 * something already counted. Removing them took roughly a second off a 3.4s run.
 *
 * COUNTS NEWLINE-TERMINATED LINES, which `split('\n').length` does not. A file ending in a newline -
 * essentially all of them - yields a trailing empty element, and the first cut of this reported 492
 * for a 491-line file. `git.mjs`'s `lines()` is not the fix either: it drops blank lines, which
 * undercounts real source instead. Both errors are silent, and both land on the branch-created path,
 * which under the in-flight package rename is exactly where the big divergences are.
 */
const lineCountMemo = new Map()
function countLines(blob) {
    if (!lineCountMemo.has(blob)) {
        const res = exec('git', ['cat-file', '-p', blob])
        let n = null
        if (res.ok) {
            n = res.out.length === 0 ? 0 : res.out.split('\n').length - (res.out.endsWith('\n') ? 1 : 0)
        }
        lineCountMemo.set(blob, n)
    }
    return lineCountMemo.get(blob)
}
