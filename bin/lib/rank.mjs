// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE BACKLOG VIEW: open in-flight notes across every ref, grouped the way the session index groups
// them, and the places that picture disagrees with the standing ranking.
//
// WHAT THIS ADDS, AND WHAT IT DELIBERATELY DOES NOT REBUILD. `bin/inflight.mjs docs list inflight
// <impact>` already reads every ref, keeps open notes, groups them by impact, scopes to one bucket
// and marks an off-baseline note with the branch it was read from. None of that is re-derived here:
// `corpusIndex` walks, `classifyNote` reads the markers, `inflightGroupOf` places. What is new is
// the carrying branch's pull request, the live-versus-archival split, the filename number, the
// delta against `docs/inflight/process-candidate-ranking.md`, and an accounting of the open notes no
// impact bucket claimed.
//
// THE GROUP RULE IS NOT COPIED, IT IS IMPORTED. Two surfaces that place the same note differently
// would be worse than one surface - a reader would have to know which to believe, and nothing would
// report the disagreement. `inflightGroupOf` is the one owner.
//
// IT NEVER SAYS A BRANCH FIXES A NOTE. A note travels on the branch that produced it, so carriage is
// cheap to know and ownership is not available at all. The worked case is a data-loss note carried
// by exactly one branch whose own text says the bug predates that branch's pull request - a row
// reading "fixed by" that pull request would be confidently wrong, which is the failure this whole
// command is organised against. Two relations are printed, both provable; `fixes` is not one of them.
//
// CARRIAGE IS INFORMATIVE ONLY OFF THE BASELINE, and the row says which case it is in. Every branch
// cut from the baseline carries a baseline note, so its carrying-ref list is evidence of nothing;
// for a branch-only note the same field is the most useful thing in the row.
//
// THE BULK PR SNAPSHOT ONLY - NO PER-BRANCH FALL-THROUGH. `branchView` falls through to
// `prForBranch` on a miss because it answers about ONE branch. This answers about many: the notes
// area's off-baseline paths spread across a few hundred distinct carrying refs, well over half of
// them absent from the bulk map, `bin/lib/cache.mjs` deliberately does not cache an absence for that
// kind, and `prForBranch` passes no timeout - so the same fall-through here is one fresh, untimed
// `gh` subprocess per pull-request-less branch, on every run. The snapshot plus the command that
// answers exactly for one branch is the bounded and honest version.
//
// No process.exit, no printing: bin/inflight.mjs owns the process boundary, and bin/lib/views.mjs
// renders. `rank` is pure over its inputs so bin/test-inflight.mjs can drive it on a fixture with no
// network; `registerBlob` is the thin git wrapper held apart from it for exactly that reason.

import { blobContents, blobsForPath, refKind } from './git.mjs'
import { INFLIGHT_IMPACT_ORDER, classifyNote } from './inflight-tags.mjs'
import { NOTES_DIR, REPO } from './repo.mjs'
import { inflightGroupOf } from './docs-shape.mjs'

/** The standing ruling this command diffs against. Read, never written. */
export const REGISTER_PATH = `${NOTES_DIR}/process-candidate-ranking.md`

/**
 * The groups that carry work to rank, in the order they are emitted.
 *
 * The impact scale first, because that is this repository's priority order and the outer sort is
 * never anything else. Then `feature` and `unmatched` - open notes the impact scale could not
 * place, which are the largest part of the corpus and would otherwise vanish from the one view
 * whose whole subject is open work. A missing or misspelt tag has to read as a finding, not as an
 * absence.
 */
export const RANKED_GROUPS = [...INFLIGHT_IMPACT_ORDER, 'feature', 'unmatched']

/**
 * Groups that are not work waiting to be done, so they are counted rather than listed.
 *
 * `registers` are consulted and never completed; `closed` and `deferred` carry a state marker. The
 * counts still appear, because an enumeration that does not say what it left out is a false negative
 * wearing the authority of a completed check.
 */
const NOT_RANKABLE = new Set(['closed', 'deferred', 'registers'])

/** A short label per group, for the view. Impact groups are their own name. */
const GROUP_LABELS = {
    feature: 'feature - proposed, no consequence attached',
    unmatched: 'unmatched - inflight-type or inflight-impact missing or misspelt',
    registers: 'registers - consulted, never completed',
    closed: 'closed or blocked - carries a state that is neither deferred nor parked',
    deferred: 'deferred - decided, not now',
}

/**
 * The number a note's filename carries in the `<area>-<NNN>-<slug>` position, or null.
 *
 * POSITION ONLY, never a bare digit scan: `release-0600-blockers.md` and
 * `ci-broker-container-exit-126-is-undiagnosable.md` both contain digits that are not identifiers,
 * and a scan that picked them up would hand a reader a command resolving to an unrelated issue.
 */
function positionalNumber(path) {
    const m = /^[a-z]+-(\d+)-/.exec(path.slice(NOTES_DIR.length + 1))
    return m ? Number(m[1]) : null
}

/**
 * What a filename's number is, and the command that looks it up - without claiming a repository.
 *
 * `docs/inflight/AGENTS.md` states the convention and its exceptions in the same breath: the number
 * is normally this fork's issue, `pr-` is the deliberate exception whose number is a fork PULL
 * REQUEST, and names predating the convention carry confluentinc numbers. Only the `pr-` case is
 * mechanically distinguishable, so it is the only one asserted; the rest print the fork-qualified
 * command and the view carries the caveat once. AGENTS.md's own rule is that a wrong reference
 * which resolves is worse than a broken one, so nothing here says which repository owns a number.
 */
function numberFor(path) {
    const numbered = positionalNumber(path)
    if (numbered === null) return null
    const isPullRequest = path.startsWith(`${NOTES_DIR}/pr-`)
    return {
        value: numbered,
        kind: isPullRequest ? 'pull-request' : 'issue',
        command: isPullRequest
            ? `gh pr view ${numbered} -R ${REPO}`
            : `gh issue view ${numbered} -R ${REPO}`,
    }
}

/**
 * The register's text from the BASELINE's blob - not the working tree.
 *
 * The whole tool exists because the checked-out copy is one version among many, and the register
 * itself has divergent versions on other refs; reading the file beside you would answer for whatever
 * branch you happen to be on. Held apart from `rank` so the analysis stays pure over its inputs and
 * the self-test can drive it with no git at all.
 */
export function registerBlob(index) {
    if (!index.ok) return { ok: false, reason: 'no corpus index, so no baseline to read the register from' }
    const found = blobsForPath([index.baseline], REGISTER_PATH)
    if (!found.ok) return { ok: false, reason: `git could not resolve ${REGISTER_PATH} on ${index.baseline}` }
    const blob = found.blobs.get(index.baseline)
    if (!blob) return { ok: false, reason: `${REGISTER_PATH} is not on ${index.baseline}` }
    const read = blobContents([blob])
    if (!read.ok) return { ok: false, reason: `git could not read ${REGISTER_PATH}'s blob` }
    return { ok: true, text: read.contents.get(blob) ?? '' }
}

/**
 * What the register NAMES, in the two literal forms it uses - and nothing inferred beyond them.
 *
 * Filenames, because the ready-picks half cites notes by name; and `astubbs#<number>`, because the
 * ranked half leads every line with a number rather than a filename. A delta keyed on filenames
 * alone would report the notes the register actually ranks as unranked - the finding firing hardest
 * exactly where the register is doing its job.
 *
 * A register entry named only in prose is out of the parse's reach. That is stated rather than
 * guessed at: a heuristic that matched prose would silently claim coverage it does not have.
 */
export function parseRegister(text) {
    const names = new Set((text.match(/[a-z][a-z0-9]*-[a-z0-9-]+\.md/g) ?? []))
    const numbers = new Set((text.match(/astubbs#(\d+)/g) ?? []).map((m) => Number(m.slice('astubbs#'.length))))
    return { names, numbers }
}

/**
 * The backlog view.
 *
 * Pure over its inputs: the corpus index, the bulk PR snapshot, and the register's text all arrive
 * as arguments, so the self-test drives the real logic against a fixture and never the network.
 *
 * @param {object} index a `corpusIndex` result, scoped to the notes area
 * @param {{prs: {ok: boolean, map: Map, reason?: string}, register: {ok: boolean, text?: string, reason?: string},
 *          group?: string|null, snapshotAgeMs?: number|null}} opts
 */
export function rank(index, { prs, register, group = null, snapshotAgeMs = null }) {
    // A FAILED WALK IS NOT AN EMPTY BACKLOG. Two P0s found while building this front door were both
    // a failure rendering as a confident empty result, which is why exit 0 and exit 2 differ.
    if (!index.ok) return { ok: false, reason: index.reason ?? 'the corpus index did not build' }

    const archival = new Map(index.refs.map((r) => [r.ref, refKind(r.full).archival]))

    // PASS ONE: EVERY VERSION OF EVERY NOTE, not one per path.
    //
    // Two branches can disagree about whether a note is open - one fixes the bug and closes it while
    // another still carries it live - and reading a single arbitrary version answers for whichever
    // ref happened to sort first. That is not a hypothetical: the data-loss note this command was
    // built around is open on the branch that owns the bug and closed on the branch that fixed
    // something adjacent, and reading only the first sorted ref dropped it from the backlog entirely.
    //
    // `docs/inflight/ci-inflight-next-commands.md` states the axis: flow with git, do not suppress
    // it. A single status is a summary that has thrown away the shape of the disagreement, so the
    // note is placed by a version that is still open work and the row NAMES the refs that disagree.
    const plan = []
    for (const [path, versions] of index.byPath) {
        const carrying = new Set()
        for (const refs of versions.values()) for (const r of refs) carrying.add(r)
        const all = [...carrying].sort()
        const live = all.filter((r) => !archival.get(r))
        // A PRESERVED CLUSTER HAS NO LIVE REF TO BE READ FROM. `stranded` marks it preserved exactly
        // when `liveRefs` is empty and `docsShape` then drops the path from the corpus entirely, so
        // the first-sorted-LIVE-ref rule is undefined here. This is where rank departs from it.
        const readable = live.length > 0 ? live : all
        plan.push({
            path,
            all,
            live,
            readable,
            onBaseline: index.basePaths.has(path),
            versions: [...versions].map(([blob, refs]) => ({ blob, refs: refs.slice().sort() })),
        })
    }

    // ONE BATCH, not one `cat-file` per version. `blobContents` takes an array and `docsShape` is
    // the worked example; a call per blob is thousands of subprocesses on this repository.
    const read = blobContents([...new Set(plan.flatMap((p) => p.versions.map((v) => v.blob)))])
    if (!read.ok) return { ok: false, reason: 'git could not read the note blobs' }

    const buckets = new Map(RANKED_GROUPS.map((k) => [k, []]))
    const excluded = new Map()
    const byName = new Map()
    const byNumber = new Map()

    for (const { path, all, live, readable, onBaseline, versions } of plan) {
        const seen = versions
            .map((v) => {
                const text = read.contents.get(v.blob)
                if (text === undefined) return null
                const note = classifyNote(text, path)
                return { ...v, note, group: inflightGroupOf(note) }
            })
            .filter(Boolean)
        if (seen.length === 0) continue

        // The version that decides the row: the baseline's own when the note is on the baseline;
        // otherwise the first still-rankable one in sorted-ref order, so a branch that closed its
        // copy cannot delete the note from a backlog another branch still has open.
        const rank_ = (v) => Math.min(...v.refs.map((r) => readable.indexOf(r)).filter((i) => i >= 0).concat([Number.MAX_SAFE_INTEGER]))
        const ordered = seen.slice().sort((a, b) => rank_(a) - rank_(b))
        const chosen = (onBaseline ? ordered.find((v) => v.refs.includes(index.baseline)) : null)
            ?? ordered.find((v) => RANKED_GROUPS.includes(v.group))
            ?? ordered[0]

        const note = chosen.note
        const key = chosen.group
        const name = path.slice(NOTES_DIR.length + 1)
        byName.set(name, { path, group: key, note })
        const numbered = numberFor(path)
        if (numbered && numbered.kind === 'issue') {
            if (!byNumber.has(numbered.value)) byNumber.set(numbered.value, [])
            byNumber.get(numbered.value).push(name)
        }

        if (NOT_RANKABLE.has(key)) {
            excluded.set(key, (excluded.get(key) ?? 0) + 1)
            continue
        }

        // What the OTHER versions say, so one status never hides a disagreement between branches.
        const disagreement = [...new Set(seen.map((v) => v.group))]
            .filter((g) => g !== key)
            .map((g) => ({ group: g, ref: seen.find((v) => v.group === g).refs[0] }))

        // NO `?? chosen.refs[0]` FALLBACK. `readable` is non-empty by construction - it is the live
        // refs, or every ref when none is live - and a fallback here silently did the archival
        // rule's job, so removing it is what leaves that rule with a control that can fail.
        const readRef = onBaseline && chosen.refs.includes(index.baseline)
            ? index.baseline
            : chosen.refs.find((r) => readable.includes(r))

        buckets.get(key).push({
            path,
            name,
            title: note.title,
            group: key,
            impact: note.impact,
            onBaseline,
            // Carriage, never ownership - the one thing this command refuses to infer.
            relation: 'carries',
            carryingRefs: onBaseline ? [] : all,
            readRef,
            preserved: live.length === 0,
            disagreement,
            number: numbered,
            // KEYED ON THE BARE BRANCH NAME. `prsByBranch` maps `headRefName`, which never carries
            // the `origin/` prefix, while a corpus ref almost always does - so looking up the full
            // ref returned null for every remote branch and made the whole corpus read as
            // pull-request-less. `prForBranch` strips it for the same reason.
            pr: prs.ok ? (prs.map.get(readRef.replace(/^origin\//, '')) ?? null) : null,
            // UNANSWERED IS NOT ABSENT. An unauthenticated or rate-limited `gh` reads exactly like a
            // branch with no pull request unless the shape carries the difference.
            prKnown: prs.ok === true,
        })
    }

    for (const rows of buckets.values()) rows.sort((a, b) => a.path.localeCompare(b.path))

    const groups = RANKED_GROUPS
        .filter((k) => buckets.get(k).length > 0)
        .filter((k) => group === null || k === group)
        .map((k) => ({ key: k, label: GROUP_LABELS[k] ?? k, rows: buckets.get(k) }))

    return {
        ok: true,
        baseline: index.baseline,
        refsTotal: index.refs.length,
        liveRefs: index.refs.filter((r) => !refKind(r.full).archival).length,
        groups,
        groupNames: RANKED_GROUPS.filter((k) => buckets.get(k).length > 0)
            .map((k) => ({ key: k, count: buckets.get(k).length })),
        excluded: [...excluded].map(([key, count]) => ({ key, count, label: GROUP_LABELS[key] ?? key })),
        prsOk: prs.ok === true,
        prsReason: prs.ok ? null : (prs.reason ?? 'the pull-request snapshot did not answer'),
        snapshotAgeMs,
        scoped: group,
        delta: delta(register, { byName, byNumber, buckets, group }),
    }
}

/**
 * The register delta - the deliverable.
 *
 * Two halves, and they are asymmetric on purpose. What the register ranks is a handful of entries,
 * so every one of them is listed with the reason it needs attention. What the register does NOT name
 * is nearly every open note in the repository, so listing it unscoped would be the whole-corpus dump
 * this command exists to avoid, arriving under the name "delta" - it is a count per group until a
 * group scopes the call.
 */
function delta(register, { byName, byNumber, buckets, group }) {
    // A REGISTER THAT COULD NOT BE READ IS A FAILED RUN, not an empty delta. The caller emits
    // everything that did run and then fails, the way `refactor-window` reports an unmeasurable
    // candidate.
    if (register.ok !== true) {
        return { ok: false, reason: register.reason ?? 'the register could not be read', ranked: [], byNumber: [], unranked: [], unrankedCounts: [], unresolvable: [] }
    }
    const { names, numbers } = parseRegister(register.text ?? '')

    // A NUMBER CAN RESOLVE TO MORE THAN ONE NOTE, and picking one silently is a confidently wrong
    // answer. Filenames get recycled and renamed here - a note and its own renamed predecessor sit
    // on different refs carrying the same positional number - so a map keeping the last writer
    // reported the register's live entry as stale, naming the dead copy. The register's entry is
    // SATISFIED when any candidate is open work in an impact bucket; only when none is does it
    // become a finding, and then it names every candidate rather than choosing between them.
    const rankedNames = new Set(names)
    const unresolvable = []
    const byNumberRows = []
    for (const n of [...numbers].sort((a, b) => a - b)) {
        const candidates = byNumber.get(n) ?? []
        if (candidates.length === 0) { unresolvable.push(n); continue }
        for (const nm of candidates) rankedNames.add(nm)
        const satisfied = candidates.some((nm) => INFLIGHT_IMPACT_ORDER.includes(byName.get(nm)?.group))
        if (!satisfied) {
            byNumberRows.push({
                number: n,
                names: candidates.slice().sort(),
                reason: candidates.length > 1
                    ? 'several notes carry this number, and none is open work in an impact bucket'
                    : (byName.get(candidates[0])?.group ?? 'absent'),
            })
        }
    }

    // What the register ranks that is not open work waiting in an impact bucket. The REASON is the
    // finding: gone needs deleting from the register, deferred needs a schedule decision, and one
    // no impact bucket claims needs a tag. Reporting all three as "not open" would turn three
    // different actions into one shrug.
    // FILENAMES ONLY. `rankedNames` also holds every note a register NUMBER resolved to, and those
    // are reported by `byNumberRows` with the collision handling above - iterating the union here
    // reported a number's stale twin a second time, as though the register had named it by name.
    const ranked = []
    for (const name of [...names].sort()) {
        const hit = byName.get(name)
        if (!hit) { ranked.push({ name, reason: 'absent' }); continue }
        if (RANKED_GROUPS.includes(hit.group) && hit.group !== 'feature' && hit.group !== 'unmatched') continue
        ranked.push({ name, reason: hit.group, path: hit.path })
    }

    const listUnranked = group !== null
    const unranked = []
    const unrankedCounts = []
    for (const [key, rows] of buckets) {
        if (group !== null && key !== group) continue
        const missing = rows.filter((row) => !rankedNames.has(row.name))
        if (missing.length === 0) continue
        unrankedCounts.push({ key, count: missing.length })
        if (listUnranked) unranked.push(...missing.map((row) => ({ path: row.path, name: row.name, group: key, title: row.title })))
    }

    return { ok: register.ok === true, reason: null, ranked, byNumber: byNumberRows, unranked, unrankedCounts, unresolvable: unresolvable.sort((a, b) => a - b) }
}
