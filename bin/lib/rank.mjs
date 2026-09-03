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
import { DIRECTORY_DOCS_RE, DOCUMENT_RE, INFLIGHT_GROUPS, INFLIGHT_GROUP_ORDER, inflightGroupOf } from './docs-shape.mjs'

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
 *
 * DERIVED FROM THE OWNING TAXONOMY, not hand-listed beside it. These two sets have to PARTITION
 * everything `inflightGroupOf` can return: a group in neither made `buckets.get(key)` undefined and
 * `.push` throw, and the front door has no top-level try/catch, so the process exited 1 - neither of
 * its two documented codes, and a caller testing for 2 would read that as a successful run. Deriving
 * the complement means a group added to `docs-shape.mjs` lands here as not-rankable and is COUNTED,
 * which is the safe direction; the guard below covers anything outside the taxonomy entirely.
 */
const NOT_RANKABLE = new Set(INFLIGHT_GROUP_ORDER.filter((k) => !RANKED_GROUPS.includes(k)))

/**
 * The number a note's filename carries in the `<area>-<NNN>-<slug>` position, or null.
 *
 * POSITION ONLY, never a bare digit scan. `ci-broker-container-exit-126-is-undiagnosable.md` has
 * digits that are not an identifier, and several hyphenated segments precede them, so the position
 * rule excludes it - a scan that picked it up would hand a reader a command for an unrelated issue.
 *
 * NO LEADING ZERO, and that is not cosmetic: `release-0600-blockers.md` is on the baseline today and
 * `0600` is the release version 0.6.0.0, not an issue. The position rule alone matched it and would
 * have printed `gh issue view 600`, which resolves - and AGENTS.md's rule is that a wrong reference
 * which resolves is worse than a broken one. An issue number is never zero-padded, so the leading
 * zero is the exact discriminator.
 *
 * WHAT THIS STILL CANNOT TELL APART, stated rather than papered over:
 * `upstream-2023-sweep-manual-review.md` yields `2023`, which is a year. Nothing in the filename
 * distinguishes a year from an issue number, and inventing a plausible-range test would be a guess.
 * What bounds the damage is `numberFor`: the note's own text has to name that number qualified
 * before a single repository is claimed for it, and a year is named by nothing, so the row prints
 * both lookups and says it cannot attribute the number. If that ever stops being true, this is the
 * place.
 */
function positionalNumber(path) {
    const m = /^[a-z]+-([1-9]\d*)-/.exec(path.slice(NOTES_DIR.length + 1))
    return m ? Number(m[1]) : null
}

/**
 * What a filename's number is, which repository it belongs to when that is provable, and the
 * command(s) that look it up.
 *
 * `docs/inflight/AGENTS.md` states the convention and its exceptions in the same breath: the number
 * is normally this fork's issue, `pr-` is the deliberate exception whose number is a fork PULL
 * REQUEST, and names predating the convention carry confluentinc numbers. The filename alone cannot
 * separate the last case, so attribution comes from the note's own text and the row prints both
 * commands when the note does not say. Three outcomes: `fork`, `upstream`, `unknown`.
 */
function numberFor(path, text) {
    const numbered = positionalNumber(path)
    if (numbered === null) return null
    const isPullRequest = path.startsWith(`${NOTES_DIR}/pr-`)
    if (isPullRequest) {
        return { value: numbered, kind: 'pull-request', attribution: 'fork', commands: [`gh pr view ${numbered} -R ${REPO}`] }
    }
    // THE NOTE'S OWN TEXT IS THE ONLY THING THAT CAN ATTRIBUTE THE NUMBER. The filename cannot: the
    // convention says the number is this fork's, and `docs/inflight/AGENTS.md` names in the same
    // breath the pre-convention notes where it is confluentinc's - `bug-857-family.md` is on the
    // baseline today and its own title reads "The confluentinc#857 family". Printing the
    // fork-qualified command for that one is a reference that RESOLVES to the wrong thing the moment
    // this fork's counter passes 857, which AGENTS.md calls worse than a broken reference.
    //
    // So the row asks the note. A qualified mention of ITS OWN number attributes it; one qualified
    // both ways, or neither, is unattributable and gets both commands rather than a guess.
    const fork = new RegExp(`astubbs#${numbered}(?!\\d)`).test(text)
    const upstream = new RegExp(`confluentinc#${numbered}(?!\\d)`).test(text)
    const attribution = fork && !upstream ? 'fork' : (upstream && !fork ? 'upstream' : 'unknown')
    const forkCmd = `gh issue view ${numbered} -R ${REPO}`
    const upstreamCmd = `gh issue view ${numbered} -R confluentinc/parallel-consumer`
    return {
        value: numbered,
        kind: 'issue',
        attribution,
        commands: attribution === 'fork' ? [forkCmd] : (attribution === 'upstream' ? [upstreamCmd] : [forkCmd, upstreamCmd]),
    }
}

/**
 * Which version of a note decides its row.
 *
 * The baseline's own when the note is on the baseline; otherwise the first still-rankable version in
 * read order, so a branch that closed its copy cannot delete the note from a backlog another branch
 * still has open. Read order is the position of a version's earliest ref in `readable`, so "first"
 * means the ref the note would be read from rather than an arbitrary blob.
 *
 * Extracted because the three chained expressions computed one thing and the read-order helper had
 * to dodge this module's own exported `rank`; a name is better than a trailing underscore.
 */
function chooseVersion(seen, { onBaseline, readable, baseline }) {
    const readOrder = (v) => Math.min(
        ...v.refs.map((r) => readable.indexOf(r)).filter((i) => i >= 0).concat([Number.MAX_SAFE_INTEGER]),
    )
    const ordered = seen.slice().sort((a, b) => readOrder(a) - readOrder(b))
    // A VERSION THAT IS STILL OPEN WORK BEATS THE BASELINE'S, and the baseline only breaks ties
    // among those. Preferring the baseline unconditionally was the same defect as the
    // first-sorted-live-ref one, a layer up and worse: `core-auto-scaling.md` is deferred on the
    // baseline and OPEN with `inflight-impact: throughput` on a live branch, so the note vanished
    // from its group AND the delta told the register it was deferred - on the baseline's word, with
    // no ref named anywhere. Answering from the baseline while a live ref says otherwise is the
    // failure this whole command exists to prevent.
    const rankable = ordered.filter((v) => RANKED_GROUPS.includes(v.group))
    const pool = rankable.length > 0 ? rankable : ordered
    return (onBaseline ? pool.find((v) => v.refs.includes(baseline)) : null) ?? pool[0]
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
 * What the register NAMES, one ENTRY at a time - never one document at a time.
 *
 * THE WHOLE-DOCUMENT SCAN WAS WRONG IN BOTH DIRECTIONS, and both were live on the real register.
 * Matching `astubbs#<n>` anywhere turned continuation lines, cross-references and the register's own
 * "What is NOT on this list" paragraph into ranked entries: every number the delta reported as
 * resolving to nothing was a false positive, and two of them were real entries whose notes the same
 * run had already resolved by FILENAME. In the other direction, a note was suppressed from the
 * unranked half on the strength of the sentence "fixing astubbs#177 does not close it" - prose about
 * a note, read as a ranking of it.
 *
 * So an entry is a LIST ITEM, and the citations on that one line belong to it. An entry is satisfied
 * when either half resolves, which is what stops a number being called unresolvable while the
 * filename beside it resolves.
 *
 * A FILENAME IS ONLY A NOTE'S when it is bare or under `docs/inflight/`. `docs/merge-checklist.md`
 * matched the old pattern and rendered as a ranked entry that is `absent` - an instruction to delete
 * a live cross-reference, indistinguishable from the real case that row exists for.
 *
 * Both spellings of the number count: `AGENTS.md` mandates the fully qualified form for anything
 * posted to GitHub, and the register already uses it once.
 */
export function parseRegister(text) {
    // AN ENTRY IS A LIST ITEM INCLUDING ITS CONTINUATION LINES, not one physical line. This register
    // routinely opens an item with the number and names the note two lines down - both of the
    // entries a line-scoped parse got wrong (`astubbs#227`, `astubbs#317`) carry their filename on a
    // continuation - so scoping to the marker line reported them as citing a note that does not
    // exist. A continuation is an indented line; any non-indented line ends the item, which is what
    // keeps the register's own "What is NOT on this list" paragraph out of the last entry.
    const items = []
    for (const line of (text ?? '').split('\n')) {
        if (/^\s*(?:\d+\.|[-*])\s/.test(line)) items.push(line)
        else if (items.length > 0 && /^\s+\S/.test(line)) items[items.length - 1] += `\n${line}`
        else if (/\S/.test(line)) items.push('')
    }
    const entries = []
    for (const item of items) {
        if (!/^\s*(?:\d+\.|[-*])\s/.test(item)) continue
        const names = []
        for (const m of item.matchAll(/(?<prefix>[\w./-]*\/)?(?<name>[a-z][a-z0-9]*-[a-z0-9-]+\.md)/g)) {
            const prefix = m.groups.prefix ?? ''
            if (prefix === '' || prefix === `${NOTES_DIR}/`) names.push(m.groups.name)
        }
        const numbers = [...item.matchAll(/astubbs(?:\/parallel-consumer)?#(\d+)/g)].map((m) => Number(m[1]))
        if (names.length > 0 || numbers.length > 0) entries.push({ names, numbers })
    }
    // THE DENOMINATOR SHIPS WITH THE NUMERATOR. A count of recognised entries with nothing to
    // compare it against reads as a complete reading of the register, and it is not: the ready-picks
    // half cites bare `#40` and `confluentinc#...` numbers, which this parse does not reach and
    // deliberately does not guess at. `items` is every list item seen, recognised or not.
    return { entries, items: items.filter((i) => /^\s*(?:\d+\.|[-*])\s/.test(i)).length }
}

/**
 * The backlog view.
 *
 * Pure over its inputs: the corpus index, the bulk PR snapshot, and the register's text all arrive
 * as arguments, so the self-test drives the real logic against a fixture and never the network.
 *
 * @param {object} index a `corpusIndex` result, scoped to the notes area
 * @param {{prs: {ok: boolean, map: Map, reason?: string}, register: {ok: boolean, text?: string, reason?: string},
 *          group?: string|null}} opts
 */
export function rank(index, { prs, register, group = null }) {
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
        // THE SAME MEMBERSHIP GUARD `docsShape` APPLIES, imported rather than restated. Without it
        // this walked `index.byPath` raw and ranked `docs/inflight/CLAUDE.md` as open work - so the
        // two surfaces disagreed about what the corpus even contains, which is the failure this
        // module's header claims importing `inflightGroupOf` prevents.
        if (!DOCUMENT_RE.test(path) || DIRECTORY_DOCS_RE.test(path)) continue
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
    const unreadable = []
    const excluded = new Map()
    const byName = new Map()
    const byNumber = new Map()

    for (const { path, all, live, readable, onBaseline, versions } of plan) {
        const seen = versions
            .map((v) => {
                const text = read.contents.get(v.blob)
                if (text === undefined) return null
                const note = classifyNote(text, path)
                // TEXT TRAVELS WITH THE VERSION. `numberFor` needs the deciding version's own words
                // to attribute its number, and reaching for the loop-local `text` at the call site
                // was a ReferenceError that crashed the command on any numbered note.
                return { ...v, note, text, group: inflightGroupOf(note) }
            })
            .filter(Boolean)
        // A BLOB `ls-tree` LISTED AND `cat-file` DID NOT RETURN is a read that FAILED, not a note
        // that is not there - a partial clone, a gc race, corruption. Dropping it silently is the
        // empty-backlog-from-a-failure shape this whole file is written against, so it is named and
        // the run reports that it could not look.
        if (seen.length === 0) { unreadable.push(path); continue }

        const chosen = chooseVersion(seen, { onBaseline, readable, baseline: index.baseline })
        const note = chosen.note
        const key = chosen.group
        const name = path.slice(NOTES_DIR.length + 1)
        byName.set(name, { path, group: key, note, live: live.length > 0 })
        const numbered = numberFor(path, chosen.text)
        // NOT AN UPSTREAM-ATTRIBUTED NUMBER. The register keys its numbers as `astubbs#<n>`, so a
        // note whose own text says the number is confluentinc's must not satisfy a fork entry -
        // that is the wrong-reference-that-resolves failure `numberFor` was extended to prevent,
        // and the discriminating field is computed one line above.
        if (numbered && numbered.kind === 'issue' && numbered.attribution !== 'upstream') {
            if (!byNumber.has(numbered.value)) byNumber.set(numbered.value, [])
            byNumber.get(numbered.value).push(name)
        }

        // `!buckets.has(key)` is the belt to the derivation's braces: a group from outside the
        // taxonomy altogether is counted rather than throwing on an undefined bucket.
        if (NOT_RANKABLE.has(key) || !buckets.has(key)) {
            excluded.set(key, (excluded.get(key) ?? 0) + 1)
            continue
        }

        // What the OTHER versions say, so one status never hides a disagreement between branches.
        const disagreement = [...new Set(seen.map((v) => v.group))]
            .filter((g) => g !== key)
            .map((g) => {
                const ref = seen.find((v) => v.group === g).refs[0]
                return { group: g, ref, archival: archival.get(ref) === true }
            })

        // FROM THE CHOSEN VERSION'S OWN REFS, never the path-level `readable`. Those two sets can be
        // disjoint: a note closed on every live ref but still open on a preserved tag makes
        // `chooseVersion` pick the tag's version, while `readable` holds only the live refs - so the
        // lookup found nothing and the row crashed on `readRef.replace(...)`. Reproduced before this
        // was written; `rank-reads-a-note-that-is-open-only-on-an-archival-ref` holds the line.
        //
        // Live refs of the chosen version first, so a version carried by both is read from somewhere
        // a reader can go; its own first ref otherwise, which is the archival case and is why the
        // preserved row can name a tag at all.
        const chosenLive = chosen.refs.filter((r) => !archival.get(r))
        const readRef = onBaseline && chosen.refs.includes(index.baseline)
            ? index.baseline
            : (chosenLive.length > 0 ? chosenLive[0] : chosen.refs[0])

        buckets.get(key).push({
            path,
            name,
            title: note.title,
            group: key,
            impact: note.impact,
            onBaseline,
            // WHETHER THE VERSION BEING READ IS THE BASELINE'S, which is not the same as the path
            // being on the baseline: a note deferred on the baseline and open on a branch is now
            // read from that branch, and the "every branch cut from it carries this" sentence would
            // be a lie about a row whose whole point is the branch.
            readFromBaseline: readRef === index.baseline,
            // Carriage, never ownership - the one thing this command refuses to infer.
            relation: 'carries',
            carryingRefs: readRef === index.baseline ? [] : all,
            readRef,
            // TWO DIFFERENT FACTS, and collapsing them loses the useful half. `preserved` says no
            // live ref carries this note AT ALL. `readRefArchival` says the version being read is on
            // an archive even though something live carries the note - which happens when every live
            // copy is closed and an open one survives on a tag. A row that named the tag without
            // saying it was one would read as a branch somebody could go and work on.
            preserved: live.length === 0,
            readRefArchival: archival.get(readRef) === true,
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

    const computed = delta(register, { byName, byNumber, buckets, group })
    // WHICH ROWS THE REGISTER ALREADY NAMES, marked on the row itself. The scoped list previously
    // gave a count and then every row with nothing saying which were which, so the
    // register-versus-corpus distinction the delta exists to draw was unavailable at every scope.
    for (const rows of buckets.values()) for (const row of rows) row.ranked = computed.rankedNames.has(row.name)

    const groups = RANKED_GROUPS
        .filter((k) => buckets.get(k).length > 0)
        .filter((k) => group === null || k === group)
        .map((k) => ({ key: k, label: INFLIGHT_GROUPS[k] ?? k, rows: buckets.get(k) }))

    return {
        ok: true,
        baseline: index.baseline,
        // SHAPED LIKE `docsShape`'s, so `scopeLine` renders the same sentence for both rather than
        // each carrying its own copy of the tool's core disclaimer. Counted FROM THE MAP ALREADY
        // BUILT, not a second `refKind` pass over every ref.
        refs: {
            total: index.refs.length,
            live: [...archival.values()].filter((isArchival) => !isArchival).length,
            archival: [...archival.values()].filter(Boolean).length,
        },
        groups,
        excluded: [...excluded].map(([key, count]) => ({ key, count, label: INFLIGHT_GROUPS[key] ?? key })),
        unreadable,
        // REFS WHOSE NOTE LISTING FAILED, from the corpus index that already computed them and had
        // no consumer. A ref that could not be listed carries an unknown number of notes, so an
        // answer that does not name it is a could-not-look wearing the authority of a found-nothing.
        unreadableRefs: index.unreadableRefs ?? [],
        prsOk: prs.ok === true,
        prsReason: prs.ok ? null : (prs.reason ?? 'the pull-request snapshot did not answer'),
        scoped: group,
        delta: computed,
    }
}

/**
 * The register delta - the deliverable.
 *
 * ENTRY-SCOPED, because the register cites a note by filename AND by number on the same line. Asking
 * the two halves independently reported one entry twice when it went stale, and called a number
 * unresolvable while the filename beside it resolved. An entry is satisfied when EITHER half names
 * open work in an impact bucket.
 *
 * The two halves are asymmetric on purpose. What the register ranks is a handful of entries, so each
 * one that needs attention is listed with the reason. What it does NOT name is nearly every open
 * note in the repository, so that half is a count per group until a group scopes the call - listing
 * it unscoped would be the whole-corpus dump this command exists to avoid, under the name "delta".
 */
function delta(register, { byName, byNumber, buckets, group }) {
    // A REGISTER THAT COULD NOT BE READ IS A FAILED RUN, not an empty delta. The caller emits
    // everything that did run and then fails, the way `refactor-window` reports an unmeasurable
    // candidate.
    if (register.ok !== true) {
        return {
            ok: false, reason: register.reason ?? 'the register could not be read',
            stale: [], unrankedCounts: [], recognised: 0, items: 0, rankedNames: new Set(),
        }
    }
    const { entries, items } = parseRegister(register.text)

    // Every note any entry cites, by either half - these are the notes the register HAS named, and
    // are therefore not reported as unranked whatever else is true of them.
    const rankedNames = new Set()
    for (const e of entries) {
        for (const n of e.names) rankedNames.add(n)
        for (const num of e.numbers) for (const nm of (byNumber.get(num) ?? [])) rankedNames.add(nm)
    }

    // OPEN WORK MEANS REACHABLE WORK. Asking only the group let an entry be satisfied by a note no
    // live ref carries, so the delta could print "nothing it ranks has stopped being open work" a
    // few lines above its own row saying that note is preserved on a tag.
    const isOpenWork = (name) => {
        const hit = byName.get(name)
        return !!hit && hit.live && INFLIGHT_IMPACT_ORDER.includes(hit.group)
    }

    // What the register ranks that is NOT open work waiting in an impact bucket. The REASON is the
    // finding: gone needs deleting from the register, deferred needs a schedule decision, and one no
    // impact bucket claims needs a tag. Reporting all three as "not open" turns three different
    // actions into one shrug.
    const stale = []
    for (const e of entries) {
        const cited = [...e.names, ...e.numbers.flatMap((n) => byNumber.get(n) ?? [])]
        if (cited.some(isOpenWork)) continue
        const known = cited.filter((nm) => byName.has(nm))
        stale.push({
            cites: [...e.names, ...e.numbers.map((n) => `astubbs#${n}`)],
            reason: known.length > 0 ? byName.get(known[0]).group : 'absent',
        })
    }

    const unrankedCounts = []
    for (const [key, rows] of buckets) {
        if (group !== null && key !== group) continue
        const missing = rows.filter((row) => !rankedNames.has(row.name))
        if (missing.length > 0) unrankedCounts.push({ key, count: missing.length })
    }

    // HOW MUCH OF THE REGISTER THE PARSE ACTUALLY SAW. Zero recognised entries and zero findings
    // renders identically to a register everything agrees with - the found-nothing / could-not-look
    // collapse, moved from the git walk to the parse.
    return { ok: true, reason: null, stale, unrankedCounts, recognised: entries.length, items, rankedNames }
}
