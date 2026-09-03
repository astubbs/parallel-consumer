// Copyright (C) 2026 Antony Stubbs and contributors
//
// A THROWAWAY CACHE, and the single home for it - the move bin/lib/inflight-tags.sh's header
// describes, made before a second private copy appears rather than after.
//
// It is for network answers ONLY. Git data is never cached here: git is already a cache, the tip
// SHAs that would key one are themselves a git read, and the corpus cache that used to live in
// bin/lib/notes.mjs was deleted for exactly that reason - it cost a 2.5MB file per key and hid a
// design mistake underneath it.
//
// What belongs here is what crosses the network and shares a rate limit with every parallel session
// in this repository: GitHub. Bounded by time rather than trusted, because PR state moves without
// any local ref moving.

import { mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const cacheDir = () => process.env.PC_INFLIGHT_CACHE_DIR
    || join(tmpdir(), `pc-inflight-cache-${process.getuid?.() ?? 0}`)

/**
 * FRESHNESS IS THIS LAYER'S JOB, NOT THE CALLER'S - and it is the reason a hook was deleted.
 *
 * Every caller used to pass its own `maxAgeMs`, so the policy for a cache lived at each of its
 * read sites and nowhere authoritative. The visible consequence was a hook named for a TRIGGER,
 * `after-pr-create-refresh-cache`, whose entire job was to reach in from outside and repair a
 * staleness the layer would not admit to - which covered exactly one way a PR can come into
 * existence (`gh pr create`, in a session that had the hook loaded) and none of the others: the
 * web, another machine, another session. A cache that needs an external event to be correct is
 * coupled to that event, and every path that does not fire it is silently wrong.
 *
 * `cacheEmpty: false` is the half that made the hook unnecessary. ABSENCE IS THE ANSWER THAT GOES
 * STALE IN THE DANGEROUS DIRECTION - "this branch has no PR" is a false negative the moment someone
 * opens one, and opening one is what people do next. Presence is safe to keep: PRs are not
 * un-created, and a stale title is cosmetic where a wrong "no PR" is not.
 */
const POLICY = {
    'prs.json': { maxAgeMs: 24 * 60 * 60 * 1000, cacheEmpty: true },
    'pr-branch.json': { maxAgeMs: 6 * 60 * 60 * 1000, cacheEmpty: false },
    // Codecov's recorded test history. Ten minutes because a burst of `inflight codecov` queries in
    // one session should cost one fetch, while a CI run finishing mid-session still shows up. Empty
    // is not cached: an empty corpus would read as "no flakes recorded" - a false negative in the
    // dangerous direction, which is the same reason `pr-branch.json` refuses to cache "no PR".
    'codecov-tests.json': { maxAgeMs: 10 * 60 * 1000, cacheEmpty: false },
    // The last failure of each context-query delivery (the plan's KTD13): a hook that fails open
    // prints nothing to the agent, so this is the only place the failure exists, and bare
    // `inflight docs` reads it back as a one-line notice. Seven days, so a record does not outlive
    // the session that could act on it; an empty map is a legitimate "nothing failed" and is stored.
    'delivery-failures.json': { maxAgeMs: 7 * 24 * 60 * 60 * 1000, cacheEmpty: true },
}
const DEFAULT_POLICY = { maxAgeMs: 60 * 60 * 1000, cacheEmpty: false }

/** The policy for a cache kind. Exported so a self-test asserts the policy, not a magic number. */
export function policyFor(name) {
    return POLICY[name] ?? DEFAULT_POLICY
}

/**
 * THE CACHES THIS CODE ACTUALLY USES, derived from the policy rather than listed a second time.
 *
 * It WAS listed a second time, in the front door, and the two had already drifted: renaming a cache
 * kind here left the other copy naming the retired one, so `cache` reported the live file as an
 * ORPHAN and the dead file as live - inverting the one thing that view exists to tell you. Anything
 * with a policy is known; anything else is a leftover, by construction.
 */
export const knownCaches = () => Object.keys(POLICY)

/**
 * ONE FILE PER KIND, with the key INSIDE it - never one file per key.
 *
 * A key-named file orphans a copy every time the key changes; 7.4MB accumulated in a single session
 * before that was fixed. A mismatched key read back is simply a miss, so exactness is unchanged and
 * the file self-cleans by being overwritten.
 */
export function cacheRead(name, { key } = {}) {
    try {
        const raw = JSON.parse(readFileSync(join(cacheDir(), name), 'utf8'))
        if (key !== undefined && raw.key !== key) return null
        // THE TTL COMES FROM THE POLICY, never from the caller. Two read sites disagreeing about
        // how old is too old is a cache with two answers, and nothing would report the difference.
        if (Date.now() - raw.at > policyFor(name).maxAgeMs) return null
        return raw.value
    } catch { return null }
}

export function cacheWrite(name, value, key) {
    // AN EMPTY ANSWER IS REFUSED where the policy says so, at the layer rather than at each caller
    // - a guard written at the call site is one an added caller does not inherit.
    if (!policyFor(name).cacheEmpty && Array.isArray(value) && value.length === 0) return
    try {
        mkdirSync(cacheDir(), { recursive: true, mode: 0o700 })
        writeFileSync(join(cacheDir(), name), JSON.stringify({ at: Date.now(), key, value }))
    } catch { /* a cache that cannot be written must not break the answer */ }
}

/**
 * What is cached, how old, and what is no longer read by anything.
 *
 * THE TIMESTAMP IS INSIDE THE FILE, NOT IN ITS NAME - deliberately. A timestamped filename shows
 * freshness in `ls` and creates a new file per write, which is exactly the orphan accumulation this
 * module's header records: 7.4MB in a single session before one-file-per-kind fixed it. The
 * filesystem's own mtime already answers "how old" for free, and the `at` field is the authoritative
 * copy the code reads, since an mtime can be rewritten by anything that touches the file.
 *
 * `known` is the set of names the current code actually uses. Anything else is an ORPHAN - left by
 * a cache that has since been removed, still occupying space, and never read again. Naming them is
 * the point: an unreadable leftover is indistinguishable from a live cache by looking at the
 * directory, and `corpus.json` sat there at 2.5MB after its cache was deleted.
 */
export function cacheStatus(known = knownCaches()) {
    let names = []
    try { names = readdirSync(cacheDir()) } catch { return { dir: cacheDir(), exists: false, entries: [] } }
    const entries = names.map((name) => {
        let bytes = 0
        let at = null
        try { bytes = statSync(join(cacheDir(), name)).size } catch { /* raced with a writer */ }
        try { at = JSON.parse(readFileSync(join(cacheDir(), name), 'utf8')).at ?? null } catch { /* not ours */ }
        return { name, bytes, at, ageMs: at ? Date.now() - at : null, orphan: !known.includes(name) }
    })
    return { dir: cacheDir(), exists: true, entries: entries.sort((a, b) => b.bytes - a.bytes) }
}

/** Delete cached files. Orphans only by default, because dropping a live cache is a separate ask. */
export function cacheClear({ all = false, known = knownCaches() } = {}) {
    const status = cacheStatus(known)
    if (!status.exists) return { removed: [], bytes: 0 }
    const doomed = status.entries.filter((e) => all || e.orphan)
    let bytes = 0
    for (const e of doomed) {
        try { rmSync(join(cacheDir(), e.name)); bytes += e.bytes } catch { /* already gone */ }
    }
    return { removed: doomed.map((e) => e.name), bytes }
}

// --- Delivery failures - the record a fail-open hook leaves behind. ------------------------------
//
// Every delivery of the document context query fails OPEN: an error prints nothing to the agent's
// context and never blocks the read or the prompt (the plan's R20). That is the right posture for a
// hook, and it has a cost this record pays: a hook that has been broken for a week looks exactly
// like a hook with nothing to say. So a delivery that catches an error writes its name, the reason
// and the time here, a later success of the SAME delivery clears its entry, and bare
// `inflight docs` prints a one-line notice while any entry exists (R26).
//
// ONE MAP, KEYED BY DELIVERY, so a flapping hook holds one entry rather than a growing list, and the
// age limit is per entry as well as per file: the policy row's seven days bounds the file, and a
// reader drops an entry older than that even when a fresher entry kept the file young.

const DELIVERY_FAILURES = 'delivery-failures.json'

/** Every recorded failure younger than the policy's age limit: `{ [delivery]: {reason, time} }`. */
export function deliveryFailures() {
    const raw = cacheRead(DELIVERY_FAILURES)
    if (!raw || typeof raw !== 'object') return {}
    const limit = Date.now() - policyFor(DELIVERY_FAILURES).maxAgeMs
    return Object.fromEntries(Object.entries(raw)
        .filter(([, v]) => v && typeof v === 'object' && Date.parse(v.time) >= limit))
}

/** Record `delivery`'s latest failure. Never throws: the record is a courtesy, not the answer. */
export function recordDeliveryFailure(delivery, reason) {
    const now = { ...deliveryFailures(), [delivery]: { reason: String(reason), time: new Date().toISOString() } }
    cacheWrite(DELIVERY_FAILURES, now)
}

/**
 * Forget `delivery`'s failure, because it just succeeded. Writes only when there was something to
 * forget - the common case is a healthy hook, and a healthy hook must not touch the disk per call.
 */
export function clearDeliveryFailure(delivery) {
    const current = deliveryFailures()
    if (!(delivery in current)) return
    const { [delivery]: cleared, ...rest } = current
    // `cleared` is the entry being dropped; nothing reads it, and the destructure is the removal.
    void cleared
    cacheWrite(DELIVERY_FAILURES, rest)
}
