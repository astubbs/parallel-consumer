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

const CACHE_DIR = join(tmpdir(), `pc-inflight-cache-${process.getuid?.() ?? 0}`)

/**
 * ONE FILE PER KIND, with the key INSIDE it - never one file per key.
 *
 * A key-named file orphans a copy every time the key changes; 7.4MB accumulated in a single session
 * before that was fixed. A mismatched key read back is simply a miss, so exactness is unchanged and
 * the file self-cleans by being overwritten.
 */
export function cacheRead(name, { key, maxAgeMs } = {}) {
    try {
        const raw = JSON.parse(readFileSync(join(CACHE_DIR, name), 'utf8'))
        if (key !== undefined && raw.key !== key) return null
        if (maxAgeMs && Date.now() - raw.at > maxAgeMs) return null
        return raw.value
    } catch { return null }
}

export function cacheWrite(name, value, key) {
    try {
        mkdirSync(CACHE_DIR, { recursive: true, mode: 0o700 })
        writeFileSync(join(CACHE_DIR, name), JSON.stringify({ at: Date.now(), key, value }))
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
export function cacheStatus(known = ['prs.json', 'pr-search.json']) {
    let names = []
    try { names = readdirSync(CACHE_DIR) } catch { return { dir: CACHE_DIR, exists: false, entries: [] } }
    const entries = names.map((name) => {
        let bytes = 0
        let at = null
        try { bytes = statSync(join(CACHE_DIR, name)).size } catch { /* raced with a writer */ }
        try { at = JSON.parse(readFileSync(join(CACHE_DIR, name), 'utf8')).at ?? null } catch { /* not ours */ }
        return { name, bytes, at, ageMs: at ? Date.now() - at : null, orphan: !known.includes(name) }
    })
    return { dir: CACHE_DIR, exists: true, entries: entries.sort((a, b) => b.bytes - a.bytes) }
}

/** Delete cached files. Orphans only by default, because dropping a live cache is a separate ask. */
export function cacheClear({ all = false, known = ['prs.json', 'pr-search.json'] } = {}) {
    const status = cacheStatus(known)
    if (!status.exists) return { removed: [], bytes: 0 }
    const doomed = status.entries.filter((e) => all || e.orphan)
    let bytes = 0
    for (const e of doomed) {
        try { rmSync(join(CACHE_DIR, e.name)); bytes += e.bytes } catch { /* already gone */ }
    }
    return { removed: doomed.map((e) => e.name), bytes }
}
