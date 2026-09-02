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

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
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
