// Copyright (C) 2026 Antony Stubbs and contributors
//
// CODECOV AS A QUERYABLE HISTORY, for the questions git cannot answer.
//
// Codecov holds one thing this repository has nowhere else: the OUTCOME AND WALL-CLOCK of every
// individual test, per commit, per branch, going back further than a CI log is retained for. That
// makes it the natural instrument for three questions an agent asks constantly and currently answers
// by re-running things:
//
//   - "when did this test start failing" - a bisect over recorded history instead of over builds
//   - "is this test flaky, or did my change break it" - the distinction docs/quarantined-tests.md
//     demands EVIDENCE for, and which a sighting ledger is currently kept by hand to supply
//   - "did this get slower" - for test wall-clock, with the caveat below
//
// NO TOKEN, ON PURPOSE. This repo is public, so Codecov's API v2 answers unauthenticated. That is
// what makes it usable from a fresh agent sandbox, from CI, and from a machine that has never run
// `gh auth login` - the tool works or it does not, with no per-machine setup step to forget.
//
// WHAT `duration_seconds` IS NOT. It is the test's wall-clock on a shared GitHub runner. It is NOT
// the library's throughput, and it must never be fed into the throughput regression comparison:
// bin/check-throughput-regression.mjs compares recordsPerSecond, a figure the performance test
// COMPUTES about the library, on a controlled arm. Test duration moves with runner contention,
// Docker pulls and broker startup - the exact confound the throughput work built control arms to
// eliminate. Treated as a regression signal it would reintroduce the noise that system exists to
// remove. It is a good coarse signal for "this test got much slower"; it is not a benchmark.
//
// FILTERS THE API HONOURS, measured rather than read off the docs: `branch` and `commit_sha` are
// applied server-side. `flags`, `interval` and `outcome` are ACCEPTED AND IGNORED - passing
// `flags=performance` returned the unfiltered total, which is the failure mode where a filter that
// silently does nothing reads as a filter that found everything. So everything except branch and
// commit is filtered here, client-side, where it can be seen to happen.
//
// These functions return findings and never exit or print; bin/inflight.mjs renders and decides the
// code. `ok: false` means COULD NOT ASK - offline, API down. A query that ran and matched nothing is
// `ok: true` with an empty list, because "no flakes" and "could not look" are different answers.

import { exec } from './git.mjs'
import { cacheRead, cacheWrite } from './cache.mjs'

const API = 'https://api.codecov.io/api/v2/github/astubbs/repos/parallel-consumer'

// Long enough that a burst of queries in one session costs one fetch, short enough that a run
// finishing mid-session shows up. The whole corpus is a few hundred KB, so this is about latency
// and politeness rather than size.
const MAX_AGE_MS = 10 * 60 * 1000

/** One GET, as JSON. Never throws; `ok:false` means the network or the API refused. */
function get(url) {
    const r = exec('curl', ['-sS', '--max-time', '25', '-w', '\n%{http_code}', url])
    if (!r.ok) return { ok: false, reason: 'curl could not reach api.codecov.io' }
    const cut = r.out.lastIndexOf('\n')
    const code = r.out.slice(cut + 1).trim()
    if (code !== '200') return { ok: false, reason: `api.codecov.io returned HTTP ${code}` }
    try {
        return { ok: true, value: JSON.parse(r.out.slice(0, cut)) }
    } catch {
        return { ok: false, reason: 'api.codecov.io returned a body that is not JSON' }
    }
}

/** Repo-wide coverage totals, and per-flag coverage ON THE DEFAULT BRANCH. */
export function coverage() {
    const totals = get(`${API}/totals/`)
    if (!totals.ok) return totals
    const flags = get(`${API}/flags/`)
    return {
        ok: true,
        value: {
            totals: totals.value.totals ?? {},
            // Every flag except `default` reads 0% here and that is CORRECT, not a broken upload:
            // this endpoint reports default-branch coverage, the per-suite `test:` job is
            // pull_request-only, and only `build:` (flag `default`) runs on a push to master.
            // Recorded because a reader who does not know that files a bug against the uploader.
            flags: flags.ok ? (flags.value.results ?? []) : [],
        },
    }
}

/**
 * Every recorded test result, newest upload first.
 *
 * Paged deliberately rather than trusting one big page_size: the corpus grows with every push, and
 * a cap that silently truncates would make "no sightings" a lie. `pages` bounds the walk so a
 * runaway cannot hang an agent, and the caller is TOLD when the bound was hit.
 */
export function testHistory({ branch, pages = 12, fresh = false } = {}) {
    const key = `v1:${branch ?? '*'}:${pages}`
    if (!fresh) {
        const hit = cacheRead('codecov-tests.json', { key, maxAgeMs: MAX_AGE_MS })
        if (hit) return { ok: true, value: { ...hit, cached: true } }
    }
    const results = []
    let url = `${API}/test-analytics/?page_size=1000`
    if (branch) url += `&branch=${encodeURIComponent(branch)}`
    let truncated = false
    for (let i = 0; i < pages; i += 1) {
        const r = get(url)
        if (!r.ok) return r
        results.push(...(r.value.results ?? []))
        // The API hands back an http:// next link even though it is served over https; following it
        // verbatim would silently downgrade the connection, so only the query half is reused.
        const next = r.value.next
        if (!next) { truncated = false; break }
        url = `${API}/test-analytics/${next.slice(next.indexOf('?'))}`
        truncated = i === pages - 1
    }
    const value = { results, truncated, cached: false }
    cacheWrite('codecov-tests.json', { results, truncated }, key)
    return { ok: true, value }
}

/**
 * Group raw rows into one entry per test, each carrying its observations newest-first.
 *
 * Exported, with the three analyses below split into a PURE half over rows and a thin fetching
 * wrapper, so the self-test drives the real logic on fixture rows instead of the network. An
 * analysis reachable only through a live API is one nothing can check when the API is down, and
 * a test that needs the network is a test that gets deleted the first time CI is offline.
 */
export function byTest(rows) {
    const m = new Map()
    for (const r of rows) {
        const name = r.computed_name ?? `${r.classname}::${r.name}`
        if (!m.has(name)) m.set(name, [])
        m.get(name).push({
            sha: (r.commit_sha ?? '').slice(0, 7),
            branch: r.branch,
            outcome: r.outcome,
            seconds: r.duration_seconds,
            at: r.timestamp,
            flags: r.flags ?? [],
            failure: r.failure_message,
        })
    }
    for (const obs of m.values()) obs.sort((a, b) => String(b.at).localeCompare(String(a.at)))
    return m
}

/**
 * One test's recorded history - THE BISECT.
 *
 * Matching is substring, case-insensitive, because an agent holding a failure has the method name
 * and rarely the fully-qualified one. Ambiguity is returned as a list of candidates rather than
 * resolved by guessing, since picking the wrong test here answers a different question convincingly.
 */
export function timelineFrom(rows, query) {
    const all = byTest(rows)
    const q = query.toLowerCase()
    const hits = [...all.entries()].filter(([name]) => name.toLowerCase().includes(q))
    return {
        query,
        matches: hits.map(([name, observations]) => ({ name, observations })),
        corpus: all.size,
    }
}

export function testTimeline(query, opts = {}) {
    const h = testHistory(opts)
    if (!h.ok) return h
    return {
        ok: true,
        value: { ...timelineFrom(h.value.results, query), truncated: h.value.truncated, cached: h.value.cached },
    }
}

/**
 * Tests recorded with MORE THAN ONE distinct outcome - the flake candidates.
 *
 * This is a candidate list and never a verdict. Two runs of one test differing across two DIFFERENT
 * commits is equally consistent with a real regression, which is the whole reason
 * docs/quarantined-tests.md refuses to quarantine on a rate alone. What this replaces is the manual
 * reconstruction of the sighting ledger from CI logs that expire.
 */
export function flakesFrom(rows) {
    const out = []
    for (const [name, observations] of byTest(rows)) {
        const outcomes = new Set(observations.map((o) => o.outcome))
        if (outcomes.size > 1) {
            const bad = observations.filter((o) => o.outcome !== 'pass')
            out.push({ name, runs: observations.length, failures: bad.length, observations })
        }
    }
    out.sort((a, b) => b.failures - a.failures || b.runs - a.runs)
    return out
}

export function flakeCandidates(opts = {}) {
    const h = testHistory(opts)
    if (!h.ok) return h
    return {
        ok: true,
        value: { candidates: flakesFrom(h.value.results), truncated: h.value.truncated, cached: h.value.cached },
    }
}

/** The slowest tests by their most recent recorded wall-clock. See the header on what this is not. */
export function slowestFrom(raw, limit = 20) {
    const rows = []
    for (const [name, observations] of byTest(raw)) {
        // The LATEST observation that carries a duration - not the max, and not the first. A test
        // whose most recent run has no timing should fall back to when it last did, rather than
        // silently dropping out of a list whose whole purpose is "what owns the wall-clock now".
        const latest = observations.find((o) => typeof o.seconds === 'number')
        if (latest) rows.push({ name, seconds: latest.seconds, sha: latest.sha, flags: latest.flags })
    }
    rows.sort((a, b) => b.seconds - a.seconds)
    return { rows: rows.slice(0, limit), tests: rows.length, totalSeconds: rows.reduce((t, r) => t + r.seconds, 0) }
}

export function slowest(limit = 20, opts = {}) {
    const h = testHistory(opts)
    if (!h.ok) return h
    return { ok: true, value: { ...slowestFrom(h.value.results, limit), cached: h.value.cached } }
}
