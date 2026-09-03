// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE PROMPT HALF OF THE DOCUMENT CONTEXT QUERY: which words in a prompt name a mechanism, and
// which documents across every live ref carry those words - the plan's KTD6 and KTD17
// (docs/plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md, unit U4).
//
// Two functions, deliberately separable. `termsFromPrompt` is pure - no git, no filesystem - so
// the hook can run it before importing anything that touches git, and a prompt with no
// identifier in it costs Node's start and nothing else (KTD5's silent-path budget). `matchDocs`
// is the one git read: a single `git grep -n` across the live refs over the corpus areas, never
// a corpus-index build, which on this repository costs five seconds and would eat the budget
// twice over.
//
// FIXED STRINGS, NOT AN ALTERNATION. The plan wrote `-E <a|b|c>`; measured 2026-09-03 on this
// repository (559 live refs, three areas), `git grep -i -E 'commit_lock|inflight\.mjs'` costs
// 2.6 s and three alternated terms 3.6 s, while `-F -e commit_lock -e inflight.mjs` costs
// 440 ms and three fixed terms 490 ms - the same 14,596 hit lines either way. An alternation
// drops git's regex engine out of its fast path; a list of fixed strings does not, and a term
// here is an identifier, never a pattern, so nothing is lost. It also means a backticked span
// holding `(` or `.` matches itself rather than needing to be escaped.
//
// MATCHING ORDER IS DERIVED FROM THE HIT LINE'S SHAPE AND POSITION (R10). A grep returns matching
// lines only, so the frontmatter block's extent is not known exactly - and asking for it (adding
// `^---$` to the pattern) would return two lines per document per ref, hundreds of thousands of
// lines, to learn where a block ends. Instead: a line inside the first FRONTMATTER_LINES shaped
// like a YAML field (`key: ...`) or an INDENTED list item (`  - item` - the solutions' lists are
// indented, markdown bullets in a body are not), or an `<!-- inflight-... -->` marker line
// anywhere, is frontmatter; a line opening with `#` is a heading; anything else is body. The
// body tier is capped per term (KTD6) because a mechanism named in prose across forty notes is
// forty lines nobody reads, while the same name in a `related_components:` field is a claim the
// author made on purpose.
//
// NEVER PRINTS, NEVER EXITS - bin/inflight.mjs and the hooks own the process boundary, and
// bin/test-inflight.mjs asserts no library under bin/lib/ contains a process exit.

import { baseline as baselineRef, exec, lines, refTips } from './git.mjs'
import { blobTitles, drift } from './notes.mjs'
import { DOC_AREAS } from './repo.mjs'

/** How many terms one prompt may search for - beyond this a prompt is a document, not a question. */
export const MAX_TERMS = 12

/** Documents kept per term at the body tier; frontmatter and heading hits are never capped here. */
export const BODY_CAP_PER_TERM = 3

/** How many of the top hits get the divergence marks - each is a `drift` summary, and they are the cost. */
export const MARK_LIMIT = 12

/** The window inside which a field-shaped line is read as frontmatter rather than body. */
const FRONTMATTER_LINES = 30

const MIN_LENGTH = 4

/**
 * WORDS THAT PASS THE SHAPE RULES AND NAME NOTHING. Two kinds: the generic tokens agents type in
 * backticks or CamelCase that would match most of the corpus (`GitHub`, `README.md`, `master`), and
 * the hyphenated English the prose here is full of (`fail-open`, `read-only`, `self-test`), which
 * the hyphen rule would otherwise admit as identifiers. Compared case-insensitively. A word here
 * is one whose hits would be noise for every prompt; a word that is noise for one prompt is what
 * the body cap and the per-session dedupe are for.
 */
export const STOP_WORDS = new Set([
    // generic nouns agents put in backticks
    'the', 'this', 'that', 'with', 'from', 'file', 'files', 'test', 'tests', 'code', 'docs', 'note', 'notes',
    'branch', 'master', 'main', 'head', 'origin', 'origin/master', 'commit', 'merge', 'push', 'pull',
    'true', 'false', 'null', 'none', 'undefined', 'string', 'number', 'boolean', 'array', 'object',
    'json', 'yaml', 'html', 'http', 'https', 'node', 'java', 'kafka', 'maven', 'docker', 'bash', 'shell',
    'readme', 'readme.md', 'agents.md', 'claude.md', 'changelog.md', 'changelog.adoc', 'readme.adoc',
    'pom.xml', 'package.json', 'settings.json', '.gitignore',
    // product and vendor names in CamelCase that name no mechanism here
    'github', 'gitlab', 'javascript', 'typescript', 'macos', 'intellij', 'openai', 'chatgpt', 'jetbrains',
    'youtube', 'stackoverflow', 'postgresql', 'mysql', 'mongodb', 'graphql', 'restapi', 'testcontainers',
    'junit', 'mockito', 'lombok', 'copilot',
    // hyphenated English, not identifiers
    'fail-open', 'read-only', 'write-only', 'self-test', 'self-tests', 'well-known', 'built-in', 'long-running',
    'end-to-end', 'up-to-date', 'so-called', 'non-empty', 'high-level', 'low-level', 'real-world', 'one-line',
    'one-off', 'follow-up', 'write-up', 'write-ups', 'trade-off', 'trade-offs', 'set-up', 'look-up', 'sign-off',
    'check-in', 'pre-existing', 're-read', 're-run', 're-check', 're-cut', 'in-flight', 'off-by-one', 'top-level',
    'per-session', 'per-file', 'per-term', 'per-prompt', 'read-time', 'write-time', 'run-time', 'runtime',
    'multi-step', 'first-class', 'out-of-date', 'in-place', 'on-disk', 'in-memory', 'cross-platform',
    'copy-paste', 'hard-coded', 'hard-code', 'open-source', 'third-party', 'first-party', 'best-effort',
    'e.g', 'i.e', 'etc', 'vs', 'a.k.a',
])

/** Punctuation prose leaves stuck to a token: brackets, quotes, sentence marks, trailing dots and colons. */
const trim = (t) => t.replace(/^[("'[{<*_]+/, '').replace(/[)"'\]}>*,;:!?.]+$/, '')

/** A markdown link's target, or a URL: never a term - the path inside a URL names a server, not a mechanism. */
const isUrl = (t) => /^[a-z]+:\/\//i.test(t) || t.startsWith('www.')

/** `astubbs#419`, `astubbs/parallel-consumer#419`, `confluentinc#857`, or bare `#419`. */
const ISSUE_REF = /^(?:[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)?)?#(\d+)$/

/** At least two capitals and a lowercase letter, all word characters: `ProducerManager`, `PCModule`; not `Kafka`, not `README`. */
const isCamel = (t) => /^[A-Za-z][A-Za-z0-9]*$/.test(t) && (t.match(/[A-Z]/g) ?? []).length >= 2 && /[a-z]/.test(t)

/** A path, a dotted or snake_case or kebab-case name: `commit_lock`, `bin/inflight.mjs`, `inflight-impact`. */
const isJoined = (t) => /[_.\-/]/.test(t) && /[A-Za-z]/.test(t)

/**
 * The tokens in `text` that look like identifiers: the shapes KTD6 names, each at least four
 * characters, none on the stop list, deduplicated case-insensitively in order of first
 * appearance, capped at MAX_TERMS.
 *
 * ISSUE REFERENCES COLLAPSE TO THEIR `#NNN` CORE. A document cites a number in whichever form its
 * author used - `astubbs#419`, `astubbs/parallel-consumer#419`, a bare `#419` in a title - and
 * `astubbs#419` as a fixed string matches only the first. `#419` is contained in all three.
 *
 * A BACKTICKED SPAN IS AN EXPLICIT CLAIM THAT THIS IS A NAME, so its words are kept without the
 * shape test - `commit lock` in backticks yields `commit` and `lock`... except that both are
 * then subject to the length floor and the stop list like any other token, which is why
 * `commit` above is on the list: it is what agents backtick when they mean git, and it matches
 * every note in the corpus.
 *
 * @param {string} text
 * @returns {string[]}
 */
export function termsFromPrompt(text) {
    if (typeof text !== 'string' || text.length === 0) return []
    const out = []
    const seen = new Set()
    const keep = (raw) => {
        const t = trim(raw)
        if (!t || isUrl(t)) return
        const issue = t.match(ISSUE_REF)
        const term = issue ? `#${issue[1]}` : t
        if (!issue && term.length < MIN_LENGTH) return
        const key = term.toLowerCase()
        if (STOP_WORDS.has(key) || seen.has(key)) return
        seen.add(key)
        out.push(term)
    }
    // Backticked spans first, so a name the author marked up is not lost to the cap behind a
    // path the prose happened to mention earlier.
    const spans = []
    const stripped = text.replace(/`([^`\n]+)`/g, (m, inner) => { spans.push(inner); return ' ' })
    for (const span of spans) for (const w of span.split(/\s+/)) if (w) keep(w)
    for (const raw of stripped.split(/\s+/)) {
        const t = trim(raw)
        if (!t) continue
        if (ISSUE_REF.test(t) || isCamel(t) || isJoined(t)) keep(t)
    }
    return out.slice(0, MAX_TERMS)
}

/**
 * BRANCH-NAME PREFIXES THAT SAY WHAT KIND OF WORK A BRANCH IS, never what it is about: the plan's
 * list, plus the ones `git branch -r | cut -d/ -f2 | sort | uniq -c` reports in use here. Most
 * would fall to the shape rules anyway - a plain word is not an identifier - so the set earns its
 * place on the hyphenated ones (`cherry-pick`, `upstream-pr`) and by saying the intent out loud.
 */
export const BRANCH_TYPE_SEGMENTS = new Set([
    'feats', 'feat', 'feature', 'features', 'fix', 'fixes', 'bugs', 'bug', 'docs', 'ci', 'test', 'tests',
    'chore', 'refactor', 'refactors', 'experiment', 'demos', 'demo', 'improvements', 'perf', 'backup',
    'handoff', 'research', 'optimize', 'debug', 'cherry-pick', 'upstream', 'upstream-pr', 'recut', 'deps',
    'dependabot', 'issues', 'web',
])

/** `origin/x`, `refs/heads/x` and `refs/remotes/origin/x` all name the branch `x` - the PR cache's key. */
const branchName = (ref) => ref.replace(/^refs\/(?:heads|remotes\/[^/]+)\//, '').replace(/^origin\//, '')

/**
 * The terms a branch's own facts yield: its name's segments and - when `prs` holds it - its PR
 * number and the identifiers in its PR title. The plan's U7: what `docs for-branch` searches for
 * and the session hook injects.
 *
 * THE SLUG IS THE IDENTIFIER, NOT ITS WORDS. `fix/857-commit-lock` yields `#857` and `commit-lock`:
 * a leading issue number is split off into the `#NNN` core every spelling collapses to, and the
 * rest stays one hyphenated term, because a document that names the branch writes the slug, while
 * one that merely uses the words `commit` and `lock` is half the corpus. The kind prefix is
 * dropped; a plain-word segment with no number meets termsFromPrompt's shape rules like any other
 * word, and usually fails them, which is right - `hasten` on its own names nothing.
 *
 * NO NETWORK, BY CONSTRUCTION. `prs` is whatever map the caller has - the CLI passes the cached PR
 * list and nothing else - and a miss yields the branch-name terms alone. Backticks in a PR title
 * are stripped before the shape rules see it: a span there is not the author claiming a word is a
 * name, and `inflight docs` in a title would otherwise yield `inflight`, which names a directory.
 *
 * @param {string} ref a branch name, with or without `origin/` or `refs/...`
 * @param {{prs?: Map<string, {number: number, title: string}> | null}} opts
 * @returns {string[]} the branch's terms first, then the PR's, deduplicated and capped as termsFromPrompt does
 */
export function termsFromBranch(ref, { prs = null } = {}) {
    if (typeof ref !== 'string' || ref.length === 0) return []
    const name = branchName(ref)
    const parts = []
    for (const seg of name.split('/').filter(Boolean)) {
        if (BRANCH_TYPE_SEGMENTS.has(seg.toLowerCase())) continue
        const m = seg.match(/^(\d+)(?:[-_](.+))?$/)
        if (m) {
            parts.push(`#${m[1]}`)
            if (m[2]) parts.push(m[2])
            continue
        }
        parts.push(seg)
    }
    const pr = prs?.get(name) ?? null
    if (pr) {
        if (Number.isInteger(pr.number)) parts.push(`#${pr.number}`)
        if (typeof pr.title === 'string') parts.push(pr.title.replaceAll('`', ' '))
    }
    return termsFromPrompt(parts.join(' '))
}

const TIER_RANK = { frontmatter: 3, heading: 2, body: 1 }

/**
 * Which tier one hit line belongs to, from its shape and position alone - the header explains
 * why the frontmatter block's true extent is not read.
 */
export function tierOfLine(lineNo, text) {
    if (/^\s*<!--\s*inflight-/.test(text)) return 'frontmatter'
    if (/^#{1,6}\s/.test(text)) return 'heading'
    if (lineNo <= FRONTMATTER_LINES && (/^[A-Za-z_][A-Za-z0-9_]*:(\s|$)/.test(text) || /^\s+-\s/.test(text))) return 'frontmatter'
    return 'body'
}

/**
 * The documents across every live ref that carry any of `terms`, best tier first.
 *
 * ONE `git grep`, over the live refs `refTips` reports and the corpus areas, and no `ls-tree`:
 * bin/test-inflight.mjs counts the subprocesses. The top `markLimit` hits then each cost one
 * `drift` summary for their marks - `onBaseline` (the path exists on the baseline at all) and
 * `divergent` (a live ref carries a version the baseline has never held) - and one batched
 * `cat-file` reads their titles. Hits past that bound carry `title: null` and no marks; the
 * hook shows at most that many anyway, and the plan bounds the cost there deliberately.
 *
 * `ok: true` with no hits is "nothing", and says which refs it covered (R21). `ok: false` is
 * only "git could not answer", never "found nothing".
 *
 * @param {string[]} terms
 * @param {{areas?: {dir: string}[], bodyCap?: number, markLimit?: number}} opts
 * @returns {{ok: boolean, reason?: string, terms: string[], baseline?: string, refsSearched: number,
 *   live: number, archival: number, hits: object[], truncated: number}}
 */
export function matchDocs(terms, { areas = DOC_AREAS, bodyCap = BODY_CAP_PER_TERM, markLimit = MARK_LIMIT } = {}) {
    const result = { ok: false, terms, refsSearched: 0, live: 0, archival: 0, hits: [], truncated: 0 }
    const cannot = (reason) => ({ ...result, reason })
    if (!Array.isArray(terms) || terms.length === 0) return cannot('no terms to search for')

    const tips = refTips()
    if (!tips.ok) return cannot('cannot list refs - is this a git repository?')
    const live = tips.tips.filter((r) => !r.archival).map((r) => r.ref)
    result.live = live.length
    result.archival = tips.tips.length - live.length
    result.refsSearched = live.length
    if (live.length === 0) return cannot('no live refs found - nothing to search')
    const base = baselineRef()
    if (!base) return cannot('neither origin/master nor master resolves - no baseline to compare against')
    result.baseline = base

    const patterns = terms.flatMap((t) => ['-e', t])
    const res = exec('git', ['grep', '-n', '-i', '-F', ...patterns, ...live, '--', ...areas.map((a) => `${a.dir}/`)])
    // git grep exits 1 for "no match" and >1 for a real error; only the latter is a problem.
    if (!res.ok && res.status > 1) return cannot(`git grep failed (status ${res.status}) - results are NOT trustworthy`)

    // `ref:path:line:text`. A ref name never holds ':'; a path could only pathologically.
    const byPath = new Map()
    const lowerTerms = terms.map((t) => t.toLowerCase())
    for (const hit of lines(res.out)) {
        const i = hit.indexOf(':')
        const j = hit.indexOf(':', i + 1)
        const k = hit.indexOf(':', j + 1)
        if (i < 0 || j < 0 || k < 0) continue
        const ref = hit.slice(0, i)
        const path = hit.slice(i + 1, j)
        const lineNo = Number(hit.slice(j + 1, k))
        const text = hit.slice(k + 1)
        if (!Number.isInteger(lineNo)) continue
        const tier = tierOfLine(lineNo, text)
        const lower = text.toLowerCase()
        if (!byPath.has(path)) byPath.set(path, { path, tier: 'body', terms: new Set(), refs: new Set(), heading: null })
        const doc = byPath.get(path)
        doc.refs.add(ref)
        for (let n = 0; n < terms.length; n++) if (lower.includes(lowerTerms[n])) doc.terms.add(terms[n])
        if (TIER_RANK[tier] > TIER_RANK[doc.tier]) doc.tier = tier
        // The document's own title, when the term sits in it: the cheapest title read there is.
        if (doc.heading === null && /^#\s/.test(text)) doc.heading = text.replace(/^#\s+/, '').trim()
    }

    const ranked = [...byPath.values()]
        .map((d) => ({ ...d, terms: terms.filter((t) => d.terms.has(t)), refs: [...d.refs].sort() }))
        .sort((a, b) => TIER_RANK[b.tier] - TIER_RANK[a.tier] || b.terms.length - a.terms.length || a.path.localeCompare(b.path))

    // The body cap, per term: a document at the body tier survives while any term it matched has
    // budget left, and spends one of each term's. Frontmatter and heading hits never pay.
    const budget = new Map(terms.map((t) => [t, bodyCap]))
    const kept = []
    for (const d of ranked) {
        if (d.tier !== 'body') { kept.push(d); continue }
        if (!d.terms.some((t) => budget.get(t) > 0)) { result.truncated += 1; continue }
        for (const t of d.terms) budget.set(t, budget.get(t) - 1)
        kept.push(d)
    }

    // Marks and titles for the top hits only - one drift summary each, then one cat-file for all.
    const top = kept.slice(0, markLimit)
    const blobs = []
    for (const d of top) {
        // `at` names the first ref the grep hit, which certainly carries the path, so the summary
        // costs no extra lookup and its blob is the version that matched.
        const s = drift(d.path, { detail: 'summary', at: { ref: d.refs[0] } })
        if (s.ok === false) return cannot(`${d.path}: ${s.reason}`)
        d.onBaseline = s.found ? s.onBaseline : false
        d.divergent = s.found ? (s.divergent ?? []).length > 0 : false
        d.blob = s.at?.blob ?? null
        if (d.blob && !d.heading) blobs.push(d.blob)
    }
    const titles = blobTitles(blobs)
    for (const d of top) d.title = d.heading ?? (d.blob ? titles.get(d.blob) ?? null : null)
    for (const d of kept.slice(markLimit)) { d.title = d.heading; d.onBaseline = null; d.divergent = null; d.blob = null }

    // `heading` was the cheap title read; `title` carries it now, and the shape has one name for it.
    for (const d of kept) delete d.heading
    result.ok = true
    result.hits = kept
    return result
}
