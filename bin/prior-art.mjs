#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Run the AGENTS.md "Before you investigate anything" checks over EVERY REF, not the working tree.
//
// WHY THIS EXISTS. Those checks were written as working-tree greps - `ls docs/plans/`,
// `grep -rl <mechanism> docs/solutions/`, `ls docs/inflight/`. In this repo most of the knowledge
// base lives on unmerged branches: measured 2026-09-01, 580 of the 901 documents under `docs/`
// across every ref exist ONLY on branches that have not merged. So a session on master cannot see
// two thirds of its own prior art, runs all six checks, gets "nothing", and reasons from a false
// negative - which is worse than not looking, because it carries the authority of a completed check.
//
// Worked incident: docs/solutions/workflow-issues/prior-art-lives-on-branches-2026-09-01.md.
//
// WHAT "NOTHING" MEANS HERE. Every section prints the size of the corpus it searched, and the tool
// exits 2 if it could not search at all. A search whose empty output is indistinguishable from a
// search that never ran is the failure in
// docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md. "No hits across
// 443 refs" is a result; a blank line is not.
//
// NOT A GATE. It exits 0 on "no prior art found", nothing in CI depends on it, and it is
// deliberately not named `check-*` - bin/AGENTS.md grants that prefix to the review agent.
//
// EXIT CODES: 0 ran (whatever it found), 2 cannot run.

import { execFileSync } from 'node:child_process'
import { statSync } from 'node:fs'

const CANNOT = 2
const REPO = 'astubbs/parallel-consumer'

/** Run a command; return {ok, out, status}. Never throws - callers decide what a failure means. */
function run(cmd, args) {
    try {
        return { ok: true, out: execFileSync(cmd, args, { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 }), status: 0 }
    } catch (e) {
        return { ok: false, out: e.stdout ?? '', status: e.status ?? -1 }
    }
}

const lines = (s) => s.split('\n').filter((l) => l.length > 0)

const terms = process.argv.slice(2)
if (terms.length === 0) {
    console.error(`Usage: bin/prior-art.mjs <term> [<term>...]

Terms are case-insensitive extended regexes, OR-ed together. Grep the MECHANISM, never the symptom
- the class, the lock, the option, the exception, the log line. A failing test's name is the weakest
search term available.

  bin/prior-art.mjs isTransactionCommittingInProgress acquireCommitLock
  bin/prior-art.mjs RetryQueue writeLock`)
    process.exit(CANNOT)
}

// Anything the caller passes is already a regex, so a term containing `|` or parens composes.
const pattern = terms.join('|')

// Local branches plus origin's, minus the symbolic HEAD which duplicates whatever it points at.
// Deliberately NOT `--all`: that pulls in tags and refs/stash, which add noise without adding docs.
const refsResult = run('git', ['for-each-ref', '--format=%(refname:short)', 'refs/heads', 'refs/remotes/origin'])
if (!refsResult.ok) {
    console.error('prior-art: cannot list refs - is this a git repository?')
    process.exit(CANNOT)
}
const refs = lines(refsResult.out).filter((r) => !r.endsWith('/HEAD'))
if (refs.length === 0) {
    console.error('prior-art: no branch refs found - nothing to search')
    process.exit(CANNOT)
}

// The ref every hit is compared against, so "not on the mainline" is a statement about a real ref
// rather than about whatever happens to be checked out.
const baseline = run('git', ['rev-parse', '--verify', '--quiet', 'origin/master']).ok ? 'origin/master' : 'master'

console.log(`prior-art: searching ${refs.length} refs for /${pattern}/i  (baseline: ${baseline})\n`)

// ------------------------------------------------------------------------------------------------
// Freshness, reported before any result. A complete search of a stale corpus is still a false
// negative, and it reads exactly like a complete search of a current one.
//
// Both warnings are real incidents. On 2026-09-01 a session investigated astubbs/parallel-consumer#44
// from the main checkout at the HEAD it opened with; master advanced 151 commits underneath it -
// including a solutions write-up on the very path under investigation - and every working-tree read
// answered for that snapshot without saying so.
// ------------------------------------------------------------------------------------------------
const gitDir = run('git', ['rev-parse', '--git-dir']).out.trim()
const commonDir = run('git', ['rev-parse', '--git-common-dir']).out.trim()
if (gitDir === commonDir) {
    console.log('  WARNING: this is the MAIN CHECKOUT, which AGENTS.md says never to work in - several')
    console.log('           sessions share it, so its HEAD can move between two of your own commands.')
    console.log('           Cut a worktree: git worktree add .claude/worktrees/<name> -b <branch> origin/master')
}

// A shallow clone does not error, it just has less history - and `git log --all -S` below then
// answers over a truncated corpus with no indication that it did.
if (run('git', ['rev-parse', '--is-shallow-repository']).out.trim() === 'true') {
    console.log('  WARNING: SHALLOW clone - the commit search below covers only the fetched depth.')
    console.log('           Run: git fetch --unshallow')
}

try {
    const ageSeconds = (Date.now() - statSync(`${commonDir}/FETCH_HEAD`).mtimeMs) / 1000
    if (ageSeconds > 3600) {
        console.log(`  WARNING: last fetch was ${Math.floor(ageSeconds / 3600)}h ago, so '${baseline}' and the ${refs.length} refs`)
        console.log("           below are that stale. Run 'git fetch origin' and re-run.")
    }
} catch {
    console.log("  WARNING: no FETCH_HEAD - this clone may never have fetched. Run 'git fetch origin'.")
}

const behind = Number(run('git', ['rev-list', '--count', `HEAD..${baseline}`]).out.trim() || '0')
if (behind > 0) {
    console.log(`  NOTE: your HEAD is ${behind} commit(s) behind ${baseline}. The search below is against the`)
    console.log('        refs, not your working tree, so it is unaffected - but anything you read out of')
    console.log(`        the working tree is ${behind} commits old. AGENTS.md: 'Read the commits you inherit'.`)
}
console.log()

// ------------------------------------------------------------------------------------------------
// Documents, across every ref.
//
// One line per PATH, not per ref: the same note usually exists on dozens of branches, and a raw
// `ref:path` listing buries the finding under its own duplicates. The flag is the point - a path
// absent from the baseline is prior art the working tree cannot show you.
// ------------------------------------------------------------------------------------------------
function searchDocs(heading, pathspec) {
    console.log(`=== ${heading} ===`)
    // git grep exits 1 for "no match" and >1 for a real error; only the latter is a problem.
    const res = run('git', ['grep', '-l', '-i', '-E', pattern, ...refs, '--', pathspec])
    if (!res.ok && res.status > 1) {
        console.error(`  ERROR: git grep failed (status ${res.status}) - results are NOT trustworthy`)
        process.exit(CANNOT)
    }
    const hits = lines(res.out)
    if (hits.length === 0) {
        console.log(`  nothing, across ${refs.length} refs\n`)
        return
    }
    // ref:path -> path -> carrying refs. A path can contain ':' only pathologically; split on the first.
    const byPath = new Map()
    for (const hit of hits) {
        const i = hit.indexOf(':')
        if (i < 0) continue
        const ref = hit.slice(0, i)
        const path = hit.slice(i + 1)
        if (!byPath.has(path)) byPath.set(path, [])
        byPath.get(path).push(ref)
    }
    for (const path of [...byPath.keys()].sort()) {
        const carriers = byPath.get(path)
        console.log(`  ${path}`)
        if (carriers.includes(baseline)) {
            console.log(`      on ${baseline}`)
        } else {
            console.log(`      NOT ON ${baseline} - e.g. ${carriers[0]} (${carriers.length} refs)`)
        }
    }
    console.log()
}

searchDocs('1. Prior investigations - docs/plans/', 'docs/plans/')
searchDocs('2. Solved problems - docs/solutions/', 'docs/solutions/')
searchDocs('3. In-flight state - docs/inflight/', 'docs/inflight/')
searchDocs('4. Everything else under docs/', 'docs/*.md')

// ------------------------------------------------------------------------------------------------
// Code history. A mechanism that was added and later removed leaves no trace in any tree - only in
// commits - so a tree search cannot find the experiment that already tried what you are proposing.
// ------------------------------------------------------------------------------------------------
console.log('=== 5. Commits that added or removed the term (git log --all -S) ===')
let foundCommit = false
for (const term of terms) {
    const res = run('git', ['log', '--all', '--format=  %h %ad %s', '--date=short', `-S${term}`])
    const found = lines(res.out).slice(0, 15)
    if (found.length > 0) {
        foundCommit = true
        console.log(`  -- ${term}`)
        console.log(found.join('\n'))
    }
}
if (!foundCommit) console.log('  nothing')
console.log()

// ------------------------------------------------------------------------------------------------
// GitHub. Optional - a missing or unauthenticated gh must not look like "no prior art".
//
// Every call names the repo. `gh` resolves a bare command against `upstream` in this fork, and a
// merged-PR search that silently answers for confluentinc reads exactly like "no prior art" - see
// docs/solutions/workflow-issues/compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md.
// ------------------------------------------------------------------------------------------------
function ghUnavailable(why) {
    console.log(`=== 6-8. GitHub checks SKIPPED - ${why} ===`)
    console.log('  These are NOT "nothing found". Run the three gh checks in AGENTS.md by hand.')
    process.exit(0)
}
if (!run('gh', ['--version']).ok) ghUnavailable('gh not installed')
if (!run('gh', ['auth', 'status']).ok) ghUnavailable('gh is not authenticated')

const jqSelect = (shape) =>
    `.[] | select((.title + " " + (.body // "")) | test("${pattern.replace(/"/g, '\\"')}"; "i")) | ${shape}`

function ghList(heading, note, args) {
    console.log(`=== ${heading} ===`)
    if (note) console.log(`    ${note}`)
    const res = run('gh', args)
    const found = lines(res.out)
    if (!res.ok) console.log('  (query failed - treat as UNKNOWN, not as nothing)')
    else if (found.length === 0) console.log('  nothing')
    else console.log(found.join('\n'))
    console.log()
}

ghList('6. Open PRs whose title or body matches (collision check)', null,
    ['pr', 'list', '-R', REPO, '--state', 'open', '--limit', '200', '--json', 'number,title,body',
        '--jq', jqSelect('"  #\\(.number) \\(.title)"')])

ghList('7. MERGED PRs whose title or body matches',
    '(the PR that already solved something in your file is, by definition, merged)',
    ['pr', 'list', '-R', REPO, '--state', 'merged', '--limit', '200', '--json', 'number,title,body',
        '--jq', jqSelect('"  #\\(.number) \\(.title)"')])

ghList('8. Issues, --state all (fork issues and upstream-mirror ones)',
    "(read the upstream original, not the mirror's summary)",
    ['issue', 'list', '-R', REPO, '--state', 'all', '--limit', '400', '--json', 'number,title,body,state',
        '--jq', jqSelect('"  #\\(.number) [\\(.state)] \\(.title)"')])
