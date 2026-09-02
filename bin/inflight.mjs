#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE FRONT DOOR for this repo's in-flight tooling. One command an agent has to know, with a help
// list that names the others, instead of N scripts in bin/ that it has to already know to reach for.
//
// WHY A FRONT DOOR AT ALL. The failure this fixes is not that the tools are missing - it is that an
// agent that cannot name the tool uses `git grep` on the working tree and gets a false negative with
// the authority of a completed check. bin/lib/prior-art.mjs's header carries the measurement: 580 of
// the 901 documents under docs/ exist ONLY on unmerged branches, so the habitual search sees a third
// of the corpus. A tool nobody reaches for is indistinguishable from a tool that does not exist.
//
// WHY WE BUILD RATHER THAN ADOPT, for the part this covers. Backlog.md is the one surveyed tracker
// that keeps state in the working tree and reconciles it across branches, so the cross-branch READER
// is genuinely a thing that ships - docs/plans/2026-09-01-001-investigate-beads-comparison.md, §10,
// owns that finding and the correction it forced. What does NOT ship anywhere is the rest of this
// surface, and in Backlog.md's case that is not an omission but a decision: GitHub Issues sync and a
// lifecycle hook system were both proposed there and both closed NOT_PLANNED by its maintainer as
// out of scope. So this front door is deliberately not a tracker. It is the layer above one.
//
// ADDING A TOOL IS ADDING A ROW to COMMANDS below - the same move bin/lib/source-patterns.mjs makes
// for rules. A tool reachable only by knowing its filename is the state this file exists to end, so
// a new tool that is not in this table is not finished.
//
// AND YOU ARE MEANT TO ADD ONE. If you needed an answer and had to get it some other way, that is a
// gap in this tool and you are holding the only evidence of what it was - patch it rather than
// working around it or writing a script beside it. Migrate query-shaped shell scripts in here
// opportunistically as you touch them (`worktree-status.sh`, `issue-index.sh`, `todo-index.sh` are
// the standing candidates); scripts that DO something rather than answer something - the builds, the
// deploys, the check-* gates - stay shell, and bin/AGENTS.md owns those. docs/inflight-tool.md, "If
// it does not answer your question, change it", carries the full guidance and the three invariants.
//
// THIS IS THE ONLY FILE HERE THAT MAY CALL process.exit, and it does so once, at the bottom. The
// libraries return findings; the views render them; the exit code is a fact about a process and
// belongs at the process boundary. A library that exits has decided something that is not its to
// decide - and cannot then be called by a self-test, which is how the first cut of this split was
// caught.
//
// NOT A GATE, and deliberately not named `check-*`: bin/AGENTS.md grants that prefix to the review
// agent by pattern, and this reaches the network through `gh`.
//
// EXIT CODES: 0 ran (whatever it found), 2 cannot run - including a usage error. Self-test:
// bin/test-inflight.mjs.

import { pathToFileURL } from 'node:url'

import { perfReport, perfStart } from './lib/perf.mjs'

import { baseline, freshnessWarnings, refTips } from './lib/git.mjs'
import { cacheClear, cacheStatus, knownCaches } from './lib/cache.mjs'
import { corpusIndex, drift, findNotes, prsByBranch, stranded } from './lib/notes.mjs'
import { branchView, commitGraph, trackingGap } from './lib/branches.mjs'
import {
    formatBranch, formatCache, formatCoverage, formatDrift, formatFind, formatFlakes,
    formatSlowest, formatStranded, formatTimeline, formatWarnings,
} from './lib/views.mjs'
import { coverage, flakeCandidates, slowest, testTimeline } from './lib/codecov.mjs'
import {
    format, formatHeader, formatSection, formatTail,
    priorArt, summary as priorArtSummary, usage as priorArtUsage,
} from './lib/prior-art.mjs'

/**
 * The two flags every codecov subcommand takes, parsed in one place rather than three - and the
 * POSITIONALS with the flag values removed.
 *
 * `rest` is the part that matters. Finding the query with `args.find(a => !a.startsWith('--'))`
 * looks right and is wrong: `--branch` takes a VALUE, and a branch ref does not start with `--`
 * either, so `codecov test --branch <ref> <name>` took the REF as the search term and reported
 * "no test matching /<ref>/" - exit 0, plausible wording, wrong question answered. That is the
 * silent-wrong-answer shape bin/lib/codecov.mjs's header is written against, in the one command
 * whose whole value is being trusted during a bisect.
 */
export const cvOpts = (args) => {
    const branchAt = args.indexOf('--branch')
    // -1 IS A SENTINEL, NOT AN INDEX. Filtering on `i !== branchAt + 1` reads correctly and is
    // wrong when the flag is absent: indexOf returns -1, so branchAt + 1 is 0 and the filter drops
    // the FIRST POSITIONAL - the query itself. `codecov test <name>` then answered "give part of a
    // test name" and `codecov slow 3` silently printed 20 rows. Resolve the value's index to a
    // value no index can equal when there is no flag.
    const branchValueAt = branchAt >= 0 ? branchAt + 1 : -1
    return {
        fresh: args.includes('--fresh'),
        branch: branchAt >= 0 ? args[branchValueAt] : undefined,
        rest: args.filter((a, i) => !a.startsWith('--') && i !== branchValueAt),
    }
}

/**
 * The registry.
 *
 * `when` is the sentence an agent needs to decide whether this is the tool - it answers "should I
 * reach for this now", which a name alone cannot.
 *
 * `run` takes the remaining argv and an `emit` for streaming, and returns `{ok, reason?}`. It never
 * exits and never decides a code: a search that ran and found nothing is `ok: true`, because "no
 * hits" and "could not look" are different answers and this repo has been bitten by conflating them.
 *
 * A command may instead carry `sub`, a nested registry. That is deliberately how the word "in-flight"
 * is disambiguated rather than by renaming the tool: `inflight note drift` is unmistakably about ONE
 * NOTE, while `inflight stranded` is a question about the whole directory. The product keeps its name
 * and the vocabulary does the work - Antony's call, and the right one, because the collision was only
 * ever between two senses of a word this repo already uses for both.
 *
 * @typedef {{name: string, summary: string, when: string, usage: string,
 *            sub?: Command[],
 *            run?: (args: string[], emit: (s: string) => void) => {ok: boolean, reason?: string}}} Command
 * @type {Command[]}
 */
const COMMANDS = [
    {
        name: 'prior-art',
        summary: priorArtSummary,
        when: 'BEFORE forming any hypothesis, and before proposing anything that sounds new',
        usage: priorArtUsage,
        run: (args, emit) => {
            const byRef = args.includes('--by-ref')
            const headings = args.includes('--headings')
            const terms = args.filter((a) => a !== '--by-ref' && a !== '--headings')
            if (terms.length === 0) return { ok: false, reason: priorArtUsage }

            // Streamed per section in the default view, because a 438-ref search that prints nothing
            // until it finishes reads as a hang. --by-ref cannot stream: a cluster is a statement
            // about every section at once, so it has nothing to say until all of them are in.
            let streamed = false
            const result = priorArt(terms, byRef ? { headings } : {
                headings,
                onSection: (section, r) => {
                    if (!streamed) { emit(formatHeader(r)); streamed = true }
                    emit(formatSection(section, r))
                },
            })
            if (!result.ok) return { ok: false, reason: `prior-art: ${result.reason}` }
            if (byRef) emit(format(result, { byRef: true }))
            else emit(formatTail(result))
            return { ok: true }
        },
    },
    {
        name: 'note',
        summary: 'questions about ONE in-flight note, across every branch tip',
        when: 'you have a note or a feature name and need to know where it lives and how it varies',
        usage: `Usage: bin/inflight.mjs note <find|drift> [args...]`,
        sub: [
            {
                name: 'find',
                summary: 'which note is this, and which branches carry it',
                when: 'you know the feature but not the filename - including notes that never reached master',
                usage: `Usage: bin/inflight.mjs note find <fuzzy-name>

Substring match over every in-flight note path that exists on ANY ref, not just the checked-out one.
Measured 2026-09-01: 570 note paths exist across the refs and 165 are on origin/master, so the
working tree can show you under a third of them.

  bin/inflight.mjs note find 857
  bin/inflight.mjs note find quarantine`,
                run: (args, emit) => {
                    const query = args[0]
                    if (!query) return { ok: false, reason: 'note find: give a fuzzy name to match' }
                    const index = corpusIndex()
                    if (!index.ok) return { ok: false, reason: `note find: ${index.reason}` }
                    emit(formatWarnings(freshnessWarnings(index.baseline, index.refs.length)))
                    emit(formatFind(findNotes(index, query), query, index))
                    return { ok: true }
                },
            },
            {
                name: 'drift',
                summary: 'how one note differs across every branch tip, and what each branch is',
                when: 'before editing a shared note, and when two branches may disagree about what is open',
                usage: `Usage: bin/inflight.mjs note drift [--all] <path>

Reports only what is DIVERGENT: versions carrying content the baseline has never held. A branch that
simply has not merged recently is not drift - it is behind, it gets further behind every time anyone
edits the note, and it is nobody's finding. For the fork's most-edited note that filter removes 198
of the 274 carrying refs.

Clusters by BLOB, so identical copies are one row and the diff runs once per distinct version rather
than once per ref. Each branch is named by facts only - its PR title, else the title of a note it
carries that the baseline does not, else the branch name. Nothing is summarised or inferred.

--all also lists the behind-only versions.

  bin/inflight.mjs note drift docs/inflight/bug-857-family.md`,
                run: (args, emit) => {
                    const all = args.includes('--all')
                    const path = args.find((a) => a !== '--all')
                    if (!path) return { ok: false, reason: 'note drift: give a note path (see: note find)' }
                    // No corpus index: this is a question about one path, so it asks git that.
                    const tips = refTips()
                    emit(formatWarnings(freshnessWarnings(baseline(), tips.tips.length)))
                    const prs = prsByBranch()
                    if (!prs.ok) {
                        // gh being unavailable is not "these branches have no PR", and the drift
                        // output cannot say which it meant unless this line says it first.
                        emit(`  WARNING: ${prs.reason} - PR titles below are UNKNOWN, not absent.\n`)
                    }
                    const d = drift(path, { prs: prs.map, all })
                    if (d.ok === false) return { ok: false, reason: `note drift: ${d.reason}` }
                    emit(formatDrift(d))
                    return { ok: true }
                },
            },
        ],
    },
    {
        name: 'branch',
        summary: 'what one branch IS - its PR, its session, what it integrates, and whether anything tracks it',
        when: 'you found a branch and do not know what it is, who owns it, or whether it is safe to delete',
        usage: `Usage: bin/inflight.mjs branch <ref>

Everything one branch is, in one place:

  PR and state; whether it is pushed anywhere at all; the Claude session that PRODUCED it (from the
  commit trailer, which travels with the branch) and separately who is HOLDING its worktree right now
  (from .worktree-owner, which is local and uncommitted); the notes it carries that the baseline has
  never had; and - from the commit graph, exactly - what it integrates and what integrates it.

RELATEDNESS IS CONTAINMENT, not a heuristic: a branch is a parent when this one already contains its
tip. One rev-list per ref builds the whole map, then every relationship is a set lookup.

It also answers whether ANYTHING tracks this branch - a PR, a docs/inflight/branch-*.md, or a mention
in any note on the baseline. When nothing does, it prints the remedy rather than a finding, because a
report gets skimmed and an instruction gets acted on. An integration branch is not an orphan and is
reported as what it is.

  bin/inflight.mjs branch origin/feats/ks-streams-reconciled`,
        run: (args, emit) => {
            const ref = args[0]
            if (!ref) return { ok: false, reason: 'branch: give a ref (e.g. origin/feats/ks-streams-reconciled)' }
            const graph = commitGraph()
            if (!graph.ok) return { ok: false, reason: `branch: ${graph.reason}` }
            emit(formatWarnings(freshnessWarnings(graph.baseline, graph.refs.length)))
            const prs = prsByBranch()
            if (!prs.ok) emit(`  WARNING: ${prs.reason} - PR state below is UNKNOWN, not absent.\n`)
            const view = branchView(graph, ref, prs.map)
            if (!view.ok) return { ok: false, reason: `branch: ${view.reason}` }
            emit(formatBranch(view, trackingGap(view)))
            return { ok: true }
        },
    },
    {
        name: 'cache',
        summary: 'what is cached, how old each kind is, and what policy decides that',
        when: 'you want to know whether an answer came from the network, or why a stale one persisted',
        usage: `Usage: bin/inflight.mjs cache            what is cached and how old
       bin/inflight.mjs cache clear     delete orphans (add --all for live caches too)

Only network answers are cached. Git data never is: git is already a cache, and a corpus cache that
lived here was deleted precisely because it hid a design mistake rather than paying for itself.

Each cache kind's freshness is stated once, in \`bin/lib/cache.mjs\`, and never by a caller. The
bulk PR listing is held for 24 hours; a per-branch answer for 6. The half that matters is that an
ABSENCE is not stored at all for the per-branch kind: "this branch has no PR" is the answer that
goes stale in the dangerous direction, so it is re-asked instead of remembered. That is what let a
hook whose whole job was to refresh this cache after \`gh pr create\` be deleted - it covered one
of the ways a PR appears and none of the others.

Freshness is stored inside each file, not in its name. A timestamped filename would show age in
\`ls\` and create a new file per write - the orphan accumulation that once left 7.4MB here in a
single session.`,
        sub: [
            {
                name: 'clear',
                summary: 'delete orphaned cache files, or everything with --all',
                when: 'an orphan is holding space, or you want the next run to go to the network',
                usage: `Usage: bin/inflight.mjs cache clear [--all]

Orphans only by default - a file no current code reads. Dropping a LIVE cache is a separate ask,
because the next run then pays full price, so it takes --all.`,
                run: (args, emit) => {
                    const r = cacheClear({ all: args.includes('--all') })
                    emit(r.removed.length === 0
                        ? '  nothing to clear'
                        : `  removed ${r.removed.length} file(s), ${Math.round(r.bytes / 1024)}K: ${r.removed.join(', ')}`)
                    return { ok: true }
                },
            },
        ],
        run: (args, emit) => {
            const known = knownCaches()
            emit(formatCache(cacheStatus(known), known))
            return { ok: true }
        },
    },
    {
        name: 'codecov',
        summary: 'coverage now; and per SUBCOMMAND, the recorded outcome and wall-clock of every test per commit',
        when: 'asking WHEN a test started failing, whether it is flaky, or what the coverage is now',
        usage: `Usage: bin/inflight.mjs codecov                    coverage totals, and per-flag
       bin/inflight.mjs codecov test <fuzzy>      one test's outcome per commit - the bisect
       bin/inflight.mjs codecov flaky             tests recorded with more than one outcome
       bin/inflight.mjs codecov slow [n]          slowest tests by last recorded wall-clock

Add --fresh to any of these to bypass the 10-minute cache, and --branch <ref> to scope to one branch.

NO TOKEN AND NO SETUP: this repo is public, so Codecov's API answers unauthenticated. It works from
a fresh sandbox and from CI, which is the whole reason it is reachable from here rather than being a
dashboard somebody has to remember to open.

WHAT IT IS FOR. Codecov keeps per-test outcome and duration per commit, for longer than a CI log is
retained. That answers "which commit did this start failing at" from RECORDED history rather than by
re-running builds, and it supplies the sighting evidence docs/quarantined-tests.md demands, which is
currently assembled by hand from logs that expire.

WHAT IT IS NOT FOR. \`duration_seconds\` is test wall-clock on a shared runner, not the library's
throughput. It must never feed the throughput regression comparison - see bin/lib/codecov.mjs.`,
        sub: [
            {
                name: 'test',
                summary: "one test's recorded outcome and duration, per commit, newest first",
                when: 'a test is failing and you need the commit it changed at, not a guess',
                usage: `Usage: bin/inflight.mjs codecov test <fuzzy-name> [--branch <ref>] [--fresh]

Substring match, case-insensitive, because you almost always hold the method name and not the
fully-qualified one. Several matches are listed rather than guessed at: resolving to the wrong test
here produces a confident answer to a question nobody asked.`,
                run: (args, emit) => {
                    const opts = cvOpts(args)
                    const q = opts.rest[0]
                    if (!q) return { ok: false, reason: 'codecov test: give part of a test name' }
                    const r = testTimeline(q, opts)
                    if (!r.ok) return { ok: false, reason: `codecov test: ${r.reason}` }
                    emit(formatTimeline(r.value))
                    return { ok: true }
                },
            },
            {
                name: 'flaky',
                summary: 'tests recorded with more than one outcome - flake CANDIDATES, never a verdict',
                when: 'building the sighting evidence a quarantine entry needs, instead of re-reading CI logs',
                usage: `Usage: bin/inflight.mjs codecov flaky [--branch <ref>] [--fresh]

A candidate list. The same evidence fits a real regression that landed between two commits, which is
exactly why docs/quarantined-tests.md refuses to quarantine on a rate alone.`,
                run: (args, emit) => {
                    const r = flakeCandidates(cvOpts(args))
                    if (!r.ok) return { ok: false, reason: `codecov flaky: ${r.reason}` }
                    emit(formatFlakes(r.value))
                    return { ok: true }
                },
            },
            {
                name: 'slow',
                summary: 'the slowest tests by their most recent recorded wall-clock',
                when: 'CI wall-clock is the complaint and you need to know which tests own it',
                usage: `Usage: bin/inflight.mjs codecov slow [n] [--branch <ref>] [--fresh]

Wall-clock on a shared GitHub runner. Good for "this test owns four minutes of every run"; not a
benchmark, and never an input to a throughput comparison.`,
                run: (args, emit) => {
                    const opts = cvOpts(args)
                    const n = opts.rest.find((a) => /^\d+$/.test(a))
                    const r = slowest(n ? Number(n) : 20, opts)
                    if (!r.ok) return { ok: false, reason: `codecov slow: ${r.reason}` }
                    emit(formatSlowest(r.value))
                    return { ok: true }
                },
            },
        ],
        run: (args, emit) => {
            const r = coverage()
            if (!r.ok) return { ok: false, reason: `codecov: ${r.reason}` }
            emit(formatCoverage(r.value))
            return { ok: true }
        },
    },
    {
        name: 'stranded',
        summary: 'notes that exist on a branch and have never reached master',
        when: 'looking for knowledge that will be lost if nobody acts - the stranded-work impact',
        usage: `Usage: bin/inflight.mjs stranded

A note absent from master is not automatically stranded. Three filters run, and the middle one was
expected to do most of the work and did almost none - stated because the wrong prediction is worth
more than the right conclusion:

  present on master now                          not stranded
  its blob lives on master under another path    a rename, proven exactly - removed 1 of 405
  master's HISTORY once had this path            it landed and was git rm'd - removed 40 more

What survives is clustered by ref-set, because one workstream's notes share their refs and listing
them separately buries the finding under its own volume.`,
        run: (args, emit) => {
            const index = corpusIndex()
            if (!index.ok) return { ok: false, reason: `stranded: ${index.reason}` }
            emit(formatWarnings(freshnessWarnings(index.baseline, index.refs.length)))
            emit(formatStranded(stranded(index), index))
            return { ok: true }
        },
    },
]

/** Flatten one level, so help and lookup see `note find` as a first-class name. */
const flatten = (cmds, prefix = '') => cmds.flatMap((c) => (c.sub
    ? [{ ...c, path: `${prefix}${c.name}` }, ...flatten(c.sub, `${prefix}${c.name} `)]
    : [{ ...c, path: `${prefix}${c.name}` }]))

const ALL = flatten(COMMANDS)

/**
 * Every command path, `note drift` included.
 *
 * Exported so the self-test walks the registry instead of scraping this file with a regex. The
 * regex it replaced matched one indent level, so it saw the three top-level names and neither
 * subcommand - and two checks named "every registered command" quietly covered three fifths of them.
 */
export const COMMAND_PATHS = ALL.map((c) => c.path)

function help() {
    const width = Math.max(...ALL.map((c) => c.path.length))
    return [
        "bin/inflight.mjs - the front door for this repo's in-flight tooling.",
        '',
        'Usage: bin/inflight.mjs [--perf] <command> [args...]',
        '       bin/inflight.mjs help [<command>]',
        '',
        '--perf reports where the time went, to stderr: how many subprocesses of each kind, their',
        'total time, and the slowest single one. The cost here is almost always the COUNT rather',
        'than any one call - one `git ls-tree` is nothing, 436 of them is over a second.',
        '',
        'Commands:',
        ...ALL.flatMap((c) => [
            `  ${c.path.padEnd(width)}  ${c.summary}`,
            `  ${' '.repeat(width)}  when: ${c.when}`,
        ]),
        '',
        'Every command exits 0 when it RAN - whatever it found - and 2 when it could not run. An empty',
        'result and a search that never happened are different answers, and nothing here reports them alike.',
    ].join('\n')
}

/** Resolve argv to `{ok, output}`. Pure, so the self-test can drive it without a subprocess. */
function dispatch(argv, emit) {
    const [name, ...rest] = argv
    if (!name) return { ok: false, reason: help() }

    if (name === 'help' || name === '--help' || name === '-h') {
        if (!rest.length) return { ok: true, reason: help() }
        // Longest match first, so `help note drift` beats `help note`.
        const topic = [...ALL].sort((a, b) => b.path.length - a.path.length)
            .find((c) => rest.join(' ').startsWith(c.path))
        if (topic) return { ok: true, reason: topic.usage }
        return { ok: false, reason: `inflight: no such command '${rest.join(' ')}'\n\n${help()}` }
    }

    const top = COMMANDS.find((c) => c.name === name)
    if (!top) return { ok: false, reason: `inflight: no such command '${name}'\n\n${help()}` }
    if (!top.sub) return top.run(rest, emit)

    const child = top.sub.find((c) => c.name === rest[0])
    if (child) return child.run(rest.slice(1), emit)
    // A PARENT MAY ALSO BE A COMMAND. `cache` with no subcommand reports status; `note` alone does
    // not mean anything, so it has no `run` and still falls through to its usage. Without this a
    // parent's own run was unreachable, which is how `cache` printed its help instead of answering.
    if (!rest.length && top.run) return top.run(rest, emit)
    const which = rest[0] ? `'${name} ${rest[0]}'` : `'${name}' on its own`
    return { ok: false, reason: `inflight: no such command ${which}\n\n${top.usage}` }
}

// Guarded so this file can be imported for its registry without running a command. It remains the
// only file here permitted to exit the process; being importable is what lets the self-test assert
// on the registry rather than on a regex over the source.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    const argv = process.argv.slice(2)
    // Stripped before dispatch, so no command has to know the flag exists.
    const perf = argv.includes('--perf')
    if (perf) perfStart()

    const { ok, reason } = dispatch(argv.filter((a) => a !== '--perf'), (s) => console.log(s))
    if (reason) (ok ? console.log : console.error)(reason)
    // stderr, so a caller piping stdout gets exactly what it would without the flag.
    if (perf) console.error(perfReport())
    process.exit(ok ? 0 : 2)
}
