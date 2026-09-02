// Copyright (C) 2026 Antony Stubbs and contributors
//
// WHERE THE TIME WENT, when you ask for it and never otherwise.
//
// Every expensive thing this tool does is a subprocess, and the cost is almost always the COUNT
// rather than any single call: one `ls-tree` is nothing, 436 of them is 1.3s. So the useful report
// is per-subcommand - how many, how long in total, and the slowest single one - not a flame graph.
//
// That framing has already caught two design mistakes here. `note drift` was building the whole
// corpus (436 ls-tree calls) to answer a question about ONE path, and `branchFacts` was rescanning
// the corpus per branch. Both were invisible in the output and obvious in a call count.
//
// OFF BY DEFAULT AND FREE WHEN OFF: recording is a Map write per subprocess, and nothing is
// rendered unless --perf asked. It writes to stderr so a caller piping stdout is unaffected.

const calls = new Map()
let started = null

/** Begin a run. Idempotent, so a library that starts one twice does not reset the clock. */
export function perfStart() {
    if (started === null) started = Date.now()
}

/**
 * Discard everything recorded so far and start again.
 *
 * The recorder is module state, which is right for a CLI - one process, one run - and wrong for
 * anything that measures twice in one process. The self-test does exactly that, and without this
 * its counts were whatever the checks before it happened to leave behind.
 */
export function perfReset() {
    calls.clear()
    started = Date.now()
}

/** Record one subprocess. `kind` is the command plus its subcommand - `git ls-tree`, `gh search`. */
export function perfRecord(kind, ms) {
    const e = calls.get(kind) ?? { n: 0, total: 0, slowest: 0 }
    e.n += 1
    e.total += ms
    if (ms > e.slowest) e.slowest = ms
    calls.set(kind, e)
}

/**
 * The report, slowest total first - because the question is always "what should I stop doing",
 * and that is answered by total time rather than by the worst single call.
 */
export function perfReport() {
    if (started === null) return ''
    const wall = Date.now() - started
    const rows = [...calls.entries()].sort((a, b) => b[1].total - a[1].total)
    const subprocessMs = rows.reduce((t, [, e]) => t + e.total, 0)
    const totalCalls = rows.reduce((t, [, e]) => t + e.n, 0)

    const out = ['', `--- perf: ${wall}ms wall, ${totalCalls} subprocess(es) totalling ${subprocessMs}ms ---`]
    const width = Math.max(12, ...rows.map(([k]) => k.length))
    for (const [kind, e] of rows) {
        out.push(`  ${kind.padEnd(width)}  ${String(e.n).padStart(4)} call(s)  `
            + `${String(e.total).padStart(6)}ms total  ${String(e.slowest).padStart(5)}ms slowest`)
    }
    // The gap is real work - JSON parsing, Map building, rendering - and naming it stops a reader
    // concluding the subprocess numbers are the whole story.
    out.push(`  ${'(in-process)'.padEnd(width)}  ${''.padStart(4)}           ${String(wall - subprocessMs).padStart(6)}ms total`)
    return out.join('\n')
}
