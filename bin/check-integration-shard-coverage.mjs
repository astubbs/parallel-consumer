#!/usr/bin/env node
//
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Did every integration test class that EXISTS produce a failsafe report somewhere?
//
// The integration lane is split into a named heavy shard and a catch-all defined by subtraction
// (bin/ci-integration-test.sh owns that arrangement). A test that runs in neither shard fails
// nothing - it just stops running, and nothing goes red to say so. This is the guard for that: it
// takes the classes the COMPILER produced, keeps the ones JUnit would treat as test classes in a
// package failsafe collects, and demands a failsafe report for each, in whichever shard.
//
// BYTECODE, NOT SOURCE, and the reason is the whole history of this check. Its first form was a shell
// scan of the .java files, and every defect it ever had was a text-matching defect: a file-wide
// annotation grep that read one method's @Tag as exempting the class; a class name taken from the
// filename, blind to the other three top-level classes in the file; a bare `extends` grep that would
// have matched a javadoc sentence and demanded a report for a helper that can never run. Each fix was
// another fragment of a Java parser written in grep. `bin/lib/compiled-classes.mjs` reads what javac
// already decided - abstract is a flag, the supertype is a field, inner classes are named `Outer$Inner`,
// @Tag values are annotation attributes - so those shapes stop existing rather than being handled.
//
// EXCLUDED GROUPS come from the caller, not from a copy. The gating run passes
// -Dexcluded.groups=<list>; the same list is passed here, so a class tagged into an excluded group is
// not demanded. A tag can arrive through a meta-annotated type (@Quarantined carries
// @Tag("quarantined")), and the library resolves that by reading the annotation type.
//
// Runs AFTER `verify`, beside the reports - it needs target/test-classes and target/failsafe-reports.
// Exit codes follow the bin/ convention: 0 ran and every required class has a report; 1 ran and at
// least one does not; 3 nothing in scope (no compiled classes or no reports - a laptop with no build,
// which check-all.sh renders as a visible skip rather than a pass). SHARD_COVERAGE_ROOT points the
// walk at a fixture tree for the self-test, the way CHAOS_SHARDS_CHECK_ROOT does for the chaos gate.

import { readdirSync, statSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { buildIndex, classFilesUnder, classesRequiringReports } from './lib/compiled-classes.mjs'
import { invokedDirectly } from './lib/invoked-directly.mjs'
import { heavyClassesFromScript } from './lib/shard-script.mjs'

const HERE = dirname(fileURLToPath(import.meta.url))
const ROOT = process.env.SHARD_COVERAGE_ROOT || join(HERE, '..')

// The package pattern failsafe collects. The pom's <includes> is `**/integrationTest*/**`, which as a
// package is any segment starting `integrationTest`.
export const INTEGRATION_PACKAGE = /(^|\.)integrationTest[^.]*\./

function arg(name, fallback) {
    const i = process.argv.indexOf(name)
    return i > -1 && process.argv[i + 1] !== undefined ? process.argv[i + 1] : fallback
}

/** Every directory named `test-classes` under a `target/`, and every `TEST-*.xml` under a `failsafe-reports/`. */
export function walk(root) {
    const classDirs = []
    const reports = []
    const visit = (dir) => {
        let entries
        try { entries = readdirSync(dir) } catch { return }
        for (const e of entries) {
            // `.claude` holds this repo's sibling worktrees; walking into them unions every
            // branch's target/ trees, so stale reports could satisfy a demand and another branch's
            // classes could be demanded here.
            if (e === '.git' || e === 'node_modules' || e === '.claude') continue
            const p = join(dir, e)
            let st
            try { st = statSync(p) } catch { continue }
            if (!st.isDirectory()) {
                if (/\/failsafe-reports$/.test(dir) && /^TEST-.*\.xml$/.test(e)) reports.push(p)
                continue
            }
            if (e === 'test-classes' && /\/target$/.test(dir)) { classDirs.push(p); continue }
            visit(p)
        }
    }
    visit(root)
    return { classDirs, reports }
}

/** Fully-qualified class names that have a report: `TEST-<fqcn>.xml`. */
export function reportedClasses(reports) {
    return new Set(reports.map((p) => p.replace(/^.*\/TEST-/, '').replace(/\.xml$/, '')))
}

export function evaluate({ classDirs, reports, heavy, excludedGroups }) {
    const classpath = classDirs.join(':')
    // EVERY compiled test class, not only those in an integration package: the lineage rule in
    // classesRequiringReports needs to see a test class that was moved out of the package but still
    // extends an integration base, which a package pre-filter here would hide from it.
    const candidates = classDirs.flatMap(classFilesUnder)
    const index = buildIndex(candidates, classpath)
    const required = classesRequiringReports(index, INTEGRATION_PACKAGE, excludedGroups)
    const reported = reportedClasses(reports)
    const heavySet = new Set(heavy)
    const missing = required.filter((fqcn) => !reported.has(fqcn))
    return {
        candidates: candidates.length,
        indexed: index.size,
        required,
        reported: reported.size,
        // A named heavy class missing from THIS shard's reports is the heavy shard's own membership
        // check to catch, not this one's - so it is reported separately rather than as "ran nowhere".
        missing: missing.filter((n) => !heavySet.has(n.split('.').pop())),
        missingHeavy: missing.filter((n) => heavySet.has(n.split('.').pop())),
    }
}

function main() {
    const heavy = (arg('--heavy-classes', '') || (heavyClassesFromScript() ?? []).join(',')).split(',').filter(Boolean)
    const excludedGroups = arg('--excluded-groups', '').split(',').map((s) => s.trim()).filter(Boolean)
    // OPT-IN, like SHARD_BALANCE_NETWORK: bin/ci-integration-test.sh sets SHARD_COVERAGE_ENFORCE=1
    // after the run whose reports this must reconcile, and the self-test points SHARD_COVERAGE_ROOT
    // at a fixture tree. Anywhere else - check-all.sh on a laptop with a partial local integration
    // run - the reports on disk are not a run this gate can judge, and demanding all of them from
    // a handful is a false RED that teaches people to ignore the gate.
    if (!process.env.SHARD_COVERAGE_ENFORCE && !process.env.SHARD_COVERAGE_ROOT) {
        console.log('check-integration-shard-coverage: reconciles a COMPLETE integration run, which is opt-in - nothing in scope')
        console.log('  bin/ci-integration-test.sh sets SHARD_COVERAGE_ENFORCE=1 after the run it gates; SHARD_COVERAGE_ROOT points at a fixture tree.')
        process.exit(3)
    }
    const { classDirs, reports } = walk(ROOT)

    if (classDirs.length === 0) {
        console.log('check-integration-shard-coverage: no target/test-classes anywhere - not built, nothing in scope')
        process.exit(3)
    }
    if (reports.length === 0) {
        console.log('check-integration-shard-coverage: no failsafe reports anywhere - integration tests have not run, nothing in scope')
        process.exit(3)
    }

    let r
    try {
        r = evaluate({ classDirs, reports, heavy, excludedGroups })
    } catch (e) {
        // inspect() rethrows when javap did not run at all (ENOENT, bad JAVA_HOME). That is "could
        // not run", exit 2 - never an uncaught stack trace, and never the exit 0 an empty index
        // would otherwise have produced.
        console.error(`check-integration-shard-coverage: FAILED to run javap - ${e.message}. Is a JDK on JAVA_HOME or PATH? Nothing can be demanded, so nothing is being guarded.`)
        process.exit(2)
    }
    // Candidates but nothing indexed means javap read nothing - a missing or broken JDK, not an
    // empty suite. An empty required set from that state would print "every class reported" over
    // nothing, so it is "could not run", never a pass.
    if (r.candidates > 0 && r.indexed === 0) {
        console.error(`check-integration-shard-coverage: FAILED to inspect any of ${r.candidates} compiled classes - is javap on JAVA_HOME or PATH? Nothing can be demanded, so nothing is being guarded.`)
        process.exit(2)
    }
    console.log(`check-integration-shard-coverage: ${r.candidates} compiled test classes, ${r.required.length} in integration lineage must report, ${r.reported} reports found`)
    if (excludedGroups.length) console.log(`  excluded groups honoured: ${excludedGroups.join(', ')}`)

    if (r.missing.length === 0) {
        console.log('ok:   every integration test class produced a failsafe report')
        process.exit(0)
    }
    console.error('check-integration-shard-coverage: FAILED - test class(es) the compiler produced that NO shard ran:')
    for (const n of r.missing) console.error(`    ${n}`)
    console.error('  Each is a compiled class with JUnit test methods (own or inherited) whose ancestry reaches an')
    console.error('  integrationTest package, and produced no failsafe report anywhere. Most likely it sits outside')
    console.error('  a package failsafe collects - moved, or created beside its base - so it silently never runs, in')
    console.error('  this arrangement or the single-job one. Check the package first.')
    if (r.missingHeavy.length) {
        console.error(`  (also absent, but named in HEAVY_CLASSES so the heavy shard owns them: ${r.missingHeavy.join(', ')})`)
    }
    process.exit(1)
}

if (invokedDirectly(import.meta.url)) main()
