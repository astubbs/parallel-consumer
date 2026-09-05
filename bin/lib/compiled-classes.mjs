// Copyright (C) 2026 Antony Stubbs and contributors
//
// What did the COMPILER see? - class facts read from bytecode, not from Java source text.
//
// WHY NOT PARSE THE SOURCE. The integration-shard coverage gate has to answer "which classes ought
// to have produced a failsafe report", which needs three facts per class: is it abstract, what does
// it extend, and does it (or an ancestor) carry a JUnit test annotation. Answering those from `.java`
// text means re-implementing a Java parser in fragments, and every defect that gate has ever had was
// exactly that: a file-wide annotation grep that exempted a class because ONE method carried the
// annotation; a class name taken from the FILENAME, blind to the other three top-level classes in
// the same file; an `extends` scan that matched the word inside a javadoc sentence and would have
// demanded a report for a helper class that never runs - a permanent false RED.
//
// The compiler already did that parsing, correctly, and `javap` reads the result. Every one of those
// failure shapes stops existing rather than being handled:
//
//   comments and strings containing "extends"  - not in bytecode
//   generic bounds `<T extends Foo>`           - erased; `super_class` is the real supertype
//   `@Nested` and other inner classes          - compile to `Outer$Inner`, filtered by name
//   several top-level classes in one file      - each is its own `.class`; filenames never consulted
//   same simple name in two modules            - `this_class` is fully qualified
//   `abstract`                                 - an access flag, not a keyword to match
//   multi-level inheritance                    - `super_class` is followed exactly, to closure
//
// THE TRADE, stated because it is real: this can only run AFTER compilation, so it is useless as a
// pre-push tree gate and its consumer exits 3 ("nothing in scope") when no classes are built. That is
// the right trade here - the gate it serves already runs after `verify`, beside the failsafe reports
// it compares against.

import { execFileSync } from 'node:child_process'
import { existsSync, readdirSync, statSync } from 'node:fs'
import { join } from 'node:path'

// The annotations that make a method a test to JUnit 5, as bytecode descriptors. `@Nested` is
// deliberately NOT here: a nested class's results are reported inside its ENCLOSING top-level class's
// XML, so it never needs a report of its own, and demanding one would be a false RED.
//
// `@ParameterizedTest` lives in `org.junit.jupiter.PARAMS`, not `.api` - the first draft listed it
// under `.api` and silently un-guarded MultiTopicTest, whose only tests are parameterised. The
// self-test pins that package.
const TEST_DESCRIPTORS = [
    'Lorg/junit/jupiter/api/Test;',
    'Lorg/junit/jupiter/api/RepeatedTest;',
    'Lorg/junit/jupiter/api/TestFactory;',
    'Lorg/junit/jupiter/api/TestTemplate;',
    'Lorg/junit/jupiter/params/ParameterizedTest;',
    // JUnit 4, still reachable through vintage. Cheap to include; expensive to discover the omission.
    'Lorg/junit/Test;',
]
const TAG_ANNOTATION = 'org.junit.jupiter.api.Tag'

export function javapBinary() {
    const home = process.env.JAVA_HOME
    if (home && existsSync(join(home, 'bin', 'javap'))) return join(home, 'bin', 'javap')
    return 'javap'
}

/** Every `.class` file under `dir`, as fully-qualified class names. Inner classes included. */
export function classFilesUnder(dir) {
    const out = []
    const walk = (abs, pkg) => {
        let entries
        try { entries = readdirSync(abs) } catch { return }
        for (const e of entries) {
            const p = join(abs, e)
            let st
            try { st = statSync(p) } catch { continue }
            if (st.isDirectory()) walk(p, pkg ? `${pkg}.${e}` : e)
            else if (e.endsWith('.class')) out.push(pkg ? `${pkg}.${e.slice(0, -6)}` : e.slice(0, -6))
        }
    }
    walk(dir, '')
    return out
}

/**
 * Parse `javap -v -p` output into one record per class.
 *
 * Split on `this_class:` rather than on a blank line or on `Compiled from`, because a package-private
 * class prints no `Compiled from` header in some javap versions and constant-pool dumps are full of
 * blank lines. Exported for the self-test, which pins this against real javap output rather than a
 * hand-written approximation of it.
 */
export function parseJavap(text) {
    const records = []
    const blocks = text.split(/^Classfile /m)
    for (const block of blocks) {
        const thisClass = block.match(/^\s*this_class:.*\/\/\s*(\S+)\s*$/m)
        if (!thisClass) continue
        const superClass = block.match(/^\s*super_class:.*\/\/\s*(\S+)\s*$/m)
        // The class's own `flags:` line is the FIRST in the block - javap prints it between the
        // version fields and `this_class:`; every later `flags:` belongs to a field or a method. The
        // first draft read the first one AFTER `super_class:` and so reported an abstract base as
        // concrete, which would have demanded a report from a class that can never run.
        const classFlags = block.match(/^\s*flags:\s*\(0x[0-9a-fA-F]+\)([^\n]*)$/m)
        const { types, tags } = classLevelAnnotations(block)
        records.push({
            name: thisClass[1].replace(/\//g, '.'),
            superName: superClass ? superClass[1].replace(/\//g, '.') : null,
            isAbstract: /ACC_ABSTRACT/.test(classFlags ? classFlags[1] : ''),
            isInterface: /ACC_INTERFACE/.test(classFlags ? classFlags[1] : ''),
            declaresTest: TEST_DESCRIPTORS.some((d) => block.includes(d)),
            classAnnotations: types,
            classTags: tags,
        })
    }
    return records
}

// CLASS-LEVEL annotations only - the `RuntimeVisibleAnnotations:` block that javap prints at column
// 0, after the methods. A method's or field's block is indented under its member, so the column is
// what separates "this class is tagged chaos" from "one method in it is". That distinction is the
// whole of the first review round's finding: a file-wide grep read a method-level tag as exempting
// the class, and un-guarded LoadTest.
function classLevelAnnotations(block) {
    const types = []
    const tags = []
    const m = block.match(/^Runtime(?:In)?[Vv]isibleAnnotations:\n((?:[ \t]+[^\n]*\n?)+)/gm)
    for (const section of m ?? []) {
        let current = null
        for (const line of section.split('\n')) {
            const type = line.match(/^\s+([A-Za-z_$][\w.$]*)\(?\s*$/)
            if (type && !/^\d+:/.test(type[1])) { current = type[1]; types.push(current); continue }
            const value = line.match(/^\s+value="([^"]*)"/)
            if (value && current === TAG_ANNOTATION) tags.push(value[1])
        }
    }
    return { types, tags }
}

/** Run `javap -v -p` over `names` on `classpath`, in batches, and return parsed records. */
export function inspect(names, classpath, { javap = javapBinary(), batch = 60 } = {}) {
    const out = []
    for (let i = 0; i < names.length; i += batch) {
        const slice = names.slice(i, i + batch)
        let text
        try {
            text = execFileSync(javap, ['-v', '-p', '-cp', classpath, ...slice], {
                encoding: 'utf8', maxBuffer: 512 * 1024 * 1024, stdio: ['ignore', 'pipe', 'ignore'],
            })
        } catch (e) {
            // javap exits non-zero when ANY name fails to resolve, but still prints the ones that
            // did. Keep that partial output rather than losing the whole batch - a class that cannot
            // be read simply never enters the index, and the caller reports it as unresolved.
            text = (e.stdout || '').toString()
        }
        out.push(...parseJavap(text))
    }
    return out
}

/**
 * Index every class in `names`, then follow `super_class` to closure so inherited tests are found
 * however deep the chain is, and stop at the first supertype outside the classpath (a library type).
 */
export function buildIndex(names, classpath, opts = {}) {
    const index = new Map()
    let frontier = names
    for (let depth = 0; frontier.length && depth < 20; depth += 1) {
        for (const r of inspect(frontier, classpath, opts)) if (!index.has(r.name)) index.set(r.name, r)
        const next = new Set()
        for (const r of index.values()) {
            if (r.superName && r.superName !== 'java.lang.Object' && !index.has(r.superName)) next.add(r.superName)
        }
        frontier = [...next]
    }
    resolveMetaTags(index, classpath, opts)
    return index
}

// A tag can arrive through an annotation TYPE rather than on the class: `@Quarantined` is itself
// annotated `@Tag("quarantined")`, so a quarantined class carries only `Lbz/.../Quarantined;` and
// JUnit finds the tag one hop away, on the annotation. Read it the same way - javap the annotation
// types the indexed classes use and take THEIR class-level tags - rather than hard-coding the name,
// so the next meta-annotated group works without anyone remembering this function exists.
function resolveMetaTags(index, classpath, opts) {
    const wanted = new Set()
    for (const r of index.values()) {
        for (const a of r.classAnnotations) if (a !== TAG_ANNOTATION && !index.has(a)) wanted.add(a)
    }
    const meta = new Map()
    for (const r of inspect([...wanted], classpath, opts)) meta.set(r.name, r.classTags)
    for (const r of index.values()) {
        const inherited = r.classAnnotations.flatMap((a) => meta.get(a) ?? [])
        r.effectiveTags = [...new Set([...r.classTags, ...inherited])]
    }
}

/** Does this class, or anything it inherits from, declare a JUnit test method? */
export function inheritsTest(index, name, seen = new Set()) {
    const r = index.get(name)
    if (!r || seen.has(name)) return false
    seen.add(name)
    if (r.declaresTest) return true
    return r.superName ? inheritsTest(index, r.superName, seen) : false
}

const NESTED_ANNOTATION = 'org.junit.jupiter.api.Nested'

/**
 * Would JUnit produce a report for this top-level class? Its own tests, inherited tests, or tests
 * inside a `@Nested` inner class - Jupiter runs those under the ENCLOSING class and writes them into
 * its XML, so the outer class must be demanded even when its own bytecode carries no test at all.
 * A plain (non-@Nested) inner class is not run by failsafe, whose default excludes drop `*$*`.
 */
export function producesReport(index, name) {
    if (inheritsTest(index, name)) return true
    for (const [inner, r] of index) {
        if (!inner.startsWith(`${name}$`)) continue
        if (r.classAnnotations.includes(NESTED_ANNOTATION) && producesReport(index, inner)) return true
    }
    return false
}

/**
 * The classes that MUST have produced a failsafe report: concrete, not an inner class, matching the
 * package pattern failsafe collects, carrying test methods of their own or by inheritance, and not
 * tagged into a group the run excluded.
 */
export function classesRequiringReports(index, packageRe, excludedTags = []) {
    const excluded = new Set(excludedTags)
    const required = []
    for (const [name, r] of index) {
        if (r.isAbstract || r.isInterface) continue
        if (name.includes('$')) continue            // inner/@Nested - reported under the enclosing class
        if (!packageRe.test(name)) continue
        if (!producesReport(index, name)) continue  // a helper that happens to live in the same package
        if ((r.effectiveTags ?? r.classTags).some((t) => excluded.has(t))) continue
        required.push(name)
    }
    return required.sort()
}
