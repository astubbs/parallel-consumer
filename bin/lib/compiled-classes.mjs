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
//   which METHODS a tag is on                  - a method's annotations are indented under it
//
// TAG SCOPE IS READ AT EVERY LEVEL JUNIT READS IT, because a report exists only if at least one test
// in the class survives the run's excluded groups. Three places a tag can sit, and the first cut of
// this module read only the first: on the concrete class; on an ANCESTOR class (`@Tag` is
// `@Inherited`, so a `@Tag("chaos")` base filters every subclass); and on the METHODS, where a class
// whose every test method is tagged out - or inherits only tagged-out tests - runs nothing and
// writes no XML. The second Codex review (astubbs#442) found the method case: the gate would have
// demanded a report from a class failsafe never ran, a permanent false RED on the catch-all shard.
// No class in the tree had that shape on the day; the guard is for the one that will.
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
const TEST_ANNOTATIONS = new Set([
    'org.junit.jupiter.api.Test',
    'org.junit.jupiter.api.RepeatedTest',
    'org.junit.jupiter.api.TestFactory',
    'org.junit.jupiter.api.TestTemplate',
    'org.junit.jupiter.params.ParameterizedTest',
    // JUnit 4, still reachable through vintage. Cheap to include; expensive to discover the omission.
    'org.junit.Test',
])
const TAG_ANNOTATION = 'org.junit.jupiter.api.Tag'

/** A JDK tool by name: JAVA_HOME's copy when one is set and present, otherwise whatever PATH has. */
export function jdkBinary(name) {
    const home = process.env.JAVA_HOME
    if (home && existsSync(join(home, 'bin', name))) return join(home, 'bin', name)
    return name
}

export const javapBinary = () => jdkBinary('javap')

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
        const testMethods = memberAnnotations(block).filter((m) => m.types.some((t) => TEST_ANNOTATIONS.has(t)))
        records.push({
            name: thisClass[1].replace(/\//g, '.'),
            superName: superClass ? superClass[1].replace(/\//g, '.') : null,
            isAbstract: /ACC_ABSTRACT/.test(classFlags ? classFlags[1] : ''),
            isInterface: /ACC_INTERFACE/.test(classFlags ? classFlags[1] : ''),
            declaresTest: testMethods.length > 0,
            testMethods,               // [{ types, tags }] - one per method carrying a test annotation
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
    const m = block.match(/^Runtime(?:In)?[Vv]isibleAnnotations:\n((?:[ \t]+[^\n]*\n?)+)/gm)
    return annotationsIn((m ?? []).flatMap((section) => section.split('\n')))
}

// Annotation type names and @Tag values from the lines of one or more `Runtime*Annotations:` blocks.
// Indent-agnostic, so the same reader serves a class-level block (entries at column 2) and a
// member-level one (entries at column 6).
function annotationsIn(lines) {
    const types = []
    const tags = []
    let current = null
    for (const line of lines) {
        const type = line.match(/^\s+([A-Za-z_$][\w.$]*)\(?\s*$/)
        if (type && !/^\d+:/.test(type[1])) { current = type[1]; types.push(current); continue }
        const value = line.match(/^\s+value="([^"]*)"/)
        if (value && current === TAG_ANNOTATION) tags.push(value[1])
    }
    return { types, tags }
}

// MEMBER-level annotations: one record per field or method in the `{ ... }` body. javap prints each
// member's signature at two spaces and its attributes - `descriptor:`, `flags:`, `Code:`,
// `RuntimeVisibleAnnotations:` - at four, with the annotation entries nested below that. So a member
// starts at `^  \S`, its annotation block starts at `^    Runtime...Annotations:`, and the next
// four-space attribute ends it. This is the reading that separates "one test in this class is tagged
// performance" from "EVERY test in this class is", which no class-level view can tell apart.
function memberAnnotations(block) {
    const open = block.indexOf('\n{\n')
    if (open < 0) return []
    const close = block.indexOf('\n}\n', open)
    const body = block.slice(open + 3, close < 0 ? undefined : close).split('\n')
    const members = []
    let current = null
    let inAnnotations = false
    for (const line of body) {
        if (/^  \S/.test(line)) { current = { lines: [] }; members.push(current); inAnnotations = false; continue }
        if (!current) continue
        if (/^    Runtime(?:In)?[Vv]isibleAnnotations:/.test(line)) { inAnnotations = true; continue }
        if (/^    \S/.test(line)) { inAnnotations = false; continue }
        if (inAnnotations) current.lines.push(line)
    }
    return members.map((m) => annotationsIn(m.lines))
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
            // Two different failures share this catch, and only one is tolerable. javap RAN and
            // exited non-zero because a name did not resolve: it still printed the classes that did,
            // so keep that partial output and let the caller report the rest as unresolved. javap
            // did NOT run - missing binary (ENOENT), a bad JAVA_HOME, no exit status at all - and
            // swallowing that left an empty index, zero required classes, and a gate that printed
            // "every class produced a report" over nothing. That is the green-while-red this whole
            // module exists to prevent, so it is rethrown and the gate exits 2.
            if (e.status == null) throw e
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
        const used = [...r.classAnnotations, ...r.testMethods.flatMap((m) => m.types)]
        for (const a of used) if (a !== TAG_ANNOTATION && !index.has(a)) wanted.add(a)
    }
    const meta = new Map()
    for (const r of inspect([...wanted], classpath, opts)) meta.set(r.name, r.classTags)
    // An annotation type may already BE in the index - the gate indexes every compiled test class,
    // and a project-defined annotation like @Quarantined compiles into the same tree. Its tags are
    // then on its index record, not in `meta`; the first draft only looked in `meta` and silently
    // dropped every meta-annotated group the moment the candidate set widened.
    const tagsOf = (a) => (index.get(a) ?? { classTags: meta.get(a) ?? [] }).classTags
    for (const r of index.values()) {
        const inherited = r.classAnnotations.flatMap(tagsOf)
        r.effectiveTags = [...new Set([...r.classTags, ...inherited])]
        // A method can carry @Quarantined too - the repo's own annotation targets METHOD as well as
        // TYPE - so a method's tags resolve through the same one hop.
        for (const m of r.testMethods) m.effectiveTags = [...new Set([...m.tags, ...m.types.flatMap(tagsOf)])]
    }
}

/**
 * Every class-level tag JUnit applies to `name`: its own, plus each ancestor's. `@Tag` is
 * `@Inherited`, so a `@Tag("chaos")` on a base class filters every concrete subclass - and JUnit
 * finds a meta-annotated `@Quarantined` on an ancestor by the same walk.
 */
export function classTagsInScope(index, name, seen = new Set()) {
    const r = index.get(name)
    if (!r || seen.has(name)) return []
    seen.add(name)
    const own = r.effectiveTags ?? r.classTags
    return [...new Set([...own, ...(r.superName ? classTagsInScope(index, r.superName, seen) : [])])]
}

/**
 * Does this class, or anything it inherits from, declare a test method that SURVIVES the excluded
 * groups? A class whose every test method is tagged out runs nothing under failsafe and writes no
 * report, so demanding one is a false RED. With no exclusions this is `inheritsTest`.
 */
export function hasRunnableTest(index, name, excluded = new Set(), seen = new Set()) {
    const r = index.get(name)
    if (!r || seen.has(name)) return false
    seen.add(name)
    if (r.testMethods.some((m) => !(m.effectiveTags ?? m.tags).some((t) => excluded.has(t)))) return true
    return r.superName ? hasRunnableTest(index, r.superName, excluded, seen) : false
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
export function producesReport(index, name, excluded = new Set()) {
    if (hasRunnableTest(index, name, excluded)) return true
    for (const [inner, r] of index) {
        if (!inner.startsWith(`${name}$`)) continue
        if (!r.classAnnotations.includes(NESTED_ANNOTATION)) continue
        if (classTagsInScope(index, inner).some((t) => excluded.has(t))) continue
        if (producesReport(index, inner, excluded)) return true
    }
    return false
}

/** Is this class, or any class it inherits from, in a package matching `packageRe`? */
export function inIntegrationLineage(index, name, packageRe, seen = new Set()) {
    if (packageRe.test(name)) return true
    const r = index.get(name)
    if (!r || !r.superName || seen.has(name)) return false
    seen.add(name)
    return inIntegrationLineage(index, r.superName, packageRe, seen)
}

/**
 * The classes that MUST have produced a failsafe report: concrete, not an inner class, carrying test
 * methods of their own or by inheritance, not tagged into a group the run excluded, and in the
 * integration LINEAGE - either in a package failsafe collects, or extending a class that is.
 *
 * The lineage half is the case the shell comment always promised and the first cut did not deliver:
 * a test class moved OUT of an integrationTest package (still extending BrokerIntegrationTest) is
 * silently uncollected by failsafe, and a gate that pre-filtered its candidates by package could
 * never demand it. Indexing every compiled test class and asking "does your ancestry reach an
 * integration package" is what makes the stderr text true.
 */
export function classesRequiringReports(index, packageRe, excludedTags = []) {
    const excluded = new Set(excludedTags)
    const required = []
    for (const [name, r] of index) {
        if (r.isAbstract || r.isInterface) continue
        if (name.includes('$')) continue            // inner/@Nested - reported under the enclosing class
        if (!inIntegrationLineage(index, name, packageRe)) continue
        if (classTagsInScope(index, name).some((t) => excluded.has(t))) continue  // tagged out, own or inherited
        if (!producesReport(index, name, excluded)) continue  // a helper, or every test tagged out
        required.push(name)
    }
    return required.sort()
}
