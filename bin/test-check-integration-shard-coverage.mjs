#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Self-test for bin/check-integration-shard-coverage.mjs and bin/lib/compiled-classes.mjs.
//
// REAL BYTECODE, NOT A HAND-WRITTEN JAVAP TRANSCRIPT. Every fixture below is Java source compiled
// with the JDK's own javac into a throwaway tree, then read back through the real javap - so what is
// pinned is the contract with the compiler and javap, not this test's guess at their output. Stub
// annotation types are declared under the REAL JUnit package names (org.junit.jupiter.api.Test and
// friends) so the fixtures compile with no dependency and produce the exact descriptors the library
// matches on. A test that compiled against a fake `Test` in the wrong package would pass while the
// gate missed every real test class - which is how @ParameterizedTest (org.junit.jupiter.PARAMS) was
// nearly missed in the first draft, and why that package is pinned here by name.
//
// EACH CASE IS A SHAPE THE SOURCE-SCANNING PREDECESSOR GOT WRONG, or was asked about in review and
// could not answer without arguing: a method-level @Tag beside plain tests (must NOT exempt the
// class); a class-level @Tag in an excluded group (must exempt it); a meta-annotated group such as
// @Quarantined; four top-level classes in one file; tests inherited across two hops through an
// undecorated intermediate; a javadoc sentence containing "extends AnnotatedBase" on a helper (must
// NOT be demanded); a generic bound `<T extends Base>` (must NOT be read as inheritance); a @Nested
// inner class (reported under its enclosing class, never demanded alone); an abstract base; the same
// simple name in two packages; and @ParameterizedTest as the class's only test.
//
// Needs a JDK on JAVA_HOME or PATH. Exits 2 when javac is not available, never 0 - a self-test that
// could not compile its fixtures has proven nothing.

import { execFileSync, spawnSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { buildIndex, classFilesUnder, classesRequiringReports, inheritsTest, javapBinary, jdkBinary, parseJavap } from './lib/compiled-classes.mjs'

const HERE = dirname(fileURLToPath(import.meta.url))
const GATE = join(HERE, 'check-integration-shard-coverage.mjs')

let failures = 0
const check = (desc, actual, expected) => {
    if (JSON.stringify(actual) === JSON.stringify(expected)) console.log(`ok    ${desc}`)
    else { console.error(`FAIL  ${desc}\n        expected: ${JSON.stringify(expected)}\n        actual:   ${JSON.stringify(actual)}`); failures++ }
}
const contains = (desc, haystack, needle) => {
    if (haystack.includes(needle)) console.log(`ok    ${desc}`)
    else { console.error(`FAIL  ${desc}\n        expected to contain: ${JSON.stringify(needle)}\n        actual:\n${haystack}`); failures++ }
}

// --- fixture sources -------------------------------------------------------------------------------
// Stub annotation types under the real JUnit package names, so descriptors match the real thing.
const STUBS = {
    'org/junit/jupiter/api/Test.java': 'package org.junit.jupiter.api; import java.lang.annotation.*; @Retention(RetentionPolicy.RUNTIME) public @interface Test {}',
    'org/junit/jupiter/api/RepeatedTest.java': 'package org.junit.jupiter.api; import java.lang.annotation.*; @Retention(RetentionPolicy.RUNTIME) public @interface RepeatedTest { int value(); }',
    'org/junit/jupiter/api/Nested.java': 'package org.junit.jupiter.api; import java.lang.annotation.*; @Retention(RetentionPolicy.RUNTIME) public @interface Nested {}',
    'org/junit/jupiter/api/Tag.java': 'package org.junit.jupiter.api; import java.lang.annotation.*; @Retention(RetentionPolicy.RUNTIME) public @interface Tag { String value(); }',
    'org/junit/jupiter/params/ParameterizedTest.java': 'package org.junit.jupiter.params; import java.lang.annotation.*; @Retention(RetentionPolicy.RUNTIME) public @interface ParameterizedTest {}',
    // Meta-annotated group, exactly as the repo's own Quarantined is.
    'fx/Quarantined.java': 'package fx; import java.lang.annotation.*; import org.junit.jupiter.api.Tag; @Retention(RetentionPolicy.RUNTIME) @Tag("quarantined") public @interface Quarantined {}',
}

const FIXTURES = {
    // Abstract base declaring the test; concrete subclasses inherit it. Four top-level classes in ONE
    // file, package-private - the probe's real shape.
    'fx/integrationTests/ProbeBase.java': `package fx.integrationTests;
import org.junit.jupiter.api.RepeatedTest;
public abstract class ProbeBase { @RepeatedTest(5) void probe() {} }`,
    'fx/integrationTests/ProbeIT.java': `package fx.integrationTests;
class ProbeIT extends ProbeBase {}
class Probe2IT extends ProbeBase {}
class Probe3IT extends ProbeBase {}
class Probe4IT extends ProbeBase {}`,
    // Two hops: Leaf -> Middle (nothing) -> ProbeBase (tests).
    'fx/integrationTests/Middle.java': 'package fx.integrationTests; public class Middle extends ProbeBase {}',
    'fx/integrationTests/LeafIT.java': 'package fx.integrationTests; public class LeafIT extends Middle {}',
    // Method-level @Tag beside a plain @Test: the CLASS is not exempt.
    'fx/integrationTests/LoadTest.java': `package fx.integrationTests;
import org.junit.jupiter.api.*;
public class LoadTest { @Test void quick() {} @Tag("performance") @Test void slow() {} }`,
    // Class-level @Tag in an excluded group: exempt.
    'fx/integrationTests/ChaosIT.java': `package fx.integrationTests;
import org.junit.jupiter.api.*;
@Tag("chaos") public class ChaosIT { @Test void storm() {} }`,
    // Meta-annotated group: exempt through the annotation type's own @Tag.
    'fx/integrationTests/FlakyIT.java': `package fx.integrationTests;
import org.junit.jupiter.api.Test;
@fx.Quarantined public class FlakyIT { @Test void flaps() {} }`,
    // A helper whose javadoc says "extends ProbeBase" on ONE line, and a generic bound naming it.
    // Neither is inheritance. The source-scanning predecessor would have demanded a report for both.
    'fx/integrationTests/Seed.java': `package fx.integrationTests;
/** Split out so it is reachable without loading it: that class extends ProbeBase, whose initialiser starts Kafka. */
public final class Seed { static int seed() { return 1; } }`,
    'fx/integrationTests/Holder.java': `package fx.integrationTests;
public class Holder<T extends ProbeBase> { T held; }`,
    // @Nested: the inner class is reported under the enclosing class, never demanded on its own.
    'fx/integrationTests/OuterIT.java': `package fx.integrationTests;
import org.junit.jupiter.api.*;
public class OuterIT { @Nested class Inner { @Test void inside() {} } }`,
    // @ParameterizedTest as the ONLY test - the org.junit.jupiter.params package.
    'fx/integrationTests/MultiTopicTest.java': `package fx.integrationTests;
import org.junit.jupiter.params.ParameterizedTest;
public class MultiTopicTest { @ParameterizedTest void eachTopic() {} }`,
    // Same simple name in two packages; only the integrationTests one is in scope.
    'fx/integrationTests/SameNameIT.java': 'package fx.integrationTests; import org.junit.jupiter.api.Test; public class SameNameIT { @Test void a() {} }',
    'fx/other/SameNameIT.java': 'package fx.other; import org.junit.jupiter.api.Test; public class SameNameIT { @Test void b() {} }',
    // Outside any integrationTests package: never in scope however many tests it has.
    'fx/unit/PlainTest.java': 'package fx.unit; import org.junit.jupiter.api.Test; public class PlainTest { @Test void u() {} }',
}

// --- compile -----------------------------------------------------------------------------------------
const tmp = mkdtempSync(join(tmpdir(), 'shard-coverage-'))
const src = join(tmp, 'src')
const mod = join(tmp, 'mod')
const classes = join(mod, 'target', 'test-classes')
const reportsDir = join(mod, 'target', 'failsafe-reports')
mkdirSync(classes, { recursive: true })
mkdirSync(reportsDir, { recursive: true })
const sources = []
for (const [rel, body] of Object.entries({ ...STUBS, ...FIXTURES })) {
    const p = join(src, rel)
    mkdirSync(dirname(p), { recursive: true })
    writeFileSync(p, body + '\n')
    sources.push(p)
}
const javac = spawnSync(jdkBinary('javac'), ['-d', classes, ...sources], { encoding: 'utf8' })
if (javac.status !== 0) {
    console.error(`could not compile the fixtures with ${jdkBinary('javac')} - a JDK is required for this self-test`)
    console.error(javac.stderr || javac.error)
    rmSync(tmp, { recursive: true, force: true })
    process.exit(2)
}

try {
    // --- library: what the compiler said ---------------------------------------------------------
    const PKG = /(^|\.)integrationTest[^.]*\./
    const all = classFilesUnder(classes)
    const index = buildIndex(all.filter((n) => PKG.test(n)), classes)
    const get = (simple) => index.get(`fx.integrationTests.${simple}`)

    check('abstract is read from the access flags', get('ProbeBase').isAbstract, true)
    check('the supertype is read from super_class, not from source text', get('Probe4IT').superName, 'fx.integrationTests.ProbeBase')
    check('four top-level classes in one file are four classes', ['ProbeIT', 'Probe2IT', 'Probe3IT', 'Probe4IT'].every((s) => get(s) !== undefined), true)
    check('a one-line subclass inherits its base\'s test', inheritsTest(index, 'fx.integrationTests.Probe4IT'), true)
    check('inheritance is followed two hops through an undecorated intermediate', inheritsTest(index, 'fx.integrationTests.LeafIT'), true)
    check('@ParameterizedTest (org.junit.jupiter.params) counts as a test', get('MultiTopicTest').declaresTest, true)
    check('a javadoc sentence saying "extends ProbeBase" is not inheritance', get('Seed').superName, 'java.lang.Object')
    check('  ...and the helper does not inherit tests', inheritsTest(index, 'fx.integrationTests.Seed'), false)
    check('a generic bound <T extends ProbeBase> is not inheritance', get('Holder').superName, 'java.lang.Object')
    check('a method-level @Tag does not become a class tag', get('LoadTest').classTags, [])
    check('a class-level @Tag is read with its value', get('ChaosIT').classTags, ['chaos'])
    check('a meta-annotated group resolves through the annotation type', get('FlakyIT').effectiveTags, ['quarantined'])

    const required = classesRequiringReports(index, PKG, ['chaos', 'quarantined'])
    const simple = required.map((n) => n.split('.').pop())
    // `Middle` is in here on purpose: a concrete class with no tests of its own that extends a test
    // base IS a test class - JUnit runs the inherited tests - and the first draft of this expectation
    // left it out. `OuterIT` is in here because its only tests live in a @Nested inner class, which
    // Jupiter reports under the enclosing class.
    check('required set: concrete test classes in scope, minus excluded groups', simple, [
        'LeafIT', 'LoadTest', 'Middle', 'MultiTopicTest', 'OuterIT', 'Probe2IT', 'Probe3IT', 'Probe4IT', 'ProbeIT', 'SameNameIT',
    ])
    check('  ...an undecorated concrete subclass of a test base IS demanded', simple.includes('Middle'), true)
    check('  ...a class whose only tests are @Nested IS demanded (reported under it)', simple.includes('OuterIT'), true)
    check('  ...the abstract base is not demanded', simple.includes('ProbeBase'), false)
    check('  ...the javadoc-near-miss helper is not demanded', simple.includes('Seed'), false)
    check('  ...the @Nested inner class is not demanded on its own', required.some((n) => n.includes('$')), false)
    check('  ...LoadTest IS demanded despite its method-level @Tag', simple.includes('LoadTest'), true)
    check('  ...the same simple name outside the package is not in scope', required.includes('fx.other.SameNameIT'), false)
    check('  ...a unit test outside any integration package is not in scope', required.includes('fx.unit.PlainTest'), false)

    // --- parser: real javap text, package-private class with no "Compiled from" modifier line --------
    const raw = execFileSync(javapBinary(), ['-v', '-p', '-cp', classes, 'fx.integrationTests.Probe4IT'], { encoding: 'utf8' })
    const parsed = parseJavap(raw)
    check('parseJavap yields one record for one class', parsed.length, 1)
    check('  ...with the class-level flags, not a member\'s', parsed[0].isAbstract, false)

    // --- the gate end to end, against the fixture tree ----------------------------------------------
    const run = () => spawnSync('node', [GATE, '--heavy-classes', 'ProbeIT,Probe2IT', '--excluded-groups', 'chaos,quarantined'], {
        encoding: 'utf8', env: { ...process.env, SHARD_COVERAGE_ROOT: tmp },
    })
    let r = run()
    check('no reports at all is nothing-in-scope (exit 3), not a pass', r.status, 3)

    const report = (fqcn) => writeFileSync(join(reportsDir, `TEST-${fqcn}.xml`), '<testsuite/>\n')
    for (const s of ['LeafIT', 'LoadTest', 'Middle', 'MultiTopicTest', 'OuterIT', 'Probe3IT', 'ProbeIT', 'Probe2IT', 'SameNameIT']) report(`fx.integrationTests.${s}`)
    r = run()
    check('one required class with no report fails (exit 1)', r.status, 1)
    contains('  ...and it is named', r.stderr, 'fx.integrationTests.Probe4IT')
    check('  ...and nothing else is', (r.stderr.match(/^\s{4}fx\./gm) || []).length, 1)

    report('fx.integrationTests.Probe4IT')
    r = run()
    check('every required class reported passes (exit 0)', r.status, 0)
    contains('  ...and says so', r.stdout, 'every integration test class produced a failsafe report')

    // A named heavy class missing is the heavy shard's business, reported separately, not as ran-nowhere.
    rmSync(join(reportsDir, 'TEST-fx.integrationTests.ProbeIT.xml'))
    r = run()
    check('a missing HEAVY class does not fail this gate', r.status, 0)
} finally {
    rmSync(tmp, { recursive: true, force: true })
}

if (failures) { console.error(`\n${failures} self-test(s) FAILED`); process.exit(1) }
console.log('\nAll bin/check-integration-shard-coverage.mjs self-tests passed')
