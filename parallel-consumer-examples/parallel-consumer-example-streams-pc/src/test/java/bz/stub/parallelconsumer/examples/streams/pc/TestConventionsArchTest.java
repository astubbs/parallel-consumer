package bz.stub.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.TestConventionRules;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

/**
 * Applies the shared {@link TestConventionRules} to this module's test classes - the rule logic lives once in
 * {@link TestConventionRules} (core test-jar); this only points ArchUnit at this module's packages.
 * <p>
 * A sibling in {@code parallel-consumer-example-streams} names the parent package
 * {@code bz.stub.parallelconsumer.examples.streams}, which reads as though it would cover this one. It does
 * not: ArchUnit imports from the classpath of the module it runs in, so each module needs its own. That is
 * what {@code EveryModuleWiresUpArchUnitTest} enforces, and it is the check that caught this module arriving
 * without one.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer.examples.streams.pc",
        importOptions = ImportOption.OnlyIncludeTests.class)
class TestConventionsArchTest {

    @ArchTest
    static final ArchTests conventions = ArchTests.in(TestConventionRules.class);
}
