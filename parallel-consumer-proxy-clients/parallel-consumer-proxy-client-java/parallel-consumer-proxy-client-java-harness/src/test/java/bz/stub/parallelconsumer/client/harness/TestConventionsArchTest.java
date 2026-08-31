package bz.stub.parallelconsumer.client.harness;
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
 * Same reasoning as the transport siblings, and it will matter more here than in either: the conformance suite
 * arrives in this module by subclassing, and a subclass named outside surefire's default includes is never
 * collected - so the lane would report green having run nothing.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer.client.harness",
        importOptions = ImportOption.OnlyIncludeTests.class)
class TestConventionsArchTest {

    @ArchTest
    static final ArchTests conventions = ArchTests.in(TestConventionRules.class);
}
