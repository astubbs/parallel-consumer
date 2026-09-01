package bz.stub.parallelconsumer.conformance;
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
 * The wrapper cannot be inherited from the test-jar instead, for the reason its siblings across the
 * repository record: surefire's {@code dependenciesToScan} would pull the whole test-jar into every module.
 * {@code EveryModuleWiresUpArchUnitTest} fails any module with a {@code src/test/java} that has no wrapper.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer.conformance",
        importOptions = ImportOption.OnlyIncludeTests.class)
class TestConventionsArchTest {

    @ArchTest
    static final ArchTests conventions = ArchTests.in(TestConventionRules.class);
}
