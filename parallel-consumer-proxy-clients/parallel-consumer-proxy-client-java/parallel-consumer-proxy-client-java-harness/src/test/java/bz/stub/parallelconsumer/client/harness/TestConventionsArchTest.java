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
 * It earns its place here more than anywhere: this module's entire test tree is one subclass of the api
 * test-jar's conformance suite, so a name outside surefire's default includes would leave the module green
 * having run nothing at all - and the gRPC transport would then have no engine-backed evidence while
 * appearing to.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer.client.harness",
        importOptions = ImportOption.OnlyIncludeTests.class)
class TestConventionsArchTest {

    @ArchTest
    static final ArchTests conventions = ArchTests.in(TestConventionRules.class);
}
