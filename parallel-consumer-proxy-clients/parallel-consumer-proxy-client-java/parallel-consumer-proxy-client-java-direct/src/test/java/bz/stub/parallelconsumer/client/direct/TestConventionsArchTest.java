package bz.stub.parallelconsumer.client.direct;
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
 * The rule that earns its place here is the surefire-naming one: this module's whole suite is a subclass of
 * the api test-jar's conformance suite, and a subclass named anything outside surefire's default includes is
 * never collected - so the transport would report green having run nothing.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer.client.direct", importOptions = ImportOption.OnlyIncludeTests.class)
class TestConventionsArchTest {

    @ArchTest
    static final ArchTests conventions = ArchTests.in(TestConventionRules.class);
}
