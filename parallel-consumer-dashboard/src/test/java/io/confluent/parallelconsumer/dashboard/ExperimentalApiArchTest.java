package io.confluent.parallelconsumer.dashboard;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.apache.kafka.common.annotation.InterfaceStability;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * The experimental marker is structural, not a sentence in a README.
 * <p>
 * Documentation is read once, if at all. An annotation is seen at every use site, by every IDE, and by anyone
 * generating an API report. The point of this rule is the type somebody adds <em>later</em>: without it, a new public
 * class ships unmarked, acquires users, and quietly acquires the compatibility obligations the module has not agreed
 * to.
 */
@AnalyzeClasses(packages = "io.confluent.parallelconsumer.dashboard",
        importOptions = ImportOption.DoNotIncludeTests.class)
class ExperimentalApiArchTest {

    @ArchTest
    static final ArchRule every_public_type_carries_the_experimental_marker =
            classes()
                    .that().arePublic()
                    .and().areNotAnonymousClasses()
                    .and().areNotLocalClasses()
                    // Lombok generates the builder classes and cannot be told to annotate them. They are reachable
                    // only through an already-marked type's builder() method, so the marker on the owning type is
                    // what a user actually sees.
                    .and().haveSimpleNameNotEndingWith("Builder")
                    .should().beAnnotatedWith(InterfaceStability.Unstable.class)
                    .because("KTD12: the module ships experimental, and the marker has to be visible from the call "
                            + "site rather than only in prose");

    /**
     * Marking a type stable would be a promise this module has not made. Catching it here means the promise has to
     * be made deliberately, in a commit that changes this rule, rather than by an annotation copied from elsewhere.
     */
    @ArchTest
    static final ArchRule no_type_claims_to_be_stable_or_evolving =
            noClasses()
                    .should().beAnnotatedWith(InterfaceStability.Stable.class)
                    .orShould().beAnnotatedWith(InterfaceStability.Evolving.class)
                    .because("nothing in this module has a compatibility guarantee yet");
}
