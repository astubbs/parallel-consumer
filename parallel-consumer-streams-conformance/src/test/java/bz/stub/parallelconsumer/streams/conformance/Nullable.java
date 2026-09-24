package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field, parameter or return value that may legitimately be {@code null}.
 * <p>
 * The build runs NullAway over {@code bz.stub}, which treats everything unannotated as non-null - and this package
 * is full of things that are deliberately absent: the ten operation slots on a case entry, of which exactly one is
 * ever set; the topic that only a source and a sink carry; the fault that only a refusal-class case names; a case
 * that failed a rule and so was never built. Annotating those says which nulls are the design and lets the checker
 * red on the ones that are not, which is the whole value of having it on.
 * <p>
 * <b>Why it is declared here.</b> Nothing on this module's test classpath supplies one: the reactor pulls in no
 * JSR-305, no Checker Framework qualifiers and no JetBrains annotations, and adding a dependency to a test-only
 * module for one annotation type buys a dependency-audit surface for nothing. NullAway matches a nullability
 * annotation by its <em>simple name</em>, so a locally declared one is understood exactly as a library one would be.
 * If the reactor ever gains a shared annotation, this is a delete-and-reimport away.
 */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.LOCAL_VARIABLE})
@interface Nullable {
}
