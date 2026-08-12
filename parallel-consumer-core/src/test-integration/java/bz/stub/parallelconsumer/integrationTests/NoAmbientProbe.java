package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Opt-out for the {@link AmbientProbeExtension} flight recorder, per test class or per test method
 * (class-level is {@link Inherited}, so it also covers subclasses). Use sparingly - the probe is
 * observer-only and cannot fail a test; the legitimate reasons to disable it are tests that are
 * sensitive to the extra admin-client read load, or tests that deliberately break the admin path.
 * The {@code -Dambient.probe=off} system property disables it globally instead.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface NoAmbientProbe {
}
