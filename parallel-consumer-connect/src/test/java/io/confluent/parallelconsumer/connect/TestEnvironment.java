package io.confluent.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the build and the JVM actually handed this test: where a class was loaded from, and what the
 * surefire execution configured. Both are environment facts the assertions here turn on, rather than
 * anything the tests themselves control.
 */
final class TestEnvironment {

    private TestEnvironment() {
    }

    /**
     * Where a class was actually loaded from. Every arm of the shadowing proof turns on this value, so the
     * two null checks are assertions rather than a bare dereference - a missing protection domain or code
     * source names the class that lacked it instead of surfacing as a NullPointerException.
     */
    static URL codeSourceOf(Class<?> type) {
        assertThat(type.getProtectionDomain()).as("no protection domain for %s", type.getName()).isNotNull();
        assertThat(type.getProtectionDomain().getCodeSource())
                .as("no code source for %s", type.getName())
                .isNotNull();
        return type.getProtectionDomain().getCodeSource().getLocation();
    }

    /**
     * A system property the surefire execution is required to set. Failing loudly matters here: an unset
     * property would otherwise let an arm skip its real assertion and still report green.
     */
    static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("missing required system property " + name);
        }
        return value;
    }
}
