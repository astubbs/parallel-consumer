/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */
package bz.stub.parallelconsumer.client;

/**
 * Prints the one line every language in this tree prints - the Java end of the polyglot build
 * scaffolding (astubbs#242).
 * <p>
 * The wording is fixed by {@code bin/foreign-client-step.sh}, which derives the expected line for
 * the eight non-JVM modules. The three JVM modules assert it in their own test frameworks instead
 * of through that wrapper, because they need no toolchain detection - but the string has to agree,
 * so a change to the wrapper's fixture must change all eleven.
 */
public final class HelloFixture {

    /** The fixture line, without a trailing newline. */
    public static final String LINE = "parallel-consumer-proxy-client hello fixture: java";

    private HelloFixture() {
    }

    public static void main(String[] args) {
        System.out.print(LINE);
    }
}
