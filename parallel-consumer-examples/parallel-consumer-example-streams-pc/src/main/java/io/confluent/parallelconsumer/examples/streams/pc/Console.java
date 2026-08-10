package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The demo's report goes to {@code System.out} rather than through SLF4J, because here the console output
 * <em>is</em> the deliverable.
 * <p>
 * A logger would prefix every line with a timestamp, level and thread, wrap the report in whatever pattern
 * the ambient logback config happens to use, and interleave it with Kafka's own logging. The measurements
 * would still be correct and nobody would be able to read them. Kafka and Streams stay on SLF4J and are
 * turned down to WARN in {@code logback.xml}, so the two do not compete for the terminal.
 *
 * @author Antony Stubbs
 */
final class Console {

    private static final int WIDTH = 100;

    private Console() {
    }

    static void line(final String format, final Object... args) {
        System.out.println(args.length == 0 ? format : String.format(format, args));
    }

    /** A titled rule, for the boundaries a reader navigates by. */
    static void section(final String title) {
        System.out.println();
        System.out.println(rule('='));
        System.out.println("== " + title);
        System.out.println(rule('='));
    }

    static void subSection(final String title) {
        System.out.println();
        System.out.println("> " + title);
    }

    /**
     * Prints prose wrapped to the console width, with a hanging indent under a label.
     * <p>
     * The explanatory text in this demo is the part that makes the numbers mean something, so it cannot be
     * allowed to run off the right-hand edge of a terminal into a single unreadable line.
     */
    static void wrapped(final String label, final String text) {
        String indent = repeat(' ', label.length());
        int available = WIDTH - label.length();
        StringBuilder currentLine = new StringBuilder();
        boolean firstLine = true;

        for (String word : text.split(" ")) {
            if (currentLine.length() > 0 && currentLine.length() + 1 + word.length() > available) {
                System.out.println((firstLine ? label : indent) + currentLine);
                currentLine.setLength(0);
                firstLine = false;
            }
            if (currentLine.length() > 0) {
                currentLine.append(' ');
            }
            currentLine.append(word);
        }
        if (currentLine.length() > 0) {
            System.out.println((firstLine ? label : indent) + currentLine);
        }
    }

    /** For the one message that must not be skimmed past. */
    static void banner(final String message) {
        String stars = repeat('*', WIDTH);
        System.out.println(stars);
        System.out.println("*** " + message);
        System.out.println(stars);
    }

    private static String rule(final char character) {
        return repeat(character, WIDTH);
    }

    private static String repeat(final char character, final int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(character);
        }
        return builder.toString();
    }
}
