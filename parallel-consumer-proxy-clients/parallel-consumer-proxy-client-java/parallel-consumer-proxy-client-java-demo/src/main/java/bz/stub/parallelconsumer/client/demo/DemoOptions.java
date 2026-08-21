package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * The demo's dials, and <b>the interface every per-language demo mirrors</b>.
 *
 * <h2>Why this is a class of its own</h2>
 *
 * Ten other demos copy this surface, so it is written down once, in one place, with its precedence
 * rule stated rather than implied. Flags beat environment variables beat defaults - the ordinary
 * convention, chosen because a container passes configuration by environment while a person at a
 * terminal passes flags, and both must be able to override the other's layer.
 *
 * <h2>R39 does not govern a demo, and this is the file where that has to be said</h2>
 *
 * R39 constrains how configuration reaches the <em>proxy</em>. A demo is an application, so its
 * flags are not a violation of it. Without this note someone reads {@code --records} as breaking
 * the plan's own rule and deletes it (plan unit U35, step 5).
 *
 * @author Antony Stubbs
 */
public final class DemoOptions {

    /** Prefix for every environment variable this demo reads, so a reader can grep one string. */
    public static final String ENV_PREFIX = "PC_DEMO_";

    private final int records;

    private final int delayMs;

    private final int maxConcurrency;

    private final int partitions;

    private final int replayFactor;

    private final String bootstrap;

    private final String topic;

    private DemoOptions(Builder builder) {
        this.records = builder.records;
        this.delayMs = builder.delayMs;
        this.maxConcurrency = builder.maxConcurrency;
        this.partitions = builder.partitions;
        this.replayFactor = builder.replayFactor;
        this.bootstrap = builder.bootstrap;
        this.topic = builder.topic;
    }

    /**
     * Whether the caller asked for the usage text rather than a run.
     * <p>
     * Handled here rather than only in {@code run.sh}, because the script is not the only way in:
     * {@code docker compose run demo --help} reaches this main directly, and answering that with
     * "unknown option: --help" would be a poor first impression of the demo the other ten copy.
     */
    public static boolean isHelpRequested(String[] args) {
        for (String arg : args) {
            if ("-h".equals(arg) || "--help".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Parses the demo's command line, falling back to the environment and then to the defaults.
     *
     * @param args the process arguments, which may legitimately be empty - that is the
     *             double-click case, and it must work
     * @param env  the environment to read, passed in rather than read from {@link System} so this
     *             is testable without mutating the JVM's own environment
     * @throws IllegalArgumentException on an unknown flag, a missing value, or a value that is not
     *                                  a positive number - a demo that silently ignores a
     *                                  misspelled flag reports numbers for settings the user did
     *                                  not ask for
     */
    public static DemoOptions parse(String[] args, Map<String, String> env) {
        var builder = new Builder();
        builder.applyEnvironment(env);

        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            switch (flag) {
                case "--records":
                    builder.records = positive(flag, value(args, ++i, flag));
                    break;
                case "--delay-ms":
                    builder.delayMs = nonNegative(flag, value(args, ++i, flag));
                    break;
                case "--concurrency":
                    builder.maxConcurrency = positive(flag, value(args, ++i, flag));
                    break;
                case "--partitions":
                    builder.partitions = positive(flag, value(args, ++i, flag));
                    break;
                case "--replay-factor":
                    // 1 or less skips the big replay, so this one is allowed to be zero
                    builder.replayFactor = nonNegative(flag, value(args, ++i, flag));
                    break;
                case "--bootstrap":
                    builder.bootstrap = value(args, ++i, flag);
                    break;
                case "--topic":
                    builder.topic = value(args, ++i, flag);
                    break;
                default:
                    throw new IllegalArgumentException("unknown option: " + flag);
            }
        }
        return builder.build();
    }

    private static String value(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException(flag + " needs a value");
        }
        return args[index];
    }

    private static int positive(String flag, String raw) {
        int parsed = number(flag, raw);
        if (parsed < 1) {
            throw new IllegalArgumentException(flag + " must be at least 1, got " + parsed);
        }
        return parsed;
    }

    private static int nonNegative(String flag, String raw) {
        int parsed = number(flag, raw);
        if (parsed < 0) {
            throw new IllegalArgumentException(flag + " must not be negative, got " + parsed);
        }
        return parsed;
    }

    private static int number(String flag, String raw) {
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " needs a whole number, got '" + raw + "'", e);
        }
    }

    public int records() {
        return records;
    }

    public int delayMs() {
        return delayMs;
    }

    public int maxConcurrency() {
        return maxConcurrency;
    }

    public int partitions() {
        return partitions;
    }

    public int replayFactor() {
        return replayFactor;
    }

    /** The records the big replay consumes in total, including the small replay's own. */
    public int bigReplayRecords() {
        return records * Math.max(1, replayFactor);
    }

    /** True when the big replay is worth running at all; a factor of 1 or less skips it. */
    public boolean bigReplayWanted() {
        return replayFactor > 1;
    }

    /**
     * The broker to use, when the caller supplied one. Empty means "start one" - the Testcontainers
     * default of KTD40. Inside the demo container this is always present, because a demo container
     * is never granted the host Docker socket and so cannot start a broker of its own; it reaches a
     * compose sibling instead (plan unit U35, step 2).
     */
    public Optional<String> bootstrap() {
        return Optional.ofNullable(bootstrap);
    }

    /** The topic to use, when the caller supplied one. Empty means the demo names its own. */
    public Optional<String> topic() {
        return Optional.ofNullable(topic);
    }

    /**
     * The effective configuration, for printing before the run.
     * <p>
     * A number without its settings is not reproducible, so this is part of the contract every
     * language's demo keeps rather than a debugging aid. The bootstrap address is deliberately
     * absent: own-cluster mode puts a user's real broker address here, and the credential-hygiene
     * rule that binds the proxy binds a demo too - nothing logged, nothing echoed.
     */
    @Override
    public String toString() {
        return "records = " + records
                + "\n  delayMs = " + delayMs
                + "\n  maxConcurrency = " + maxConcurrency
                + "\n  partitions = " + partitions
                + "\n  replayFactor = " + replayFactor;
    }

    private static final class Builder {

        private int records = 2_000;

        private int delayMs = 2;

        private int maxConcurrency = 100;

        private int partitions = 10;

        private int replayFactor = 20;

        private String bootstrap;

        private String topic;

        void applyEnvironment(Map<String, String> env) {
            envValue(env, "RECORDS").ifPresent(raw -> records = positive("PC_DEMO_RECORDS", raw));
            envValue(env, "DELAY_MS").ifPresent(raw -> delayMs = nonNegative("PC_DEMO_DELAY_MS", raw));
            envValue(env, "CONCURRENCY")
                    .ifPresent(raw -> maxConcurrency = positive("PC_DEMO_CONCURRENCY", raw));
            envValue(env, "PARTITIONS").ifPresent(raw -> partitions = positive("PC_DEMO_PARTITIONS", raw));
            envValue(env, "REPLAY_FACTOR")
                    .ifPresent(raw -> replayFactor = nonNegative("PC_DEMO_REPLAY_FACTOR", raw));
            envValue(env, "BOOTSTRAP").ifPresent(raw -> bootstrap = raw);
            envValue(env, "TOPIC").ifPresent(raw -> topic = raw);
        }

        private static Optional<String> envValue(Map<String, String> env, String suffix) {
            String raw = env.get(ENV_PREFIX + suffix.toUpperCase(Locale.ROOT));
            return raw == null || raw.trim().isEmpty() ? Optional.empty() : Optional.of(raw.trim());
        }

        DemoOptions build() {
            // Checked as a long here rather than trusted as an int later: records * replayFactor
            // overflows silently, and a wrapped value turns the big replay into a tiny one that
            // still prints a confident throughput figure.
            long bigReplay = (long) records * Math.max(1, replayFactor);
            if (bigReplay > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("--records times --replay-factor is " + bigReplay
                        + ", which is more records than the demo can count; lower one of them");
            }
            return new DemoOptions(this);
        }
    }
}
