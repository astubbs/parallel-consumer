package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */


import java.util.List;
import java.util.Optional;

/*
 * NO JAVA RECORDS ANYWHERE IN THIS MODULE, AND THAT IS A BUILD FACT RATHER THAN A STYLE CHOICE.
 *
 * The source these were taken from writes this and four other value types as records, each carrying
 * Jabel's @Desugar. Jabel REQUIRES that annotation on every record it sees - a record without one
 * fails the compile with "Must be annotated with @Desugar" even at release 17 - and what the
 * annotation then does is rewrite the record into a class whose generated members have no source
 * positions. Error Prone cannot read that: 2.42.0 crashes outright on it, first in
 * UnnecessaryStringBuilder ("invalid replacement: [0, -1)") and, once that one is suppressed, again
 * in DuplicateBranches. A crash is not a finding - it fails the whole compilation, attributed to
 * line 1 of whichever file javac listed first, which is why it reads as unrelated.
 *
 * Neither term can move. The root pom pins Error Prone at 2.42.0 because 2.43.0 is compiled to
 * class file 65 and JDK 17 cannot load it, and Jabel is what serves the release 8 target. The
 * branch these came from never met any of this: its root pom has no Error Prone at all.
 *
 * So the fix is the one the repository had already made everywhere else - `grep -rn "record "` over
 * master finds no Java record in any module - and which this PR's own SpikeFixture.Seed already
 * follows: a plain final class with the same accessors. Nothing is lost but the boilerplate, and
 * the boilerplate is what makes the module compile.
 */
/**
 * A named, seedable product behaviour the proxy must exhibit, expressed without reference to any client
 * language.
 * <p>
 * These names are the beginning of the cross-language conformance set: every client, in every language, runs
 * against the same scenarios on the same JVM-side harness, so the scenario is named for the <em>product
 * behaviour</em> it demonstrates rather than for the Java test method that first exercised it. The {@link #name}
 * doubles as the scenario's wire/CLI identity (and as the harness's topic name), so it is kebab-case and stable.
 * <p>
 * A scenario owns only what the engine side must know: which records to seed, with which keys. What the
 * <em>client</em> does with them (succeed, fail, stay silent) is the client's half of the scenario and lives
 * with the test that drives it - a foreign client makes those moves over gRPC, an in-JVM client makes them as a
 * plain function.
 *
 * @author Antony Stubbs
 * @see ConformanceHarness
 */
public final class HarnessScenario {

    /** One record to seed, key first because the key decides the shard. Offsets are assigned by list order. */
    public static final class SeedRecord {

        private final String key;

        private final String value;

        public SeedRecord(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public String value() {
            return value;
        }
    }

    private final String name;

    private final List<SeedRecord> seeds;

    public HarnessScenario(String name, List<SeedRecord> seeds) {
        this.name = name;
        this.seeds = List.copyOf(seeds);
    }

    /** The scenario's stable name: its identity in the harness, in the guide and in every binding's run. */
    public String name() {
        return name;
    }

    /** The records to seed, in offset order. */
    public List<SeedRecord> seeds() {
        return seeds;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * The trivial baseline every language runs first: one record in, processed once, committed offset advances
     * past it.
     */
    public static final HarnessScenario A_PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET =
            new HarnessScenario("a-processed-record-advances-the-committed-offset",
                    List.of(new SeedRecord("lone-key", "hello")));

    /**
     * The negative control: a client that never reports leaves the offset uncommitted, and the harness's
     * convergence condition FAILS rather than passing vacuously. A harness that goes green when the client is
     * broken would go green for ten languages at once.
     */
    public static final HarnessScenario AN_UNREPORTED_RECORD_HOLDS_BACK_THE_COMMIT =
            new HarnessScenario("an-unreported-record-holds-back-the-commit",
                    List.of(new SeedRecord("lone-key", "never-reported")));

    /**
     * A reported failure is redelivered, and the redelivery carries the record's history: the attempt count is
     * incremented and the earlier failure reason is visible.
     */
    public static final HarnessScenario A_FAILED_RECORD_IS_REDELIVERED_WITH_ITS_FAILURE_HISTORY =
            new HarnessScenario("a-failed-record-is-redelivered-with-its-failure-history",
                    List.of(new SeedRecord("lone-key", "fails-once")));

    /**
     * Key ordering's core promise: records sharing a key are serialized on one shard, while records with
     * distinct keys proceed concurrently on distinct shards - even within a single partition.
     */
    public static final HarnessScenario RECORDS_SHARING_A_KEY_SHARE_A_SHARD_DISTINCT_KEYS_RUN_CONCURRENTLY =
            new HarnessScenario("records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently",
                    List.of(new SeedRecord("shared", "first-of-shared"),
                            new SeedRecord("shared", "second-of-shared"),
                            new SeedRecord("distinct", "only-of-distinct")));

    /**
     * The in-flight ceiling: however many records are waiting, a client may hold only as many unresolved at
     * once as the {@code max_concurrency} it configured - queued <em>plus</em> executing.
     * <p>
     * <b>Six records on six DISTINCT keys, so key ordering is not what limits concurrency.</b> Seeded on one
     * key, a small observed concurrency would prove only that the shard serialized them, and the scenario
     * would pass for a client that respected no ceiling at all. Distinct keys mean the engine is free to
     * dispatch every one of them at once, and the only thing standing between the client and six concurrent
     * records is the ceiling it asked for.
     * <p>
     * The count is a multiple of the ceiling the conformance suite drives this with, so the prescribed
     * behaviour's groups divide exactly - {@code ConformanceScenarios} owns that number, because how many
     * records may be outstanding is the client's half of the scenario rather than the engine's.
     */
    public static final HarnessScenario THE_IN_FLIGHT_CEILING_BOUNDS_UNRESOLVED_RECORDS =
            new HarnessScenario("the-in-flight-ceiling-bounds-unresolved-records",
                    List.of(new SeedRecord("ceiling-a", "first"),
                            new SeedRecord("ceiling-b", "second"),
                            new SeedRecord("ceiling-c", "third"),
                            new SeedRecord("ceiling-d", "fourth"),
                            new SeedRecord("ceiling-e", "fifth"),
                            new SeedRecord("ceiling-f", "sixth")));

    /**
     * The conformance set so far, in the order a new client should attempt them. Grown here, scenario by
     * scenario, as the engine units land behaviours worth conforming to.
     */
    public static List<HarnessScenario> conformanceScenarios() {
        return List.of(
                A_PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET,
                AN_UNREPORTED_RECORD_HOLDS_BACK_THE_COMMIT,
                A_FAILED_RECORD_IS_REDELIVERED_WITH_ITS_FAILURE_HISTORY,
                RECORDS_SHARING_A_KEY_SHARE_A_SHARD_DISTINCT_KEYS_RUN_CONCURRENTLY,
                THE_IN_FLIGHT_CEILING_BOUNDS_UNRESOLVED_RECORDS);
    }

    /**
     * Looks a scenario up by its stable name - the form a spawning foreign test or the test-mode sidecar's
     * command line refers to it by.
     */
    public static Optional<HarnessScenario> byName(String name) {
        return conformanceScenarios().stream()
                .filter(scenario -> scenario.name().equals(name))
                .findFirst();
    }
}
