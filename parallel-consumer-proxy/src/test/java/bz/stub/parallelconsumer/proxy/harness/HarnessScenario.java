package bz.stub.parallelconsumer.proxy.harness;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.github.bsideup.jabel.Desugar;

import java.util.List;
import java.util.Optional;

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
 * @see ProxyHarness
 */
@Desugar // Jabel requires the annotation on every record, even in this module where release=17 makes it a no-op
public record HarnessScenario(String name, List<SeedRecord> seeds) {

    /** One record to seed, key first because the key decides the shard. Offsets are assigned by list order. */
    @Desugar
    public record SeedRecord(String key, String value) {
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
