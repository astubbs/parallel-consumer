package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;
import org.apache.commons.lang3.RandomUtils;

/**
 * A chaos run's schedule seed, and the full invocation that replays it. Every sighting in the
 * confluentinc#857 family ledger ({@code docs/inflight/bug-857-family.md}) names the seed replay as
 * its deciding experiment, so this is the one value a chaos failure cannot be diagnosed without.
 * <p>
 * Split out of {@code ChaosScenarioBase} so it is reachable without loading it: that class extends
 * {@code BrokerIntegrationTest}, whose static initialiser starts a Kafka Testcontainer, which puts
 * seed resolution and replay-command formatting out of reach of the unit suite. Here they are plain
 * surefire-testable functions ({@code AmbientProbeExtensionTest}).
 */
@Value
public class ChaosSeed {

    /** {@code -Dchaos.seed=<long>} replays a schedule; unset = a fresh random seed. */
    public static final String SEED_PROPERTY = "chaos.seed";

    long value;

    public static ChaosSeed resolve() {
        String seedProp = System.getProperty(SEED_PROPERTY);
        return new ChaosSeed(seedProp == null ? RandomUtils.nextLong() : Long.parseLong(seedProp));
    }

    /**
     * The FULL replay invocation, not just the seed - a raw CI log must be self-sufficient to
     * reproduce (the chaos tag is excluded by default, so the seed alone is not enough).
     */
    public String replayCommand() {
        return "./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true"
                + " -Dincluded.groups=chaos -Dexcluded.groups= -D" + SEED_PROPERTY + "=" + value;
    }

    /**
     * How a chaos scenario hands its seed to {@code AmbientProbeExtension}, which lifts it into the
     * failure autopsy block. Implemented by {@code ChaosScenarioBase}; the extension cannot name that
     * class (it is package-private here), and reading a per-test instance field is what keeps the
     * hand-off free of state shared between chaos classes JUnit may run concurrently.
     */
    public interface Holder {

        /** {@code null} until the scenario resolves a seed - it does so inside the test method. */
        ChaosSeed getChaosSeed();
    }
}
