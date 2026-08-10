package io.confluent.parallelconsumer.streams;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Map;

/**
 * One task's answer to "is PC dispatch on, and with what pool size" - resolved once, from that task's own
 * {@code StreamsConfig}, and immutable thereafter.
 *
 * <h2>Why this exists, when {@link PcDispatchSwitch} already answered the question</h2>
 * {@link PcDispatchSwitch} is process-global, and deliberately so: a {@code StreamTask} is built several
 * layers inside {@code KafkaStreams}, with no seam through which a caller can hand it a collaborator. That
 * reasoning is intact. What it missed is that the constructor is handed an {@code InternalProcessorContext},
 * and {@code AbstractProcessorContext} - already one of this module's patched classes - holds the
 * {@code StreamsConfig} and exposes {@code appConfigs()}. So the user's own configuration <em>is</em>
 * reachable at the one point where the decision is taken.
 * <p>
 * A JVM-wide switch is enough to run a whole suite twice with a different flag. It is not enough for the two
 * things an adopter actually wants:
 * <ul>
 *   <li><b>Measure PC dispatch against their own topology.</b> Both arms have to be alive at once, or every
 *       between-run confound comes back and the comparison stops being one.</li>
 *   <li><b>Run their existing Kafka Streams test suite with the seam on, to see whether it still passes.</b>
 *       That is a far better adoption path than any number this project could publish - it converts "trust
 *       our alpha" into "verify it yourself" - and it is precisely what a process-global flag makes
 *       impossible. Tests toggle each other's switch, read each other's counters, and the resulting flakes
 *       read as "this module is broken" rather than "this configuration is wrong".</li>
 * </ul>
 *
 * <h2>Precedence, and why it is this way round</h2>
 * <ol>
 *   <li>The {@code StreamsConfig} property, when the instance sets one.</li>
 *   <li>Otherwise {@link PcDispatchSwitch} - which is the system property, and any programmatic
 *       {@link PcDispatchSwitch#enable(int)} / {@link PcDispatchSwitch#disable()}.</li>
 *   <li>Otherwise the default: <b>on</b>, with a pool of 4. Depending on this artifact is the opt-in.</li>
 * </ol>
 * Step 2 is not a courtesy to old callers. This module's build sets
 * {@code -Dpc.streams.dispatch.enabled=false} on the surefire execution that runs Apache Kafka's own 188
 * tests, and those tests construct tasks from a {@code StreamsConfig} that says nothing about PC dispatch.
 * If silence meant "use the default" rather than "fall through to the switch", that execution would quietly
 * run with the seam <em>on</em> and the module's behaviour-preservation claim would become vacuous without a
 * single test turning red.
 *
 * <h2>Configuring it</h2>
 * Both names below work in a {@code StreamsConfig} <em>and</em> as a {@code -D} system property, so there is
 * one vocabulary rather than one per location:
 * <pre>{@code
 * Properties props = new Properties();
 * props.put(PcDispatchSettings.ENABLED_CONFIG, false);   // stock Kafka Streams dispatch, this instance only
 * props.put(PcDispatchSettings.POOL_SIZE_CONFIG, 8);     // 8 workers per task, this instance only
 * KafkaStreams streams = new KafkaStreams(topology, props);
 * }</pre>
 * Kafka Streams keeps configuration keys it does not recognise, and forwards them to the embedded consumer,
 * producer and admin client on purpose - that is how a custom {@code TimestampExtractor} or
 * {@code RocksDBConfigSetter} is configured, and it is why {@code ProcessorContext#appConfigs()} exists. The
 * visible cost is that each of those clients reports the key once at startup, through
 * {@code AbstractConfig.logUnused()}: <em>"These configurations '[pc.streams.dispatch.enabled]' were
 * supplied but are not used yet."</em> That line is <b>INFO, not WARN</b> (checked against Kafka 3.9.2), and
 * it is expected. Silencing it would mean patching {@code StreamsConfig} to register these keys in its
 * static {@code ConfigDef} - a fifth patched class, in the way of Kafka's own {@code StreamsConfigTest}, to
 * remove one INFO line. Not a trade worth making.
 *
 * <h2>A value that cannot be understood is an error, not an "off"</h2>
 * Every parse here fails loudly. The property whose whole job is to turn the seam off is the worst possible
 * place for a typo to be read as {@code false}: the run would look like a control arm while being nothing of
 * the kind, and nothing would say so.
 *
 * @author Antony Stubbs
 * @see PcDispatchSwitch
 * @see PcTaskDispatcher
 */
public final class PcDispatchSettings {

    /**
     * Set this to {@code false} in your {@code StreamsConfig} to give <em>this</em> {@code KafkaStreams}
     * instance stock Kafka Streams dispatch back, leaving every other instance in the JVM alone.
     * <p>
     * The same string is also the system property (it is {@link PcDispatchSwitch#ENABLED_PROPERTY}), so
     * {@code -Dpc.streams.dispatch.enabled=false} still turns the seam off for a whole JVM.
     */
    public static final String ENABLED_CONFIG = PcDispatchSwitch.ENABLED_PROPERTY;

    /**
     * Worker threads per task, and simultaneously PC's {@code maxConcurrency} - see {@link PcTaskDispatcher},
     * which uses it for both so that PC never hands out more work than the pool can start.
     * <p>
     * Lowercase and dot-separated, which is the Kafka Streams convention;
     * {@link PcDispatchSwitch#POOL_SIZE_PROPERTY} is the older camelCase spelling and is still honoured as a
     * system property.
     */
    public static final String POOL_SIZE_CONFIG = "pc.streams.dispatch.pool.size";

    private final boolean enabled;

    private final int poolSize;

    PcDispatchSettings(final boolean enabled, final int poolSize) {
        this.enabled = enabled;
        this.poolSize = poolSize;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Resolve one task's settings.
     *
     * @param streamsConfigs the instance's configuration, as {@code ProcessorContext#appConfigs()} returns it
     *                       - {@code StreamsConfig.originals()} merged over {@code values()}, so a key the
     *                       user set is present verbatim. Null or empty is legal and means "this instance
     *                       said nothing", which falls through to {@link PcDispatchSwitch}.
     * @throws IllegalArgumentException if a value is present but cannot be understood
     */
    public static PcDispatchSettings resolve(final Map<String, ?> streamsConfigs) {
        final Object rawEnabled = valueOf(streamsConfigs, ENABLED_CONFIG);
        final boolean resolvedEnabled = rawEnabled == null
                ? PcDispatchSwitch.isEnabled()
                : parseEnabled(rawEnabled, configSource(ENABLED_CONFIG));

        final Object rawPoolSize = valueOf(streamsConfigs, POOL_SIZE_CONFIG);
        final int resolvedPoolSize = rawPoolSize == null
                ? PcDispatchSwitch.getPoolSize()
                : parsePoolSize(rawPoolSize, configSource(POOL_SIZE_CONFIG));

        return new PcDispatchSettings(resolvedEnabled, resolvedPoolSize);
    }

    private static Object valueOf(final Map<String, ?> streamsConfigs, final String key) {
        return streamsConfigs == null ? null : streamsConfigs.get(key);
    }

    static String configSource(final String key) {
        return "StreamsConfig property '" + key + "'";
    }

    static String systemPropertySource(final String key) {
        return "System property '" + key + "'";
    }

    /**
     * {@code Boolean} and {@code String} are both accepted because a user's {@code Properties} or
     * {@code Map} may hold either, and {@code StreamsConfig.originals()} hands back whatever was put in.
     *
     * @param source how to name the offending setting in the failure message - the caller knows whether it
     *               came from a config or a {@code -D}, and a message that cannot say which is a message
     *               nobody can act on
     */
    static boolean parseEnabled(final Object raw, final String source) {
        if (raw instanceof Boolean) {
            return (Boolean) raw;
        }
        final String text = String.valueOf(raw).trim();
        if ("true".equalsIgnoreCase(text)) {
            return true;
        }
        if ("false".equalsIgnoreCase(text)) {
            return false;
        }
        throw new IllegalArgumentException(source + " must be 'true' or 'false', was '" + raw + "'. "
                + "PC dispatch is ON unless this says 'false', so a typo here would leave the seam on while "
                + "the run looked like a control arm.");
    }

    /**
     * @param source see {@link #parseEnabled}
     */
    static int parsePoolSize(final Object raw, final String source) {
        final int value;
        if (raw instanceof Number) {
            value = ((Number) raw).intValue();
        } else {
            try {
                value = Integer.parseInt(String.valueOf(raw).trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(source + " must be a whole number, was '" + raw + "'", e);
            }
        }
        if (value < 1) {
            throw new IllegalArgumentException(source + " must be at least 1, was " + value);
        }
        return value;
    }

    @Override
    public String toString() {
        return "PcDispatchSettings(enabled=" + enabled + ", poolSize=" + poolSize + ")";
    }
}
