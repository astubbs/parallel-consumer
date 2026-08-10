package io.confluent.parallelconsumer.streams;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The one switch that decides whether a {@code StreamTask} feeds records to Parallel Consumer's
 * {@code WorkManager} or to Kafka Streams' own {@code PartitionGroup}.
 * <p>
 * <b>Single path, switched - never both.</b> The plan's KTD8 forbids registering records into both: nothing
 * would drain the partition group, {@code StreamTask.addRecords} pauses a partition once its buffer passes
 * {@code maxBufferedSize}, and the run stalls with the consumer paused and no error anywhere to say why.
 * So this is a switch, not a fan-out.
 * <p>
 * It defaults to <b>on</b>. <b>Depending on the artifact is the opt-in.</b> Nobody puts
 * {@code parallel-consumer-streams} on their classpath by accident - it is a separate, loudly-labelled
 * alpha artifact - and having done so deliberately, they wanted the PC seam. Making them also set a system
 * property is friction that buys nothing.
 * <p>
 * The property is still how you turn it <b>off</b>: {@code -Dpc.streams.dispatch.enabled=false} puts a
 * whole JVM back on stock Kafka Streams dispatch, which is what an A/B comparison needs. Tests that want the
 * stock path say so explicitly with {@link #disable()} rather than leaning on a default, because a control arm
 * that is only a control by default stops being one the moment the default moves - as it just did.
 * <p>
 * (An earlier revision defaulted this off, so that the U3 control arm stayed stock without saying so. That was
 * a test concern being paid for by every user of the artifact; the tests now state their requirement at each
 * site instead.)
 * <p>
 * Global mutable static state is normally a smell. Here it is the only thing that reaches the call site:
 * {@code StreamTask} is constructed several layers inside {@code KafkaStreams}, with no seam through which a
 * test could hand it a collaborator. An alpha gets to pay that price; a stable product would not.
 * <p>
 * <b>Since astubbs#255 this is the JVM-wide <em>fallback</em>, not the whole story.</b> There is a seam
 * after all - the {@code StreamTask} constructor is handed an {@code InternalProcessorContext}, whose
 * {@code appConfigs()} carries the instance's own {@code StreamsConfig} - so a task first asks
 * {@link PcDispatchSettings} what <em>its</em> instance was configured with, and only falls through to this
 * class when the instance said nothing. That is what lets two {@code KafkaStreams} instances in one JVM run
 * different arms at once, which is the whole point of being able to benchmark and test the seam against your
 * own topology. Everything below still works exactly as it did, and is still what a whole-JVM run uses.
 *
 * @author Antony Stubbs
 * @see PcDispatchSettings for the per-instance configuration this falls back from
 * @see PcTaskDispatcher
 */
public final class PcDispatchSwitch {

    /**
     * Set {@code -Dpc.streams.dispatch.enabled=false} to turn the seam off for a whole JVM and get stock
     * Kafka Streams dispatch back. Unset, or {@code =true}, means the seam is on. Tests should call
     * {@link #disable()} / {@link #enable(int)} instead, so they can put it back afterwards.
     * <p>
     * The same string is also the per-instance {@code StreamsConfig} key
     * ({@link PcDispatchSettings#ENABLED_CONFIG}) - one name, usable in either place.
     */
    public static final String ENABLED_PROPERTY = "pc.streams.dispatch.enabled";

    /**
     * Worker threads per task, and simultaneously PC's {@code maxConcurrency} - see
     * {@link PcTaskDispatcher}, which uses it for both so that PC never hands out more work than the pool
     * can start.
     * <p>
     * <b>The older spelling.</b> camelCase is not the Kafka Streams convention, so the documented name is
     * the dot-separated {@link PcDispatchSettings#POOL_SIZE_CONFIG}, which works both as a
     * {@code StreamsConfig} key and as a system property. This one is already published in the module's
     * README and {@code pom.xml} description, so it stays honoured rather than being broken; it is read only
     * when the dot-separated name is absent.
     */
    public static final String POOL_SIZE_PROPERTY = "pc.streams.dispatch.poolSize";

    private static final int DEFAULT_POOL_SIZE = 4;

    private static volatile boolean enabled = readEnabledProperty();

    private static volatile int poolSize = readPoolSizeProperty();

    private PcDispatchSwitch() {
    }

    public static boolean isEnabled() {
        return enabled;
    }

    public static int getPoolSize() {
        return poolSize;
    }

    /**
     * Turn PC dispatch on for tasks created from now on. Existing tasks keep whatever they were built with -
     * the decision is taken once, in the {@code StreamTask} constructor, so that a task cannot change record
     * paths halfway through a run and leave records stranded in the path it abandoned.
     *
     * @param workerPoolSize threads per task; must be at least 1
     */
    public static void enable(final int workerPoolSize) {
        if (workerPoolSize < 1) {
            throw new IllegalArgumentException("Worker pool size must be at least 1, was " + workerPoolSize);
        }
        poolSize = workerPoolSize;
        enabled = true;
    }

    /**
     * Back to stock Kafka Streams dispatch.
     */
    public static void disable() {
        enabled = false;
    }

    /**
     * Put the switch back to what this JVM started with - {@link #ENABLED_PROPERTY} if it was set, on
     * otherwise. Intended as a test teardown: leaving the switch wherever the last test left it re-creates
     * exactly the implicit-default coupling that flipping this default was meant to remove.
     */
    public static void resetToDefault() {
        poolSize = readPoolSizeProperty();
        enabled = readEnabledProperty();
    }

    /**
     * Absent, the seam is on - depending on the artifact is the opt-in. A value that is neither
     * {@code true} nor {@code false} fails loudly rather than being silently read as "off": a typo in a
     * property whose whole job is to turn the seam off would otherwise leave the seam on, and the run would
     * look like a control arm while being nothing of the kind.
     */
    private static boolean readEnabledProperty() {
        final String raw = System.getProperty(ENABLED_PROPERTY);
        if (raw == null) {
            return true;
        }
        return PcDispatchSettings.parseEnabled(raw, PcDispatchSettings.systemPropertySource(ENABLED_PROPERTY));
    }

    /**
     * The dot-separated name first, then the older camelCase one, then the default.
     * <p>
     * Deliberately not {@code Integer.getInteger}, which returns the default for anything it cannot parse -
     * so {@code -Dpc.streams.dispatch.pool.size=eight} would silently run a pool of 4 and the measurement
     * taken from that run would be attributed to a configuration that never existed.
     */
    private static int readPoolSizeProperty() {
        final String dotted = System.getProperty(PcDispatchSettings.POOL_SIZE_CONFIG);
        if (dotted != null) {
            return PcDispatchSettings.parsePoolSize(dotted,
                    PcDispatchSettings.systemPropertySource(PcDispatchSettings.POOL_SIZE_CONFIG));
        }
        final String legacy = System.getProperty(POOL_SIZE_PROPERTY);
        if (legacy != null) {
            return PcDispatchSettings.parsePoolSize(legacy,
                    PcDispatchSettings.systemPropertySource(POOL_SIZE_PROPERTY));
        }
        return DEFAULT_POOL_SIZE;
    }
}
