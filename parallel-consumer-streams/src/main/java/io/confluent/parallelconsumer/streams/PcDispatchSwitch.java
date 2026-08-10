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
 *
 * @author Antony Stubbs
 * @see PcTaskDispatcher
 */
public final class PcDispatchSwitch {

    /**
     * Set {@code -Dpc.streams.dispatch.enabled=false} to turn the seam off for a whole JVM and get stock
     * Kafka Streams dispatch back. Unset, or {@code =true}, means the seam is on. Tests should call
     * {@link #disable()} / {@link #enable(int)} instead, so they can put it back afterwards.
     */
    public static final String ENABLED_PROPERTY = "pc.streams.dispatch.enabled";

    /**
     * Worker threads per task, and simultaneously PC's {@code maxConcurrency} - see
     * {@link PcTaskDispatcher}, which uses it for both so that PC never hands out more work than the pool
     * can start.
     */
    public static final String POOL_SIZE_PROPERTY = "pc.streams.dispatch.poolSize";

    /**
     * Set {@code -Dpc.streams.wakeOnWork.enabled=false} to put the patched {@code StreamThread} back on a
     * single full-budget {@code Consumer#poll()} - see {@link PcWorkSignal} for what that costs.
     * <p>
     * Two reasons this exists rather than the seam being the only switch. It is an escape hatch on a fifth
     * patched Kafka class, which is the one with the widest blast radius if a future Kafka changes the poll
     * phase under us. And it is the <b>control arm</b>: the before/after measurement for wake-on-work has to
     * vary exactly one term, and flipping this leaves the build, the JVM, the broker and the warm-up
     * identical in a way that comparing against a parent commit never can.
     */
    public static final String WAKE_ON_WORK_PROPERTY = "pc.streams.wakeOnWork.enabled";

    private static final int DEFAULT_POOL_SIZE = 4;

    private static volatile boolean enabled = readEnabledProperty();

    private static volatile int poolSize = Integer.getInteger(POOL_SIZE_PROPERTY, DEFAULT_POOL_SIZE);

    private static volatile boolean wakeOnWork = readBooleanProperty(WAKE_ON_WORK_PROPERTY);

    private PcDispatchSwitch() {
    }

    public static boolean isEnabled() {
        return enabled;
    }

    /**
     * Whether the patched {@code StreamThread} should split its poll wait and let a worker completion end it.
     * <p>
     * Reports false whenever the seam is off, unconditionally: with records going through Kafka's own
     * {@code PartitionGroup} there are no workers, nothing can raise the signal, and a split wait would be
     * pure cost. That keeps the patch's condition to a single call - a seam-off run takes the stock poll
     * without the patch having to ask two questions.
     */
    public static boolean isWakeOnWorkEnabled() {
        return enabled && wakeOnWork;
    }

    /**
     * Turn wake-on-work off (or back on) for this JVM. Intended for the control arm of the benchmark; the
     * system property is the equivalent for a whole run.
     */
    public static void setWakeOnWork(final boolean wakeOnWorkEnabled) {
        wakeOnWork = wakeOnWorkEnabled;
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
        poolSize = Integer.getInteger(POOL_SIZE_PROPERTY, DEFAULT_POOL_SIZE);
        enabled = readEnabledProperty();
        wakeOnWork = readBooleanProperty(WAKE_ON_WORK_PROPERTY);
    }

    /**
     * Absent, the seam is on - depending on the artifact is the opt-in. A value that is neither
     * {@code true} nor {@code false} fails loudly rather than being silently read as "off": a typo in a
     * property whose whole job is to turn the seam off would otherwise leave the seam on, and the run would
     * look like a control arm while being nothing of the kind.
     */
    private static boolean readEnabledProperty() {
        return readBooleanProperty(ENABLED_PROPERTY);
    }

    /**
     * Absent means on, and anything that is not {@code true}/{@code false} throws. Shared by both switches
     * rather than copied, because the loud-failure rule above is the whole point and two copies is how one of
     * them quietly stops enforcing it.
     */
    private static boolean readBooleanProperty(final String property) {
        final String raw = System.getProperty(property);
        if (raw == null) {
            return true;
        }
        if ("true".equalsIgnoreCase(raw)) {
            return true;
        }
        if ("false".equalsIgnoreCase(raw)) {
            return false;
        }
        throw new IllegalArgumentException("System property " + property + " must be 'true' or "
                + "'false', was '" + raw + "'. The feature is ON unless this property says 'false'.");
    }
}
