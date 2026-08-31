package bz.stub.parallelconsumer.streams;

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
 * <h2>It defaults to OFF, and this is an opt-in preview</h2>
 * Turn it on for a whole JVM with {@code -Dpc.streams.dispatch.enabled=true}, or per test with
 * {@link #enable(int)}.
 * <p>
 * <b>The reason is a missing refusal, not caution.</b> Joins, windows, suppression, exactly-once and
 * stream-time punctuation are unsupported on the PC path <em>and are not yet refused</em> - a topology using
 * one of them is dispatched anyway and gets wrong behaviour with nothing in the log to say so. Until the
 * refusal envelope exists, the only honest default is one that cannot silently mis-run a topology nobody
 * checked. When it lands, the default becomes a real choice again rather than a hedge.
 * <p>
 * <b>This reverses an inherited decision, and the argument it reverses was a different one.</b> The seam
 * defaulted <em>on</em> in the feasibility study (astubbs#271) on the grounds that depending on a separate,
 * loudly-labelled alpha artifact <em>is</em> the opt-in, so demanding a system property as well is friction
 * that buys nothing. That is still a good argument, and it is why this is not written as "on is unsafe":
 * it beats the objection it was aimed at, which was that an earlier revision defaulted off merely so a
 * control-arm test would stay stock without having to say so. A test concern must never be paid for by
 * every user of the artifact, and it is not being paid for here - the arms below still state their
 * requirement at each site. What the artifact-is-the-opt-in argument does not cover is a user who opts in
 * to <em>per-key concurrency</em> and gets <em>silently altered semantics</em> on a topology shape nobody
 * refused. Restore the on-by-default the moment that gap closes; do not restore it merely because this
 * paragraph looks like timidity.
 * <p>
 * Tests that want the stock path still say so explicitly with {@link #disable()} rather than leaning on the
 * default, because a control arm that is only a control by default stops being one the moment the default
 * moves - which it has now done twice.
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
     * Set {@code -Dpc.streams.dispatch.enabled=true} to turn the seam on for a whole JVM. Unset, or
     * {@code =false}, leaves stock Kafka Streams dispatch in place. Tests should call
     * {@link #enable(int)} / {@link #disable()} instead, so they can put it back afterwards.
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

    /**
     * Absent means OFF for the seam, and absent means ON for wake-on-work. The two defaults differ because
     * the questions differ: the seam asks "may this JVM run topologies through PC at all", which nothing has
     * yet made safe to answer yes by default, while wake-on-work asks "given that it is running through PC,
     * should the poll wait be split", where the stock answer is measurably the wrong one and the whole
     * mechanism exists to fix it. Wake-on-work is unreachable while the seam is off, so it never applies to
     * a JVM that did not ask for the seam - see {@link #isWakeOnWorkEnabled()}.
     */
    private static final boolean SEAM_DEFAULT = false;
    private static final boolean WAKE_ON_WORK_DEFAULT = true;

    private static volatile boolean enabled = readBooleanProperty(ENABLED_PROPERTY, SEAM_DEFAULT);

    private static volatile int poolSize = Integer.getInteger(POOL_SIZE_PROPERTY, DEFAULT_POOL_SIZE);

    private static volatile boolean wakeOnWork = readBooleanProperty(WAKE_ON_WORK_PROPERTY, WAKE_ON_WORK_DEFAULT);

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
     * Put the switch back to what this JVM started with - each property if it was set, its default
     * otherwise. Intended as a test teardown: leaving the switch wherever the last test left it re-creates
     * exactly the implicit-default coupling that stating each arm's requirement was meant to remove.
     */
    public static void resetToDefault() {
        poolSize = Integer.getInteger(POOL_SIZE_PROPERTY, DEFAULT_POOL_SIZE);
        enabled = readBooleanProperty(ENABLED_PROPERTY, SEAM_DEFAULT);
        wakeOnWork = readBooleanProperty(WAKE_ON_WORK_PROPERTY, WAKE_ON_WORK_DEFAULT);
    }

    /**
     * Absent takes {@code whenAbsent}; anything that is not {@code true}/{@code false} throws rather than
     * being silently read as the default. A typo in a property whose whole job is to move the seam would
     * otherwise leave it wherever it already was, and the run would look like the arm it was asked for while
     * being nothing of the kind.
     * <p>
     * Shared by both switches rather than copied, because that loud-failure rule is the whole point and two
     * copies is how one of them quietly stops enforcing it. The default is a parameter for the same reason:
     * the two switches genuinely differ (see {@link #SEAM_DEFAULT}), and a second reader is how they would
     * drift.
     */
    private static boolean readBooleanProperty(final String property, final boolean whenAbsent) {
        final String raw = System.getProperty(property);
        if (raw == null) {
            return whenAbsent;
        }
        if ("true".equalsIgnoreCase(raw)) {
            return true;
        }
        if ("false".equalsIgnoreCase(raw)) {
            return false;
        }
        throw new IllegalArgumentException("System property " + property + " must be 'true' or "
                + "'false', was '" + raw + "'. Absent, it is "
                + (whenAbsent ? "ON" : "OFF") + ".");
    }
}
