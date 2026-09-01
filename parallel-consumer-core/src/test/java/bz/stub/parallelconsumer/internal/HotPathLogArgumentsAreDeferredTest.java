package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * The control loop logs its queue depths through {@code log.atTrace().addArgument(Supplier)}, and that choice is
 * load-bearing rather than stylistic: {@code getNumberOfWorkQueuedInShardsAwaitingSelection()} sums a counter over
 * every processing shard, so evaluating it once per control-loop pass is O(shards) on the hottest path in the
 * library. Under {@code KEY} ordering the shard map is keyed per record key, so that scan grows with in-flight key
 * cardinality precisely when the loop is spinning fastest.
 * <p>
 * <b>What this test protects is an SLF4J behaviour we depend on, not our own code.</b> SLF4J defers formatting but
 * not argument evaluation, so the classic {@code log.trace("...", expensive())} form runs the scan at every level
 * including the ones production runs at. The fluent form is only free because {@code Logger#atTrace()} returns the
 * NOP builder when trace is disabled, and the NOP builder's {@code addArgument(Supplier)} never calls
 * {@code get()}. If a future SLF4J upgrade changed that - evaluating suppliers eagerly, or returning a real
 * builder regardless of level - the fix would silently stop working and nothing else in the build would notice:
 * the source still reads correctly, and the cost is a throughput number nobody is gating on.
 * <p>
 * <b>This test is the only automated guard on the rule.</b> A dedicated source gate existed briefly and was
 * deleted: one bespoke script for one pattern is what {@code bin/lib/source-patterns.mjs} exists to replace, and
 * the rule was not carried over as a row there. So nothing mechanical catches somebody writing the eager form
 * again at a NEW call site - this test only proves SLF4J still behaves the way the fix depends on. If that gap
 * ever costs something, a row in the rule table is the cheap fix, not another script.
 * <p>
 * Background: the eager form reached the control loop on astubbs/parallel-consumer#29, and a controlled
 * measurement established it as the cause of that branch's throughput shortfall - roughly 43,500 records/second
 * failing, 77,000 passing, one main-code term changed, corroborated by a two-arm local run. See
 * {@code docs/inflight/perf-control-loop-log-argument-evaluated-eagerly.md}.
 */
class HotPathLogArgumentsAreDeferredTest {

    private static final Logger log = LoggerFactory.getLogger(HotPathLogArgumentsAreDeferredTest.class);

    /**
     * The level is PINNED rather than inherited, and that is the difference between a test and a coin flip.
     * This suite's level is configurable ({@code -Dpc.log.level=trace}), so a test that adapted to whatever it
     * found - asserting one evaluation when trace was on, zero when it was off - would pass under every
     * configuration while proving deferral under only one of them. Both cases below therefore run at a level
     * where trace is DISABLED, which is the only condition the production claim is about.
     */
    private ch.qos.logback.classic.Logger classicLogger;
    private ch.qos.logback.classic.Level originalLevel;

    @BeforeEach
    void pinLevelToTraceDisabled() {
        classicLogger = (ch.qos.logback.classic.Logger) log;
        originalLevel = classicLogger.getLevel();
        classicLogger.setLevel(ch.qos.logback.classic.Level.INFO);
        assertThat(log.isTraceEnabled()).isFalse();
    }

    @AfterEach
    void restoreLevel() {
        classicLogger.setLevel(originalLevel);
    }

    /**
     * The whole point of the fluent form: with trace disabled, the supplier is never called, so the O(shards)
     * scan the control loop passes here costs nothing at all rather than merely skipping its string formatting.
     */
    @Test
    void supplierArgumentsAreNotEvaluatedWhenTheLevelIsDisabled() {
        AtomicInteger evaluations = new AtomicInteger();

        log.atTrace()
                .addArgument(() -> {
                    evaluations.incrementAndGet();
                    return "expensive";
                })
                .log("deferred argument: {}");

        assertThat(evaluations.get()).isEqualTo(0);
    }

    /**
     * The control arm, and the behaviour the control loop was actually suffering from: at the SAME disabled
     * level, a plain argument is still evaluated. Without this case the test above could pass because nothing
     * ran at all - a broken logger, a misconfigured level - rather than because deferral worked.
     */
    @Test
    void plainArgumentsAreEvaluatedEvenWhenTheLevelIsDisabled() {
        AtomicInteger evaluations = new AtomicInteger();

        log.trace("eager argument: {}", evaluate(evaluations));

        assertThat(evaluations.get()).isEqualTo(1);
    }

    private String evaluate(AtomicInteger counter) {
        counter.incrementAndGet();
        return "expensive";
    }
}
