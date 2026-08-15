package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Two languages, driven at the same time, with opposite verdicts - the property the whole fan-out depends
 * on, measured rather than configured.
 * <p>
 * <b>Why this is a test and not a properties file.</b> Nine language waves will run against this suite
 * concurrently, and the thing that must hold is that each one gets its own engine, its own loopback port
 * and its own verdict, with no shared state to make one language's failure look like another's. A
 * {@code junit-platform.properties} says only that somebody intended parallelism; this asserts that two
 * runners were genuinely in flight at once and that the suite still told them apart.
 * <p>
 * The second language is a deliberately broken runner rather than a real client, because the interesting
 * half of the claim is the DISAGREEMENT: green beside red, at the same moment, correctly attributed.
 * <p>
 * The green half is whichever binding this run selected first - a foreign runner when the selector named
 * one, the core control arm when it did not. A row that selected only the control arm must still run this
 * proof: "no language was selected" is not a licence to quietly skip a test.
 *
 * @author Antony Stubbs
 */
@Slf4j
class LanguagesRunInParallelTest {

    /** Long enough that the Go run cannot help but overlap it, short enough not to pad the build. */
    private static final int BROKEN_RUNNER_DAWDLE_SECONDS = 8;

    @Test
    void twoLanguagesRunAtOnceAndAreJudgedSeparately() throws Exception {
        var scenario = ConformanceScenarios.PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET;
        var real = ConformanceBindings.aSelectedBinding();
        var broken = LanguageRunners.deliberatelyFailing(
                AbsentAndBrokenRunnersFailTest.writeCrashingRunner("parallel-broken-runner",
                        BROKEN_RUNNER_DAWDLE_SECONDS));

        // Built before the clock starts: a first cold build of a foreign runner is minutes of toolchain
        // work and would swamp the overlap measurement with something that is not the thing being measured.
        real.ensureAvailable();

        var realStart = new AtomicLong();
        var realEnd = new AtomicLong();
        var brokenStart = new AtomicLong();
        var brokenEnd = new AtomicLong();

        var pool = Executors.newFixedThreadPool(2);
        try {
            var futures = pool.invokeAll(List.<Callable<Throwable>>of(
                    timed(realStart, realEnd, () -> ConformanceDriver.drive(real, scenario)),
                    timed(brokenStart, brokenEnd, () -> ConformanceDriver.drive(broken, scenario))));

            var realOutcome = futures.get(0).get();
            var brokenOutcome = futures.get(1).get();

            assertWithMessage("the real client's verdict, run beside a failing one")
                    .that(realOutcome).isNull();
            assertWithMessage("the broken runner must FAIL, and its failure must not be attributed to the "
                    + "language running beside it")
                    .that(brokenOutcome).isNotNull();
            assertWithMessage("the broken runner's failure names itself")
                    .that(brokenOutcome.getMessage()).contains("deliberately-failing");
            assertWithMessage("and does not name the language that passed")
                    .that(brokenOutcome.getMessage()).doesNotContain(real.name() + " binding's exit status");

            // The overlap itself, from this test's own clock rather than a shared high-water mark - a
            // global peak could have been set by some other test and would read as a pass here vacuously.
            assertWithMessage("the two runs must OVERLAP: real [%s,%s], broken [%s,%s] (JVM-wide peak "
                            + "concurrent runners so far: %s)",
                    realStart.get(), realEnd.get(), brokenStart.get(), brokenEnd.get(),
                    ConformanceDriver.peakConcurrentRuns())
                    .that(realStart.get() < brokenEnd.get() && brokenStart.get() < realEnd.get()).isTrue();
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /** Runs the body, returns the failure it produced (or null), and records when it ran. */
    private static Callable<Throwable> timed(AtomicLong start, AtomicLong end, Runnable body) {
        return () -> {
            start.set(System.nanoTime());
            try {
                body.run();
                return null;
            } catch (AssertionError | RuntimeException failure) {
                log.info("Runner verdict: {}", failure.toString());
                return failure;
            } finally {
                end.set(System.nanoTime());
            }
        };
    }
}
