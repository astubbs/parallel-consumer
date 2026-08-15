package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The runner contract carried out <b>in this JVM</b>: the four {@link RunnerBehaviour} tokens, the fixed
 * failure literal, one {@link DispatchObservation} per delivery, and an exit status as the verdict channel -
 * written once for every binding whose "wire" is a method call.
 * <p>
 * <b>It exists so that an in-process binding cannot be held to a lighter contract than a foreign runner.</b>
 * Three bindings need it today - the engine driven by a plain Java function, and the two Java client
 * transports - and the moment the prescription were spelled out three times, the three would drift and the
 * agreement between them would stop being evidence. The scenarios cannot tell which binding produced a
 * transcript, which is exactly the property {@link ConformanceBinding} exists to protect.
 * <p>
 * <b>The one thing it does differently from a runner process is the {@code report-nothing} hold, and it does
 * it better.</b> A runner buys the suite a live client for the contract's fixed 3s and then exits; this stays
 * blocked in the user function until {@link #close()}, which is after the assertions have been made - so the
 * negative control watches a record that is genuinely still out with a worker, for exactly as long as it
 * looks.
 *
 * @author Antony Stubbs
 * @see CoreBinding
 * @see JvmClientBinding
 */
@Slf4j
final class PrescribedRun implements ConformanceBinding.Run {

    /** The binding this run belongs to - it names the transcript and every failure message. */
    private final String bindingName;

    private final ConformanceScenario scenario;

    private final List<DispatchObservation> observations = new ArrayList<>();

    private final StringBuilder diagnostics = new StringBuilder();

    private final AtomicInteger observed = new AtomicInteger();

    private final CountDownLatch secondArrived = new CountDownLatch(1);

    private final CountDownLatch allObserved;

    private final CountDownLatch allCompleted;

    /** Released when the observation window closes, so a held record can never outlive the assertions. */
    private final CountDownLatch windowClosed = new CountDownLatch(1);

    private volatile int exitCode = RunnerContract.EXIT_OK;

    PrescribedRun(String bindingName, ConformanceScenario scenario) {
        this.bindingName = bindingName;
        this.scenario = scenario;
        this.allObserved = new CountDownLatch(scenario.expectedDispatches());
        this.allCompleted = new CountDownLatch(scenario.expectedDispatches());
    }

    /**
     * One delivery: record the observation, then do exactly what the scenario prescribed - blocking here for
     * as long as the prescription says the record is held, because a function that has not returned is how
     * the JVM says a record is still out.
     * <p>
     * The caller turns the answer into whatever its own layer calls a verdict: core's binding throws, a
     * client binding returns {@code Outcome.failure}. Neither is free to decide anything else, which is the
     * point of the closed token set.
     *
     * @return the failure reason to report for this record, or empty to report success
     */
    Optional<String> deliver(String key, long offset, int attempt, String lastFailureReason) {
        int ordinal = observe(key, offset, attempt, lastFailureReason);

        // A switch EXPRESSION over the closed token set, so a behaviour added to RunnerBehaviour fails this
        // file to compile rather than falling through a default into a plausible-looking success.
        return switch (scenario.behaviour()) {
            case SUCCEED -> {
                allCompleted.countDown();
                yield Optional.empty();
            }

            // Never report. Blocking is how this layer says "this record's function has not returned", and
            // it holds until the observation window closes - which is after the assertions, so the negative
            // control is watching a record that is still genuinely out.
            case REPORT_NOTHING -> {
                if (!await(windowClosed, scenario.runnerBudget().toSeconds(),
                        "the observation window never closed while a record was held")) {
                    abandon();
                }
                allCompleted.countDown();
                yield Optional.empty();
            }

            case FAIL_THEN_SUCCEED -> {
                allCompleted.countDown();
                // The reason is the contract's fixed literal, never a message this binding composes: the
                // suite asserts the redelivery carries it back verbatim.
                yield attempt == 1 ? Optional.of(RunnerContract.PRESCRIBED_FAILURE_REASON) : Optional.empty();
            }

            case HOLD_FIRST_UNTIL_SECOND -> {
                if (ordinal == 1 && !await(secondArrived, scenario.runnerBudget().toSeconds(),
                        "no second delivery arrived while the first was held")) {
                    // The same verdict a runner process gives here: it could not do what was prescribed, so
                    // the run fails rather than reporting a plausible-looking outcome.
                    abandon();
                    yield Optional.of("conformance: no second delivery arrived while the first was held");
                }
                allCompleted.countDown();
                yield Optional.empty();
            }
        };
    }

    /** Prints the delivery into the transcript and returns its 1-based ordinal in arrival order. */
    private int observe(String key, long offset, int attempt, String lastFailureReason) {
        // The ordinal and the observation are taken under one lock so the transcript's ORDER is the arrival
        // order the ordinals were handed out in - two shards deliver concurrently here.
        int ordinal;
        synchronized (observations) {
            ordinal = observed.incrementAndGet();
            observations.add(new DispatchObservation(key, offset, attempt,
                    lastFailureReason == null ? "" : lastFailureReason));
        }
        log.info("{} binding observed delivery {}: key={} offset={} attempt={}", bindingName, ordinal, key,
                offset, attempt);

        if (ordinal >= 2) {
            secondArrived.countDown();
        }
        allObserved.countDown();
        return ordinal;
    }

    /**
     * Waits for the prescription to finish, and sets the verdict. {@code report-nothing} completes at
     * OBSERVATION, because by prescription its record is never reported and so can never complete; every
     * other behaviour completes when the last delivery has had its outcome decided.
     */
    void awaitPrescribedBehaviour() {
        var waitOn = scenario.behaviour() == RunnerBehaviour.REPORT_NOTHING ? allObserved : allCompleted;
        if (!await(waitOn, scenario.runnerBudget().toSeconds(), "the prescribed behaviour did not complete: "
                + "observed " + observed.get() + " of " + scenario.expectedDispatches() + " deliveries")) {
            exitCode = RunnerContract.EXIT_BEHAVIOUR_FAILED;
        }
    }

    /**
     * The in-process spelling of "exit 1": the prescription could not be carried out, so the verdict is set
     * and every wait is released rather than left to time out one by one.
     */
    private void abandon() {
        exitCode = RunnerContract.EXIT_BEHAVIOUR_FAILED;
        while (allObserved.getCount() > 0) {
            allObserved.countDown();
        }
        while (allCompleted.getCount() > 0) {
            allCompleted.countDown();
        }
    }

    private boolean await(CountDownLatch latch, long seconds, String whatWentWrong) {
        try {
            if (latch.await(seconds, TimeUnit.SECONDS)) {
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (diagnostics) {
            diagnostics.append(whatWentWrong).append('\n');
        }
        return false;
    }

    @Override
    public RunnerTranscript transcript() {
        synchronized (observations) {
            var stdout = observations.stream()
                    .map(o -> RunnerContract.DISPATCH_LINE_PREFIX + "key=" + o.key() + " offset=" + o.offset()
                            + " attempt=" + o.attempt() + " reason=" + o.reason())
                    .reduce("", (all, line) -> all + line + "\n");
            return new RunnerTranscript(bindingName,
                    "in-process " + bindingName + " binding: --scenario " + scenario.name()
                            + " --behaviour " + scenario.behaviour().token()
                            + " --expect-dispatches " + scenario.expectedDispatches(),
                    exitCode, List.copyOf(observations), stdout, diagnostics.toString());
        }
    }

    @Override
    public void close() {
        windowClosed.countDown();
        // A held record is released back into an ordinary success, so the engine's own close drains rather
        // than waiting out a user function that is never going to return.
        await(allCompleted, ProxyHarness.CONVERGENCE_BUDGET.toSeconds(),
                "a held record was still executing when the run closed");
    }
}
