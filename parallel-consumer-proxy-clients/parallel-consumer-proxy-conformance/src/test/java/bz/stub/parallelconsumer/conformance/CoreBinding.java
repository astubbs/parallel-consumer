package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ExceptionInUserFunctionException;
import bz.stub.parallelconsumer.RecordContext;
import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The engine itself, as a binding: Parallel Consumer driven by a plain Java function, with no proxy, no
 * wire and no child process between the scenario and the code it is a claim about.
 * <p>
 * <b>This is the suite's control arm, and it is why the arm exists at all.</b> Every other binding puts a
 * client, a protocol and a language runtime between the scenario and the engine, so a red run has three
 * suspects and the client is always the first one looked at. If a scenario is red HERE, the scenario is
 * wrong - there is nothing else left for it to be - and nobody spends an afternoon debugging an innocent
 * Ruby client over a bad assertion. It also makes this suite the single place "correct" is defined for the
 * product, rather than a client-only harness that happens to share some names with it.
 * <p>
 * <b>It implements the same prescription as a foreign runner, in the same terms.</b> The four
 * {@link RunnerBehaviour} tokens, {@link RunnerContract#PRESCRIBED_FAILURE_REASON} verbatim, one
 * {@link DispatchObservation} per delivery, and an exit status as the verdict channel - a run that could not
 * do what was prescribed reports {@link RunnerContract#EXIT_BEHAVIOUR_FAILED} exactly as a runner process
 * would. Nothing about the assertions can therefore be written to suit this binding: they cannot tell it
 * from the others.
 * <p>
 * <b>The one thing it does differently is the {@code report-nothing} hold</b>, and it does it better. A
 * runner process buys the suite a live client for the contract's fixed 3s and then exits; this binding stays
 * blocked in the user function until the run is closed, which is after the assertions have been made - so
 * the negative control watches a record that is genuinely still out with a worker, for exactly as long as it
 * looks.
 *
 * @author Antony Stubbs
 * @see ConformanceBinding
 */
@Slf4j
public final class CoreBinding implements ConformanceBinding {

    /** The name this binding answers to in the matrix and on the selector's command line. */
    public static final String NAME = "core";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Run execute(ProxyHarness harness, ConformanceScenario scenario) {
        var run = new CoreRun(scenario);
        harness.start(run::process);
        harness.seed();
        run.awaitPrescribedBehaviour();
        return run;
    }

    @Override
    public String toString() {
        return NAME;
    }

    /**
     * One scenario's run against the engine: the tracker, the prescribed behaviour, and the transcript it
     * produces. Per-run state lives here rather than on the binding because one binding instance drives the
     * whole matrix, concurrently.
     */
    private static final class CoreRun implements Run {

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

        private CoreRun(ConformanceScenario scenario) {
            this.scenario = scenario;
            this.allObserved = new CountDownLatch(scenario.expectedDispatches());
            this.allCompleted = new CountDownLatch(scenario.expectedDispatches());
        }

        /**
         * The user function: observe the delivery, then do exactly what the scenario prescribed. It is the
         * same shape as every runner's processor, deliberately - a binding that took a shortcut here would
         * be conforming to a contract nobody else implements.
         */
        private void process(RecordContext<String, String> record) throws Exception {
            int ordinal = observe(record);

            switch (scenario.behaviour()) {
                case SUCCEED -> allCompleted.countDown();

                // Never report. Blocking in the user function is how the JVM says "this record's function
                // has not returned", and it holds until the observation window closes - which is after the
                // assertions, so the negative control is watching a record that is still genuinely out.
                case REPORT_NOTHING -> {
                    if (!await(windowClosed, scenario.runnerBudget().toSeconds(),
                            "the observation window never closed while a record was held")) {
                        abandon();
                    }
                    allCompleted.countDown();
                }

                case FAIL_THEN_SUCCEED -> {
                    allCompleted.countDown();
                    if (record.getNumberOfFailedAttempts() == 0) {
                        // The reason is the contract's fixed literal, never a message this binding composes:
                        // the suite asserts the redelivery carries it back verbatim.
                        throw new IllegalStateException(RunnerContract.PRESCRIBED_FAILURE_REASON);
                    }
                }

                case HOLD_FIRST_UNTIL_SECOND -> {
                    if (ordinal == 1 && !await(secondArrived, scenario.runnerBudget().toSeconds(),
                            "no second delivery arrived while the first was held")) {
                        // The same verdict a runner process gives here: it could not do what was
                        // prescribed, so the run fails rather than reporting a plausible-looking outcome.
                        abandon();
                        throw new IllegalStateException("conformance: no second delivery arrived while the "
                                + "first was held");
                    }
                    allCompleted.countDown();
                }
            }
        }

        /** Prints the delivery into the transcript and returns its 1-based ordinal in arrival order. */
        private int observe(RecordContext<String, String> record) {
            // The ordinal and the observation are taken under one lock so the transcript's ORDER is the
            // arrival order the ordinals were handed out in - two shards deliver concurrently here.
            int ordinal;
            synchronized (observations) {
                ordinal = observed.incrementAndGet();
                var reason = lastFailureReason(record);
                observations.add(new DispatchObservation(record.key(), record.offset(),
                        record.getNumberOfFailedAttempts() + 1, reason));
            }
            log.info("core binding observed delivery {}: {}", ordinal, record);

            if (ordinal >= 2) {
                secondArrived.countDown();
            }
            allObserved.countDown();
            return ordinal;
        }

        /**
         * Waits for the prescription to finish, and sets the verdict. {@code report-nothing} completes at
         * OBSERVATION, because by prescription its record is never reported and so can never complete;
         * every other behaviour completes when the last delivery has had its outcome decided.
         */
        private void awaitPrescribedBehaviour() {
            var waitOn = scenario.behaviour() == RunnerBehaviour.REPORT_NOTHING ? allObserved : allCompleted;
            if (!await(waitOn, scenario.runnerBudget().toSeconds(), "the prescribed behaviour did not "
                    + "complete: observed " + observed.get() + " of " + scenario.expectedDispatches()
                    + " deliveries")) {
                exitCode = RunnerContract.EXIT_BEHAVIOUR_FAILED;
            }
        }

        /**
         * The reason text a foreign client would have been handed, derived from the Throwable core kept.
         * <p>
         * <b>Two normalisations, and neither is this binding's invention.</b> Core's
         * {@code getLastFailureReason} returns a NULL Optional before the first failure, because
         * {@code WorkContainer#lastFailureReason} has no initializer; and core wraps whatever the user
         * function threw in an {@link ExceptionInUserFunctionException} whose own message is the fixed
         * "Error occurred in code supplied by user", so the reason the scenario prescribed is the CAUSE's.
         * The engine's serializer meets both and answers them the same way - {@code RecordCodec}'s
         * {@code lastFailureReasonText} owns that rule, and this is the in-process side of the same
         * translation. Skipping the unwrap here would have made the control arm red on a scenario every
         * foreign client passes, which is the control arm failing at its own job.
         */
        private static String lastFailureReason(RecordContext<String, String> record) {
            return Optional.ofNullable(record.getLastFailureReason()).flatMap(reason -> reason)
                    .map(CoreRun::unwrap)
                    .map(cause -> cause.getMessage() != null ? cause.getMessage() : cause.toString())
                    .orElse("");
        }

        private static Throwable unwrap(Throwable recorded) {
            Throwable cause = recorded;
            while (cause instanceof ExceptionInUserFunctionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            return cause;
        }

        /**
         * The in-process spelling of "exit 1": the prescription could not be carried out, so the verdict is
         * set and every wait is released rather than left to time out one by one.
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
                return new RunnerTranscript(NAME,
                        "in-process core binding: --scenario " + scenario.name()
                                + " --behaviour " + scenario.behaviour().token()
                                + " --expect-dispatches " + scenario.expectedDispatches(),
                        exitCode, List.copyOf(observations), stdout, diagnostics.toString());
            }
        }

        @Override
        public void close() {
            windowClosed.countDown();
            // A held record is released back into an ordinary success, so the harness's own close drains
            // rather than waiting out a user function that is never going to return.
            await(allCompleted, ProxyHarness.CONVERGENCE_BUDGET.toSeconds(),
                    "a held record was still executing when the run closed");
        }
    }
}
