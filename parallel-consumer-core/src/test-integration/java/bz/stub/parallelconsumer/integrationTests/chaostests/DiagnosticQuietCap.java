package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

/**
 * How long {@code -Dchaos.diagnoseStallRecovery=true} may watch the quiet phase, given the scenario's
 * own {@code @Timeout}.
 * <p>
 * <b>Why this is its own class and not a method on the scenario.</b> Every class in this package
 * descends from {@code BrokerIntegrationTest}, whose static initialiser calls
 * {@code kafkaContainer.start()} - and initialising a subclass initialises its superclasses, so even
 * a {@code static} helper hanging off {@code AbstractRevokeUnderWorkScenario} boots Kafka under
 * Testcontainers the moment a test calls it. Two successive attempts to give this arithmetic a
 * "broker-free" test failed on exactly that, the second while asserting broker-freedom in its own
 * javadoc; the measured tell was a pure-{@link Duration} test taking ~4.6s and its failsafe report
 * carrying {@code tryStart}. Only a type outside that hierarchy actually escapes it.
 * <p>
 * The defect this arithmetic prevents is silent: a watch longer than the enclosing {@code @Timeout}
 * means JUnit kills the run mid-observation, and a killed run is neither of the two outcomes the
 * diagnostic exists to distinguish ("the backlog drained" / "it did not"). So the only interesting
 * result was unreachable, which is why the experiment two documents called cheap went unrun for five
 * days. Covered by {@code DiagnosticQuietCapIT}.
 */
@Slf4j
final class DiagnosticQuietCap {

    /**
     * Requested watch length. Override with {@code -Dchaos.diagnosticQuietCapMinutes=<n>}.
     * <p>
     * The 20-minute default cannot fit under the 600s {@code @Timeout} every scenario carries, which
     * is deliberate: {@link #within} shortens it and says so, rather than the default silently
     * promising a watch no scenario can deliver.
     */
    static final Duration REQUESTED =
            Duration.ofMinutes(Integer.getInteger("chaos.diagnosticQuietCapMinutes", 20));

    /**
     * Held back from the {@code @Timeout} so the quiet wait ends on its OWN cap, leaving time for
     * {@code settleRun} (conductor stop, drain joins, producer join, fleet settle) and the final
     * assertions. Without it, shortening the watch would merely move the silent kill from the wait
     * into the teardown.
     * <p>
     * <b>90s is a typical-case figure, NOT a derivation, and the worst case escapes it - stated
     * because a bare constant reads as calculated.</b> The bounded parts are the 10s producer join
     * and {@code joinDrainers}' shared 60s budget; the unbounded part is {@code settleFleet}, which
     * waits up to 15s per close-pending instance SEQUENTIALLY, so a 10-14 instance fleet with most
     * instances still closing can reach 150-210s on its own - worst case exceeds this by more than 2x.
     * <p>
     * <b>Why it is not simply raised.</b> The reserve is subtracted from the watch, so a
     * worst-case-sized reserve would spend the diagnostic's entire budget defending against an
     * extreme. When it is exceeded the JUnit kill lands in teardown, AFTER the watch has produced its
     * answer, and only in this opt-in mode: what is lost is the run summary, not the result.
     * <b>Do not raise this to fix a teardown that got slower</b> - bound the teardown instead, or the
     * diagnostic quietly gets shorter every time the fleet does.
     */
    static final Duration TEARDOWN_RESERVE = Duration.ofSeconds(90);

    private DiagnosticQuietCap() {
    }

    /**
     * The watch this run can actually deliver: {@link #REQUESTED}, shortened to what {@code ceiling}
     * leaves after {@code spent} and {@link #TEARDOWN_RESERVE}.
     *
     * @param ceiling      the scenario's {@code @Timeout} budget, or {@code null} when it has none
     * @param spent        how much of that budget the run has already used
     * @param scenarioName only for the operator-facing messages
     * @throws IllegalStateException when no observation time remains - a diagnostic that cannot watch
     *                               anything must say so rather than run and report an outcome it
     *                               never observed
     */
    static Duration within(Duration ceiling, Duration spent, String scenarioName) {
        if (ceiling == null) {
            log.warn("=== no @Timeout on {} - nothing to fit inside, so the full {} watch stands ===",
                    scenarioName, REQUESTED);
            return REQUESTED;
        }
        Duration available = ceiling.minus(spent).minus(TEARDOWN_RESERVE);
        Duration timeoutNeededForFullWatch = spent.plus(TEARDOWN_RESERVE).plus(REQUESTED);

        if (available.isNegative() || available.isZero()) {
            throw new IllegalStateException(String.format(
                    "chaos.diagnoseStallRecovery has no time left to watch anything: %s is allowed %s in "
                            + "total and %s is already spent, with %s held back for shutdown. Raise this "
                            + "scenario's @Timeout to at least %s to get the requested %s watch.",
                    scenarioName, ceiling, spent, TEARDOWN_RESERVE, timeoutNeededForFullWatch, REQUESTED));
        }
        if (REQUESTED.compareTo(available) <= 0) {
            return REQUESTED;
        }
        log.warn("=== diagnostic watch SHORTENED to fit the test's own time limit: you asked for {}, this "
                        + "run can only give {}. {} is allowed {} in total, {} of it is already spent, and "
                        + "{} is held back for shutdown. The watch will now end by itself, so 'the backlog "
                        + "did not drain' is a real answer rather than JUnit killing the test mid-look. To "
                        + "get the full {}, raise this scenario's @Timeout to at least {}. ===",
                REQUESTED, available, scenarioName, ceiling, spent, TEARDOWN_RESERVE, REQUESTED,
                timeoutNeededForFullWatch);
        return available;
    }
}
