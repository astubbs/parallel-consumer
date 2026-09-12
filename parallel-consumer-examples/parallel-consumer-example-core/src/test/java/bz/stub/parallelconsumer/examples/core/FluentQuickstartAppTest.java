package bz.stub.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.fluent.ParkedView;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import bz.stub.parallelconsumer.sandbox.Bound;
import bz.stub.parallelconsumer.sandbox.Sandbox;
import ch.qos.logback.classic.Level;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The primary success signal (KD7, AE24): the README's first example, started with no broker anywhere, and asked
 * whether it did what the README says it does.
 *
 * <h2>Why this is the signal rather than a compile check</h2>
 * A quickstart that compiles proves the names still exist. This one runs: records are generated into the
 * definition's own topics, decoded by each route's own deserialiser, handed to each route's own function, and the
 * outcomes they report are read back off the instance and off the meters. When the fluent API drifts - a renamed
 * method, an outcome that stops being counted, a park that quietly becomes a retry - this goes red, and so does the
 * README that includes this example's tagged region.
 *
 * <h2>What the run looks like</h2>
 * Ten seconds at fifty records per second <em>per topic</em>, so about five hundred orders and about five hundred
 * parcel scans. The orders route succeeds or filters; the scans route's downstream is always down, so its records
 * exhaust their two retries at two hundred milliseconds each and park roughly four hundred milliseconds after they
 * arrive - well inside the bound, which is what makes "a record parked before the run ended" a fact about the
 * definition rather than a race.
 *
 * <h2>Why the counts are floors rather than numbers</h2>
 * The generator paces on wall-clock time and the run shares a machine with whatever else CI is doing, so the record
 * counts are asserted as floors. What is asserted exactly is what the definition fixes: which topics exist, how
 * many times a parked record's function ran, and which outcome each route reached.
 */
@Slf4j
@Timeout(300)
class FluentQuickstartAppTest {

    /**
     * The forty-line budget from the plan's Goal Capsule: one screen, so a returning developer can read the whole
     * definition without scrolling. Counted over the region the README includes, not over the file.
     */
    private static final int ONE_SCREEN_BUDGET = 40;

    private static final Path QUICKSTART_SOURCE =
            Paths.get("src/main/java/bz/stub/parallelconsumer/examples/core/FluentQuickstartApp.java");

    /**
     * The two topics the quickstart routes, spelled here because the example spells them as literals: the constants
     * it used to hold read badly in a README, where a reader can see the reference but not the declaration, so they
     * went with the wrapper. A test needs names for them either way, and these are asserted against what the run
     * actually generated, so a drift between the two shows up as a topic nothing was published to.
     */
    private static final String ORDERS_TOPIC = "orders";

    private static final String SCANS_TOPIC = "parcel-scans";

    private static final String OUTCOME_COUNTER = "pc.route.records";

    private static final int RATE_PER_SECOND = 50;

    private static final Duration RUN = Duration.ofSeconds(10);

    private static final int EXPECTED_PER_TOPIC = RATE_PER_SECOND * (int) RUN.getSeconds();

    private static final int LOWEST_CREDIBLE_PER_TOPIC = EXPECTED_PER_TOPIC / 2;

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    /**
     * Outcome counters read at the moment the instance sweeps them.
     * <p>
     * The instance deregisters its meters as it closes, deliberately and before the engine's own shutdown, so a test
     * that read them afterwards would read nothing at all - the registry is empty by then, which is the contract
     * and not an accident. Recording them on the way out is how the run's outcomes are asserted without pausing the
     * run to look.
     */
    private final Map<String, Double> outcomesAtClose = new ConcurrentHashMap<>();

    /**
     * AE24, whole: the definition the README shows, unaltered, against generated records with no broker; both
     * routes see their own topic's records decoded into their own type; the failing route's records park before the
     * bound; and the console says so.
     */
    @Test
    void theQuickstartRunsInTheSandboxAndParksWhatItCannotProcess() throws IOException {
        recordOutcomeCountersAsTheyAreSweptAway();
        FluentQuickstartApp app = new FluentQuickstartApp();
        ParallelConsumerDefinition pc = FluentQuickstartApp.defineConsumer().withMetrics(registry);

        ParallelConsumerInstance instance;
        Sandbox sandbox;
        // The key pool is the one setting here that is not cosmetic. Under key ordering a parked record holds its
        // key (R11), so with the sandbox's default pool of ten keys every scan after the first on each key would
        // queue behind a parked one, and the instance's drain-first close would then wait out its whole drain timeout
        // on work it can never take. That is the documented behaviour rather than a defect - the README says so
        // beside park - and what it means for a sandbox run is that ten keys models a topic with ten customers.
        try (ConsoleCapture printed = new ConsoleCapture();
             LogCapture logged = LogCapture.of(FluentQuickstartApp.class, Level.INFO)) {
            // tag::quickstartSandbox[]
            sandbox = Sandbox.builder()
                    .perSecond(50)                                   // <1>
                    .bound(Bound.after(Duration.ofSeconds(10)))      // <2>
                    .keyCardinality(5_000)                           // <3>
                    .feeding(ORDERS_TOPIC, FluentQuickstartApp::anOrder)   // <4>
                    .feeding(SCANS_TOPIC, FluentQuickstartApp::aScan)
                    .build();

            instance = pc.start(sandbox);                              // <5>
            instance.awaitShutdown();                                  // <6>
            // end::quickstartSandbox[]

            // OUTSIDE the tagged region deliberately: awaitShutdown() is what the README shows a demo doing, and
            // it returns whatever the run did. Asking the bound is the TEST's step, and it is the only one that
            // can fail - sandbox.awaitBound is the sole route to RecordDriver#rethrowAnyFailure, and the
            // bound's own callback wraps its wait in try/finally { instance.close() }, so a wait that REFUSED still
            // closes the instance, awaitShutdown() still returns normally, and the recorded failure never leaves the
            // log. A run that failed to account for its records would otherwise be read as a completed one - which
            // is astubbs#504's own defect class leaving its own primary success signal green. The bound has
            // already been reached by the time awaitShutdown returns, so this is a verdict rather than a wait.
            assertWithMessage("the ten-second bound should have been reached and its close completed, with the "
                    + "driver recording no failure")
                    .that(sandbox.awaitBound(Duration.ofSeconds(60))).isTrue();

            assertConsole(logged, printed);
        }

        // Everything the run left behind, read after the close: the bound closes the instance drain first, so what
        // is here is the end of the run rather than the middle of it.
        assertGenerated(sandbox);
        assertOutcomes();
        assertParked(instance, app);
    }

    /**
     * About five hundred records into each of the definition's two topics, and into no others: the generator reads
     * the definition, so a topic the definition does not route is a topic nothing is generated for.
     */
    private static void assertGenerated(Sandbox sandbox) {
        assertThat(sandbox.consumer().publishedCounts().keySet()).containsExactly(
                new TopicPartition(ORDERS_TOPIC, 0),
                new TopicPartition(SCANS_TOPIC, 0));
        long orders = sandbox.consumer().publishedCounts()
                .get(new TopicPartition(ORDERS_TOPIC, 0));
        assertWithMessage("about %s orders should have been generated in %s at %s/s, but %s were",
                EXPECTED_PER_TOPIC, RUN, RATE_PER_SECOND, orders)
                .that(orders).isAtLeast(LOWEST_CREDIBLE_PER_TOPIC);
    }

    /**
     * Every outcome the README's first screen names, counted: the orders route succeeded and filtered, the scans
     * route parked, and nothing on the orders route parked.
     */
    private void assertOutcomes() {
        assertWithMessage("orders should have been processed, but the outcome counters at close were %s",
                outcomesAtClose).that(outcome(ORDERS_TOPIC, "succeeded")).isGreaterThan(0.0);
        assertWithMessage("the filtered outcome is one of the README's callouts, so a run in which it never fires "
                + "leaves that line of the example unproven; the generator fills the status field from a known set "
                + "of parcel statuses, one of which is RETURNED. Counters at close: %s", outcomesAtClose)
                .that(outcome(ORDERS_TOPIC, "filtered")).isGreaterThan(0.0);
        // Absent and zero are the same reading here - outcome() defaults a missing counter to 0.0, and the
        // counters are created on first use, so a route that never parks has no parked counter at all. What
        // keeps this from passing over a broken meter sweep or a renamed tag is the pair of assertions around
        // it: the orders tag is proven live above, and the parked tag is proven live below.
        assertWithMessage("a healthy route parks nothing")
                .that(outcome(ORDERS_TOPIC, "parked")).isEqualTo(0.0);
        assertWithMessage("every scan's downstream is down, so every scan that ran out of attempts parked")
                .that(outcome(SCANS_TOPIC, "parked")).isGreaterThan(0.0);
    }

    /**
     * The parked set as the README shows an operator reading it: by route from the instance, with everything needed
     * to decide what to do about each record. Driven through the example's own {@code reportParked} as well, so
     * that snippet is executed rather than merely compiled.
     */
    private static void assertParked(ParallelConsumerInstance instance, FluentQuickstartApp app) {
        ParkedView parked = instance.topic(SCANS_TOPIC).parked();
        assertWithMessage("at least one scan should be parked by the end of the run")
                .that(parked.count()).isAtLeast(1);
        assertThat(parked.oldestAge().isPresent()).isTrue();
        assertThat(parked.records()).isNotEmpty();
        parked.records().forEach(record -> {
            assertThat(record.topic()).isEqualTo(SCANS_TOPIC);
            // A retry limit of two allows two attempts after the first, so the function ran three times.
            assertThat(record.attempts()).isEqualTo(3);
            assertThat(record.failure()).isInstanceOf(IllegalStateException.class);
        });
        assertWithMessage("the scans route is the only one that parks in this run, so the all-topics view holds "
                + "exactly what the one-topic view does - isAtLeast would have been true by construction")
                .that(instance.parkedAllTopics().count()).isEqualTo(parked.count());

        app.reportParked(instance);
    }

    /**
     * The console output AE24 asks about: the orders route printing what it reserved, and the park observer saying
     * which record gave up and after how many attempts.
     */
    private static void assertConsole(LogCapture logged, ConsoleCapture printed) {
        assertThat(logged.messagesAt(Level.INFO, "Reserving stock for order")).isNotEmpty();
        String console = printed.text();
        assertWithMessage("the orders route prints what it processed, which is one of the README's callouts, so a "
                + "run that never printed it leaves that line unproven. Console:\n%s", console)
                .that(console).contains("Processed order ");
        assertWithMessage("the park observer prints the record that gave up and after how many attempts, which is "
                + "the callout the park section rests on. Console:\n%s", console)
                .that(console).contains("Parked scan at offset ");
    }

    /**
     * The example's console sink prints with {@code System.out} rather than through a logger, deliberately - a first
     * example that needs a configured logger before it prints anything is not a first example - so the two printed
     * callouts are asserted off the console rather than off a log appender.
     * <p>
     * It tees rather than swallows: a run that fails still shows its output where a reader of the build log expects
     * it. Restoring the original stream is the close, so a failing assertion cannot leave the capture installed.
     */
    private static final class ConsoleCapture implements AutoCloseable {

        private final PrintStream original = System.out;

        private final ByteArrayOutputStream captured = new ByteArrayOutputStream();

        private final PrintStream tee;

        ConsoleCapture() throws UnsupportedEncodingException {
            tee = new PrintStream(new OutputStream() {
                @Override
                public void write(int b) {
                    original.write(b);
                    captured.write(b);
                }
            }, true, "UTF-8");
            System.setOut(tee);
        }

        String text() {
            tee.flush();
            return new String(captured.toByteArray(), StandardCharsets.UTF_8);
        }

        @Override
        public void close() {
            System.setOut(original);
        }
    }

    private void recordOutcomeCountersAsTheyAreSweptAway() {
        registry.config().onMeterRemoved(meter -> {
            Meter.Id id = meter.getId();
            if (meter instanceof Counter && OUTCOME_COUNTER.equals(id.getName())) {
                outcomesAtClose.put(outcomeKey(id.getTag("topic"), id.getTag("outcome")),
                        ((Counter) meter).count());
            }
        });
    }

    private double outcome(String topic, String outcome) {
        return outcomesAtClose.getOrDefault(outcomeKey(topic, outcome), 0.0);
    }

    private static String outcomeKey(String topic, String outcome) {
        return topic + ' ' + outcome;
    }

    /**
     * AE13, read off the source rather than trusted: the region the README includes is one screen.
     * <p>
     * Counting the file would measure the javadoc and the stand-in clients around it, which a reader of the README
     * never sees. Counting the region measures what the objective is actually about - whether the definition fits
     * on a screen - so it goes red when someone grows the example, not when someone documents it.
     */
    @Test
    void theQuickstartDefinitionFitsOnOneScreen() throws IOException {
        List<String> region = taggedRegion(QUICKSTART_SOURCE, "quickstart");
        assertWithMessage("the quickstart definition is %s lines, over the %s-line one-screen budget:\n%s",
                region.size(), ONE_SCREEN_BUDGET, String.join("\n", region))
                .that(region.size()).isAtMost(ONE_SCREEN_BUDGET);
        // A budget an empty region passes would be a green test over a deleted example.
        assertWithMessage("the tagged region should hold the whole definition")
                .that(region.size()).isAtLeast(10);
    }

    /**
     * The lines between {@code // tag::<name>[]} and {@code // end::<name>[]}, exclusive - exactly what the
     * asciidoc template plugin puts in the README.
     */
    private static List<String> taggedRegion(Path source, String tag) throws IOException {
        assertWithMessage("the quickstart source should be readable from the module directory, at %s",
                source.toAbsolutePath()).that(Files.exists(source)).isTrue();
        List<String> lines = Files.readAllLines(source, StandardCharsets.UTF_8);
        int start = indexOfLineContaining(lines, "tag::" + tag + "[]");
        int end = indexOfLineContaining(lines, "end::" + tag + "[]");
        assertWithMessage("both the tag and its end should be present for %s", tag).that(start).isLessThan(end);
        return lines.subList(start + 1, end);
    }

    private static int indexOfLineContaining(List<String> lines, String marker) {
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).contains(marker)) {
                return i;
            }
        }
        throw new AssertionError("no line in the quickstart source contains " + marker);
    }
}
