package bz.stub.parallelconsumer.dashboard.ui;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.vertx.core.json.JsonObject;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The offsets view model - {@code panels/offsets/model.js} - executed as the browser executes it, and asserted from
 * Java.
 *
 * <h2>What this suite is for</h2>
 * <p>
 * Everything the offsets panel shows is computed here: the spans, the counts, the ordering, and the counterfactual
 * offset a single-threaded consumer would be stopped at. A regression in any of it draws a picture that is wrong
 * while remaining a picture - flat where it should be wide, or off by exactly the amount a {@code double} loses -
 * which no compile step and no screenshot glance would catch. This suite gates, runs in about a second, and needs
 * nothing installed.
 *
 * <h2>The trap it exists to close</h2>
 * <p>
 * Kafka offsets are 64-bit and a JavaScript {@code Number} holds 53 bits exactly. The whole string encoding on the
 * wire, and the whole {@code BigInt} discipline in the page, exist to keep an offset above 2<sup>53</sup> exact. The
 * assertions below therefore pin <strong>digits</strong>, and pin them at values chosen so that the {@code double}
 * answer and the correct answer differ - a test using small offsets would pass against a page that had thrown the
 * precision away.
 *
 * <h2>One thread, deliberately</h2>
 * <p>
 * A GraalVM context may not be entered from a second thread, and this repository runs JUnit methods
 * concurrently by default - so without {@code SAME_THREAD} the suite fails with a thread-access error that
 * says nothing about the page. One context per class, used by that class's methods in turn.
 */
@Execution(ExecutionMode.SAME_THREAD)
class OffsetModelTest {

    private static PageModules page;

    private static Value model;

    /**
     * The row the precision assertions are about. Its committed and sequential markers sit one above
     * {@code Number.MAX_SAFE_INTEGER}, where {@code double} arithmetic starts stepping in twos.
     */
    private static final String HUGE_SEQUENTIAL = PageViews.ABOVE_SAFE_INTEGER;

    /**
     * One ABOVE {@link #HUGE_SEQUENTIAL}, because the committed gauge carries the next offset to consume rather than
     * the last one done - see {@link PageViews#caughtUpPartition}. Fixtures that made this equal to the sequential
     * marker are what let the off-by-one in the ordering invariant and the ready span survive.
     */
    private static final String HUGE_COMMITTED = "9007199254740994";

    private static final String HUGE_COMPLETED = "9007199254741999";

    private static final String HUGE_SEEN = "9007199254742100";

    /**
     * {@code 9007199254741999 - 9007199254740993}, done properly. Done in {@code double}s the same subtraction gives
     * 1008, because both ends round to an even neighbour - so this number failing is the signal that the page has
     * started computing what it shows from the numbers it draws with.
     */
    private static final String HUGE_WON_RECORDS = "1006";

    private static final String HUGE_STOP_OFFSET = "9007199254740994";

    @BeforeAll
    static void loadTheModules() {
        page = PageModules.open();
        model = page.module(PageModules.OFFSET_MODEL);
    }

    @AfterAll
    static void closeTheEngine() {
        if (page != null) {
            page.close();
        }
    }

    @Test
    void everyPartitionInTheViewBecomesOneRowModel() {
        Value models = buildModels(threePartitionView());

        assertThat(models.getArraySize()).isEqualTo(3);
    }

    /**
     * The partition doing the most impressive work - equally, the one whose head-of-line blocking is most severe -
     * has to be at the top, rather than wherever its partition number happens to put it.
     */
    @Test
    void theWidestWonSpanSortsFirstAndTheEmptiestLast() {
        Value models = buildModels(threePartitionView());

        assertThat(labels(models)).containsExactly("orders-0", "orders-1", "payments-7");
    }

    @Test
    void recordsWonPastTheBlockAreExactAboveTwoToTheFiftyThree() {
        Value first = buildModels(threePartitionView()).getArrayElement(0);

        assertThat(page.stringify(first.getMember("wonCount")))
                .as("the won span is computed in BigInt from the offset strings, never from the geometry numbers")
                .isEqualTo(HUGE_WON_RECORDS);
        assertThat(digitsOf(first.getMember("wonCountText").asString()))
                .as("the displayed count is the same number, grouped for reading")
                .isEqualTo(HUGE_WON_RECORDS);
    }

    /**
     * The offset a plain consumer would be stuck at is one above the highest unbroken run of successes - and it is
     * printed to the reader, so it has to be exact. In {@code double}s this comes out as 9007199254740992, which is
     * both wrong and, being even, indistinguishable from a rounding artefact.
     */
    @Test
    void theOffsetASingleThreadedConsumerWouldStopAtIsExact() {
        Value first = buildModels(threePartitionView()).getArrayElement(0);

        assertThat(first.getMember("stopOffsetText").asString()).isEqualTo(HUGE_STOP_OFFSET);
    }

    @Test
    void theExactOffsetsAreCarriedThroughUntouched() {
        Value offsets = buildModels(threePartitionView()).getArrayElement(0).getMember("offsets");

        assertThat(offsets.getMember("committed").asString()).isEqualTo(HUGE_COMMITTED);
        assertThat(offsets.getMember("sequential").asString()).isEqualTo(HUGE_SEQUENTIAL);
        assertThat(offsets.getMember("completed").asString()).isEqualTo(HUGE_COMPLETED);
        assertThat(offsets.getMember("seen").asString()).isEqualTo(HUGE_SEEN);
    }

    /**
     * The hero number on the page. Summed in {@code BigInt}, so a page with one enormous partition and several small
     * ones still adds up.
     */
    @Test
    void theTotalWonAcrossPartitionsIsSummedExactly() {
        Value models = buildModels(threePartitionView());

        Value total = model.getMember("totalWonRecords").execute(models);

        // 1006 from the huge partition, 5 from orders-1, nothing from the caught-up one
        assertThat(page.stringify(total)).isEqualTo("1011");
    }

    /**
     * A partition with every marker on the same offset is completely caught up. It must read as healthy - and it
     * must not send a renderer dividing a zero-width axis into spans.
     */
    @Test
    void aCaughtUpPartitionIsCollapsedRatherThanBroken() {
        Value caughtUp = byLabel(buildModels(threePartitionView()), "payments-7");

        assertThat(caughtUp.getMember("collapsed").asBoolean()).isTrue();
        assertThat(caughtUp.getMember("degenerateAxis").asBoolean()).isTrue();
        assertThat(caughtUp.getMember("hasIncomplete").asBoolean()).isFalse();
        assertThat(page.stringify(caughtUp.getMember("wonCount"))).isEqualTo("0");
    }

    /**
     * Each row is scaled to its own axis, so a partition running millions ahead of its commit cannot squash every
     * informative span on the page into a single pixel.
     */
    @Test
    void positionsAreFractionsOfTheRowsOwnAxis() {
        Value ordinary = byLabel(buildModels(threePartitionView()), "orders-1");
        Value positions = ordinary.getMember("positions");

        // the committed gauge reads 50, so the highest offset actually committed is 49 - which is where the bar
        // starts. sequential 55, completed 60, seen 64: an axis 15 wide.
        assertThat(positions.getMember("axisWidth").asDouble()).isEqualTo(15d);
        assertThat(positions.getMember("committed").asDouble()).isEqualTo(0d);
        assertThat(positions.getMember("sequential").asDouble()).isCloseTo(6d / 15d, within());
        assertThat(positions.getMember("completed").asDouble()).isCloseTo(11d / 15d, within());
        assertThat(positions.getMember("seen").asDouble()).isEqualTo(1d);
    }

    /**
     * The committed gauge is the NEXT offset to consume, so the run of successes waiting to be committed starts AT it,
     * not above it. Reading it as inclusive makes this span one short, and makes it negative - clamped to zero, and so
     * invisible - on a partition whose commit has caught up.
     */
    @Test
    void theReadySpanCountsFromTheCommittedGaugeInclusive() {
        Value ordinary = byLabel(buildModels(threePartitionView()), "orders-1");

        // offsets 50, 51, 52, 53, 54, 55 are done and sitting above the commit point
        assertThat(page.stringify(ordinary.getMember("readyCount"))).isEqualTo("6");
        assertThat(ordinary.getMember("committedInclusiveText").asString())
                .as("the axis is drawn from the highest offset actually committed")
                .isEqualTo("49");
        assertThat(ordinary.getMember("offsets").getMember("committed").asString())
                .as("but the reader is still shown the raw wire value, which is what the broker holds")
                .isEqualTo("50");
    }

    /**
     * The shape a real instance produces once it is caught up - committed one above every other marker - which is
     * exactly the shape every fixture in this suite used to avoid. If the model treats the committed offset as
     * inclusive, this row is not collapsed, its ready span is a phantom, and the "Caught up" sentence, the
     * degenerate-axis branch and the ordering badge are all unreachable or permanently wrong.
     */
    @Test
    void aCommitThatHasCaughtUpReadsAsCaughtUpRatherThanAsAnInversion() {
        Value caughtUp = byLabel(buildModels(threePartitionView()), "payments-7");

        assertThat(caughtUp.getMember("offsets").getMember("committed").asString())
                .as("the fixture must be the shape a real consumer produces, or it proves nothing")
                .isEqualTo("901");
        assertThat(caughtUp.getMember("collapsed").asBoolean()).isTrue();
        assertThat(caughtUp.getMember("degenerateAxis").asBoolean()).isTrue();
        assertThat(page.stringify(caughtUp.getMember("readyCount")))
                .as("nothing is waiting to be committed on a partition whose commit has caught up")
                .isEqualTo("0");
        assertThat(caughtUp.getMember("committedInclusiveText").asString()).isEqualTo("900");
    }

    /**
     * A missing reading and a measured zero are different facts, and the model must not merge them - a null span
     * draws nothing, where a zero span would draw a marker claiming the offset is known.
     */
    @Test
    void anAbsentOffsetIsNotTreatedAsZero() {
        JsonObject partial = PageViews.partition("orders", 4, null, "40", "44", "50", 3L);

        Value only = buildModels(PageViews.view(partial)).getArrayElement(0);

        assertThat(page.stringify(only.getMember("readyCount")))
                .as("no committed offset means the ready span is unknown, not empty")
                .isNull();
        assertThat(page.stringify(only.getMember("wonCount")))
                .as("the won span needs only sequential and completed, both of which are present")
                .isEqualTo("4");
    }

    /**
     * Markers read at different instants can arrive out of order. Drawing a bar backwards would be a worse answer
     * than drawing nothing, so a negative span clamps - and the row says so in its own right through the
     * consistency flag, which the model carries rather than re-deriving.
     */
    @Test
    void aBackwardsSpanClampsToZeroAndTheRowIsFlaggedInconsistent() {
        JsonObject skewed = PageViews.inconsistentlySampledPartition("orders", 9, "100", "140", "120", "150", 2L);

        Value only = buildModels(PageViews.view(skewed)).getArrayElement(0);

        assertThat(page.stringify(only.getMember("wonCount"))).isEqualTo("0");
        assertThat(only.getMember("offsetOrderingConsistent").asBoolean()).isFalse();
    }

    private static JsonObject threePartitionView() {
        return PageViews.view(
                // deliberately not in the order they should come out in.
                // orders-1 has a commit that is BEHIND its processing: the last commit recorded offset 49, so the
                // gauge reads 50, and offsets 50..55 are done and waiting to be committed.
                PageViews.partition("orders", 1, "50", "55", "60", "64", 4L),
                PageViews.caughtUpPartition("payments", 7, "900"),
                PageViews.partition("orders", 0, HUGE_COMMITTED, HUGE_SEQUENTIAL, HUGE_COMPLETED, HUGE_SEEN, 7L));
    }

    private static Value buildModels(JsonObject view) {
        return model.getMember("buildPartitionModels").execute(page.parseJson(view.encode()));
    }

    private static java.util.List<String> labels(Value models) {
        java.util.List<String> labels = new java.util.ArrayList<>();
        for (long i = 0; i < models.getArraySize(); i++) {
            labels.add(models.getArrayElement(i).getMember("label").asString());
        }
        return labels;
    }

    private static Value byLabel(Value models, String label) {
        for (long i = 0; i < models.getArraySize(); i++) {
            Value candidate = models.getArrayElement(i);
            if (label.equals(candidate.getMember("label").asString())) {
                return candidate;
            }
        }
        throw new AssertionError("No row model for " + label + " - the view had " + labels(models));
    }

    /**
     * The digits of a grouped count, with whatever separator the runtime's locale uses removed. The assertion is
     * about precision, not about formatting, and a CI machine in a different locale must not be able to fail it -
     * {@code UiFormattingTest} is where the grouping itself is pinned.
     */
    private static String digitsOf(String formatted) {
        return formatted.replaceAll("[^0-9]", "");
    }

    private static org.assertj.core.data.Offset<Double> within() {
        return org.assertj.core.data.Offset.offset(1e-9);
    }
}
