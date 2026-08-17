package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The measurement the ceiling scenario rests on, tested on its own.
 * <p>
 * <b>{@link RunnerTranscript#peakUnresolved()} is where "how many records was this client holding" is
 * decided for every language at once</b>, so a mistake in it would read as eleven conforming clients rather
 * than as one broken sweep. The cases below are the shapes a runner can actually produce: interleaved
 * arrivals and settlements, a redelivery, and a record the prescription never resolves.
 *
 * @author Antony Stubbs
 */
class TranscriptOverlapTest {

    /**
     * The shape {@code hold-until-ceiling-full} produces against a conforming client: two arrivals, then two
     * settlements, three times over. The peak is the group's width and never the record count.
     */
    @Test
    void aFullGroupPeaksAtTheGroupsWidthAndNotAtTheRecordCount() {
        var transcript = of(
                "dispatch key=a offset=0 attempt=1 reason=",
                "dispatch key=b offset=1 attempt=1 reason=",
                "settled key=a offset=0 attempt=1 reason=",
                "settled key=b offset=1 attempt=1 reason=",
                "dispatch key=c offset=2 attempt=1 reason=",
                "dispatch key=d offset=3 attempt=1 reason=",
                "settled key=c offset=2 attempt=1 reason=",
                "settled key=d offset=3 attempt=1 reason=");

        assertThat(transcript.peakUnresolved()).isEqualTo(2);
        assertThat(transcript.dispatches()).hasSize(4);
        assertThat(transcript.settlements()).hasSize(4);
    }

    /**
     * The shape a client that freed a slot it was still holding produces: a third record arrives while two
     * are unresolved. This is the only line between a green ceiling scenario and a red one, so it is the
     * case worth reading twice.
     */
    @Test
    void anExtraArrivalWhileTheGroupIsHeldRaisesThePeak() {
        var transcript = of(
                "dispatch key=a offset=0 attempt=1 reason=",
                "dispatch key=b offset=1 attempt=1 reason=",
                "dispatch key=c offset=2 attempt=1 reason=",
                "settled key=a offset=0 attempt=1 reason=",
                "settled key=b offset=1 attempt=1 reason=",
                "settled key=c offset=2 attempt=1 reason=");

        assertWithMessage("three records were unresolved at once, which is what a ceiling of two forbids")
                .that(transcript.peakUnresolved()).isEqualTo(3);
    }

    /** A redelivery is two windows one after the other, not one window twice as wide. */
    @Test
    void aRedeliveryDoesNotOverlapItself() {
        var transcript = of(
                "dispatch key=a offset=0 attempt=1 reason=",
                "settled key=a offset=0 attempt=1 reason=conformance-prescribed-failure",
                "dispatch key=a offset=0 attempt=2 reason=conformance-prescribed-failure",
                "settled key=a offset=0 attempt=2 reason=");

        assertThat(transcript.peakUnresolved()).isEqualTo(1);
    }

    /**
     * {@code report-nothing} prints no settled line, by prescription - so its record stays open to the end
     * of the transcript, which is exactly what "still out with a worker" should look like here.
     */
    @Test
    void aRecordTheRunnerNeverSettledStaysUnresolved() {
        var transcript = of("dispatch key=a offset=0 attempt=1 reason=");

        assertThat(transcript.peakUnresolved()).isEqualTo(1);
        assertThat(transcript.settlements()).isEmpty();
    }

    /**
     * A settled line with no arrival to close is a runner reporting an outcome for a record it was never
     * handed. Answering zero, or a negative running count, would let that pass as an unusually well-behaved
     * client.
     */
    @Test
    void settlingARecordThatWasNeverDispatchedIsLoud() {
        var transcript = of("settled key=a offset=0 attempt=1 reason=");

        var thrown = assertThrows(IllegalStateException.class, transcript::peakUnresolved);
        assertThat(thrown).hasMessageThat().contains("never");
    }

    /** Lines a runner prints that are neither observation are ignored, as a spawned sidecar's logging is. */
    @Test
    void nonObservationLinesAreIgnored() {
        var transcript = of(
                "some sidecar logging that happens to be on this stream",
                "dispatch key=a offset=0 attempt=1 reason=",
                "settled key=a offset=0 attempt=1 reason=");

        assertThat(transcript.observations()).hasSize(2);
        assertThat(transcript.peakUnresolved()).isEqualTo(1);
    }

    /** Builds a transcript the way {@code ConformanceDriver} does: by parsing a runner's stdout in order. */
    private static RunnerTranscript of(String... lines) {
        var stdout = String.join("\n", lines);
        var observations = List.of(lines).stream()
                .map(DispatchObservation::parse)
                .flatMap(java.util.Optional::stream)
                .toList();
        return new RunnerTranscript("test", "n/a", RunnerContract.EXIT_OK, observations, stdout, "");
    }
}
