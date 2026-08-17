package bz.stub.parallelconsumer.mutiny;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PCRetriableException;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Does Mutiny need an unwrap step the way Reactor does?
 * <p>
 * {@code ReactorProcessor.onError} calls {@code Exceptions.unwrap} before classifying, because Reactor repackages
 * what it propagates and core cannot name reactor's wrapper types. {@code MutinyProcessor.onError} has no equivalent,
 * and Mutiny ships no {@code unwrap} helper - so the question is whether it needs one, and nothing answered it.
 * <p>
 * Answering it by observation rather than by reading Mutiny's source: push a {@code PCRetriableException} through the
 * same {@code Uni} failure path the engine subscribes to, and look at what actually arrives at the failure consumer.
 * If it arrives unwrapped, the missing step is a non-issue and this test says so permanently. If Mutiny ever starts
 * wrapping, this fails and names the wrapper.
 *
 * @author Antony Stubbs
 */
class MutinyRetriableClassificationTest {

    @Test
    void mutinyHandsTheFailureConsumerTheOriginalThrowable() {
        var retriable = new PCRetriableException("retry me");
        var arrived = new AtomicReference<Throwable>();

        Uni.createFrom().<String>failure(retriable)
                .subscribe().with(ignored -> {
                }, arrived::set);

        assertWithMessage("Mutiny wrapped the failure, so MutinyProcessor.onError needs an unwrap step like Reactor's")
                .that(arrived.get())
                .isSameInstanceAs(retriable);
    }

    /**
     * The shape that actually reaches the engine: PC's own wrapper around the user's retriable. The engine's
     * classifier peels that itself, so this pins the end-to-end answer rather than only the framework's half.
     */
    @Test
    void aRetriableSurvivingMutinysFailurePathIsStillClassifiedExpected() {
        var wrapped = new bz.stub.parallelconsumer.ExceptionInUserFunctionException(
                "Error occurred in code supplied by user", new PCRetriableException("retry me"));
        var arrived = new AtomicReference<Throwable>();

        Uni.createFrom().<String>failure(wrapped)
                .subscribe().with(ignored -> {
                }, arrived::set);

        assertThat(PCRetriableException.isPresentIn(arrived.get())).isTrue();
    }
}
