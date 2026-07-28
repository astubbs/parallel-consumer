package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ExceptionInUserFunctionException;
import io.confluent.parallelconsumer.PCRetriableException;
import io.confluent.parallelconsumer.ParallelConsumerException;
import io.confluent.parallelconsumer.offsets.BitSetEncodingNotSupportedException;
import io.confluent.parallelconsumer.offsets.EncodingNotSupportedException;
import io.confluent.parallelconsumer.offsets.KafkaStreamsEncodingNotSupported;
import io.confluent.parallelconsumer.offsets.NoEncodingPossibleException;
import io.confluent.parallelconsumer.offsets.OffsetDecodingError;
import io.confluent.parallelconsumer.offsets.RunLengthV1EncodingNotSupported;
import io.confluent.parallelconsumer.offsets.RunLengthV2EncodingNotSupported;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.google.common.truth.Truth.assertThat;

/**
 * Guards the hand-written constructors on every exception class that used to carry Lombok's
 * {@code @StandardException}. Those were converted to hand-written constructors to remove a flaky
 * annotation-processing compile race (see {@link InternalRuntimeException} for the full explanation). This
 * test reflectively exercises the three standard {@code Throwable}-shaped constructors on each class, so a
 * future refactor that drops one - or reintroduces {@code @StandardException} alongside a custom
 * constructor and revives the race - fails here rather than flaking in CI.
 *
 * @author Antony Stubbs
 */
class ExceptionConstructorsTest {

    @ParameterizedTest
    @ValueSource(classes = {
            PCRetriableException.class,
            ParallelConsumerException.class,
            ExceptionInUserFunctionException.class,
            InternalRuntimeException.class,
            InternalException.class,
            EncodingNotSupportedException.class,
            BitSetEncodingNotSupportedException.class,
            RunLengthV1EncodingNotSupported.class,
            RunLengthV2EncodingNotSupported.class,
            NoEncodingPossibleException.class,
            OffsetDecodingError.class,
            KafkaStreamsEncodingNotSupported.class,
    })
    void standardConstructorsExistAndPropagate(Class<? extends Throwable> type) throws Exception {
        var messageCtor = type.getConstructor(String.class);
        var messageAndCauseCtor = type.getConstructor(String.class, Throwable.class);
        var causeCtor = type.getConstructor(Throwable.class);

        var cause = new IllegalStateException("root");

        var byMessage = messageCtor.newInstance("boom");
        assertThat(byMessage.getMessage()).isEqualTo("boom");

        var byMessageAndCause = messageAndCauseCtor.newInstance("boom", cause);
        assertThat(byMessageAndCause.getMessage()).isEqualTo("boom");
        assertThat(byMessageAndCause.getCause()).isSameInstanceAs(cause);

        var byCause = causeCtor.newInstance(cause);
        assertThat(byCause.getCause()).isSameInstanceAs(cause);
    }
}
