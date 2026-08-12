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
 * Guards the hand-written constructors on the exception classes that used to carry Lombok's
 * {@code @StandardException}. They were converted to hand-written constructors to remove a flaky
 * annotation-processing compile race (see {@link InternalRuntimeException} for the full explanation), and
 * then trimmed to only the {@code Throwable}-shaped constructors each class actually needs (to keep the near
 * -identical boilerplate below the duplication-detector threshold).
 * <p>
 * Because the set of constructors now varies per class, this test exercises whichever of the standard
 * {@code (String)}, {@code (String, Throwable)} and {@code (Throwable)} constructors a class exposes and
 * asserts the message/cause actually propagate to {@link Throwable}. It requires each class to expose at
 * least one of them, so a class can never end up with no usable constructor. A future revert to
 * {@code @StandardException} that revives the race is caught at compile time (call sites stop resolving),
 * not here.
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
    void exposedStandardConstructorsPropagate(Class<? extends Throwable> type) throws Exception {
        var cause = new IllegalStateException("root");
        int exercised = 0;

        // Some classes expose only a no-arg constructor (e.g. KafkaStreamsEncodingNotSupported, whose no-arg
        // ctor supplies a fixed message). Just confirm it constructs.
        var noArgCtor = ctorOrNull(type);
        if (noArgCtor != null) {
            noArgCtor.newInstance();
            exercised++;
        }

        var messageCtor = ctorOrNull(type, String.class);
        if (messageCtor != null) {
            assertThat(messageCtor.newInstance("boom").getMessage()).isEqualTo("boom");
            exercised++;
        }

        var messageAndCauseCtor = ctorOrNull(type, String.class, Throwable.class);
        if (messageAndCauseCtor != null) {
            var e = messageAndCauseCtor.newInstance("boom", cause);
            assertThat(e.getMessage()).isEqualTo("boom");
            assertThat(e.getCause()).isSameInstanceAs(cause);
            exercised++;
        }

        var causeCtor = ctorOrNull(type, Throwable.class);
        if (causeCtor != null) {
            assertThat(causeCtor.newInstance(cause).getCause()).isSameInstanceAs(cause);
            exercised++;
        }

        assertThat(exercised).isGreaterThan(0);
    }

    private static java.lang.reflect.Constructor<? extends Throwable> ctorOrNull(Class<? extends Throwable> type,
                                                                                 Class<?>... paramTypes) {
        try {
            return type.getConstructor(paramTypes);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }
}
