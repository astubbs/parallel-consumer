package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the encapsulation and thread-visibility of the fields {@link io.confluent.parallelconsumer.PCHealth} is built
 * from.
 * <p>
 * Reflection rather than ArchUnit: the shared convention suite is declared with
 * {@code ImportOption.OnlyIncludeTests}, so a rule about a production class's modifiers would evaluate against an empty
 * set and pass vacuously.
 *
 * @author Antony Stubbs
 */
class ControllerStateEncapsulationTest {

    /**
     * Lombok's bare {@code @Setter} generates a public setter. On the run state that would let any caller holding the
     * concrete type drive the instance to {@link io.confluent.parallelconsumer.State#CLOSED}, which is not something a
     * library user should be able to do - least of all now that the state is readable as public API.
     */
    @Test
    void setStateIsNotPubliclyAccessible() throws NoSuchMethodException {
        Method setState = AbstractParallelEoSStreamProcessor.class
                .getDeclaredMethod("setState", io.confluent.parallelconsumer.State.class);

        int modifiers = setState.getModifiers();
        // protected is not good enough: ParallelEoSStreamProcessor is a public subclass in another package, so a
        // protected setter would be inherited straight onto the user-facing type.
        assertThat(Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers))
                .as("AbstractParallelEoSStreamProcessor#setState must be package-private - see @Setter(AccessLevel.PACKAGE)")
                .isFalse();
    }

    /**
     * A health check reads these from a thread that never wrote them. Without {@code volatile} there is no
     * happens-before edge, so the reader can observe a stale value - specifically a fresh {@code RUNNING} beside a
     * stale null failure cause, which reports a crashed instance as healthy.
     */
    @Test
    void fieldsReadByHealthChecksArePublishedSafely() throws NoSuchFieldException {
        assertFieldIsVolatile(AbstractParallelEoSStreamProcessor.class, "state");
        assertFieldIsVolatile(AbstractParallelEoSStreamProcessor.class, "failureReason");
        assertFieldIsVolatile(BrokerPollSystem.class, "runState");
    }

    private static void assertFieldIsVolatile(Class<?> owner, String fieldName) throws NoSuchFieldException {
        Field field = owner.getDeclaredField(fieldName);
        assertThat(Modifier.isVolatile(field.getModifiers()))
                .as("%s#%s is read cross-thread by the health check and must be volatile", owner.getSimpleName(), fieldName)
                .isTrue();
    }
}
