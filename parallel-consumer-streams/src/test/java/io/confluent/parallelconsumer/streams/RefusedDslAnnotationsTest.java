package io.confluent.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Layer 1: the refused DSL methods carry {@code @Deprecated} and {@code @DoNotCall}, so a call site is a
 * compile <b>error</b> under ErrorProne and a warning without it.
 * <p>
 * <b>Why the two halves are asserted differently.</b> {@code java.lang.Deprecated} is {@code RUNTIME}-
 * retained, so reflection can enumerate every overload and check each one - which makes that half
 * exhaustive by construction, and it is the half that catches a missed overload after a Kafka upgrade adds
 * one. {@code @DoNotCall} is {@code CLASS}-retained: it is in the class file but invisible to reflection.
 * So it is asserted per class, by looking for its descriptor in the compiled constant pool. Weaker, and
 * stated here rather than quietly skipped.
 * <p>
 * <b>Why the compile error itself is not asserted.</b> Nothing in this repository compiles under ErrorProne,
 * so there is no build in which a call site could be observed failing. The annotation being present on the
 * symbol the compiler resolves is what this test can prove, and it is the whole of layer 1's mechanism.
 * <p>
 * The annotations go on the <b>interfaces</b> rather than the impls because a call site resolves to the
 * symbol its receiver's static type declares - {@code stream.join(...)} against a {@code KStream} variable
 * resolves to {@code KStream#join}, and an annotation on {@code KStreamImpl} would never be consulted.
 *
 * @author Antony Stubbs
 * @see PcUnsupportedConstruct
 */
class RefusedDslAnnotationsTest {

    private static final String DO_NOT_CALL_DESCRIPTOR = "Lcom/google/errorprone/annotations/DoNotCall;";

    /**
     * The refused method names, per interface. Deliberately name-based and coarse: every overload of these
     * names on these types is refused, so a Kafka upgrade that adds a twenty-ninth {@code join} overload
     * fails this test instead of shipping an unguarded hole.
     */
    private static final List<String> KSTREAM_REFUSED = Arrays.asList("join", "leftJoin", "outerJoin");
    private static final List<String> KTABLE_REFUSED = Arrays.asList("join", "leftJoin", "outerJoin", "suppress");
    private static final List<String> WINDOWED_BY = Arrays.asList("windowedBy");

    @Test
    void everyRefusedKStreamMethodIsDeprecated() {
        assertEveryOverloadDeprecated(KStream.class, KSTREAM_REFUSED, 28);
    }

    @Test
    void everyRefusedKTableMethodIsDeprecated() {
        assertEveryOverloadDeprecated(KTable.class, KTABLE_REFUSED, 25);
    }

    @Test
    void everyKGroupedStreamWindowedByIsDeprecated() {
        assertEveryOverloadDeprecated(KGroupedStream.class, WINDOWED_BY, 3);
    }

    @Test
    void everyCogroupedKStreamWindowedByIsDeprecated() {
        assertEveryOverloadDeprecated(CogroupedKStream.class, WINDOWED_BY, 3);
    }

    @Test
    void everyRefusedInterfaceCarriesDoNotCall() throws IOException {
        for (final Class<?> refused : Arrays.asList(
                KStream.class, KTable.class, KGroupedStream.class, CogroupedKStream.class)) {
            assertThat(classFileOf(refused))
                    .as("%s must reference %s, or a call site is only a warning rather than a compile error",
                            refused.getSimpleName(), DO_NOT_CALL_DESCRIPTOR)
                    .contains(DO_NOT_CALL_DESCRIPTOR);
        }
    }

    @Test
    void supportedMethodsOnTheSameInterfacesAreNotDeprecated() {
        // The control. Without it, an indiscriminate pass that deprecated the entire interface would satisfy
        // every assertion above - and would have refused the operators this module exists to support.
        assertNotDeprecated(KStream.class, "mapValues");
        assertNotDeprecated(KStream.class, "filter");
        assertNotDeprecated(KStream.class, "groupByKey");
        assertNotDeprecated(KGroupedStream.class, "count");
        assertNotDeprecated(KGroupedStream.class, "reduce");
        assertNotDeprecated(KTable.class, "toStream");
    }

    private static void assertEveryOverloadDeprecated(final Class<?> type,
                                                      final List<String> refusedNames,
                                                      final int expectedCount) {
        final List<Method> refused = new ArrayList<>();
        for (final Method method : type.getMethods()) {
            if (refusedNames.contains(method.getName())) {
                refused.add(method);
            }
        }

        // A count assertion, so that a rename upstream fails loudly rather than making the loop below vacuous.
        assertThat(refused)
                .as("%s: refused overloads found. If Kafka changed this surface, the patch needs revisiting "
                        + "before this number is edited", type.getSimpleName())
                .hasSize(expectedCount);

        for (final Method method : refused) {
            assertThat(method.isAnnotationPresent(Deprecated.class))
                    .as("%s#%s%s must be @Deprecated", type.getSimpleName(), method.getName(),
                            Arrays.toString(method.getParameterTypes()))
                    .isTrue();
        }
    }

    private static void assertNotDeprecated(final Class<?> type, final String methodName) {
        for (final Method method : type.getMethods()) {
            if (method.getName().equals(methodName)) {
                assertThat(method.isAnnotationPresent(Deprecated.class))
                        .as("%s#%s is a supported operator and must not be refused", type.getSimpleName(), methodName)
                        .isFalse();
            }
        }
    }

    /**
     * The compiled class file as text. Crude, and enough: the descriptor is a UTF8 constant-pool entry, and it
     * can only be there if something in the file references the annotation.
     */
    private static String classFileOf(final Class<?> type) throws IOException {
        final String resource = type.getName().replace('.', '/') + ".class";
        try (InputStream in = type.getClassLoader().getResourceAsStream(resource)) {
            assertThat(in).as("%s must be loadable as a resource", resource).isNotNull();
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
            // ISO_8859_1 keeps every byte addressable rather than collapsing invalid UTF-8
            // sequences, and the descriptor is pure ASCII, so contains() over it is reliable.
            return new String(out.toByteArray(), StandardCharsets.ISO_8859_1);
        }
    }
}
