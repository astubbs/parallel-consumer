package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Open;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsServiceGrpc;
import org.junit.jupiter.api.Test;

import java.net.URL;

import static com.google.common.truth.Truth.assertThat;

/**
 * The generated protocol classes must appear on the compile path exactly once.
 *
 * <p>The root build adds {@code target/generated-sources} as a test source root in every module, and a source root
 * sweeps its whole subtree - so generating beneath that path compiles every class twice, once as a main source and
 * again into {@code target/test-classes}. The duplicate then precedes {@code target/classes} on the surefire
 * classpath, so tests silently exercise a different copy from the one the jar ships. The protocol module measured
 * this and moved its output; this module inherits the fix and this test is what keeps it.
 */
class GeneratedCodePlacementTest {

    @Test
    void generatedMessagesAreNotAlsoCompiledIntoTestClasses() {
        assertThat(locationOf(Open.class)).doesNotContain("test-classes");
    }

    @Test
    void generatedServiceStubsAreNotAlsoCompiledIntoTestClasses() {
        assertThat(locationOf(StreamsServiceGrpc.class)).doesNotContain("test-classes");
    }

    /**
     * Where the class was actually loaded from. Resolving the class's own resource reports the copy that won on the
     * classpath, which is precisely the thing at risk - asserting the file is absent from a directory would pass
     * while a stale duplicate elsewhere shadowed it.
     */
    private static String locationOf(Class<?> type) {
        URL resource = type.getResource(type.getSimpleName() + ".class");
        assertThat(resource).isNotNull();
        return resource.toString();
    }
}
