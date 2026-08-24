package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * The two classes that actually reach the wire, tested directly.
 *
 * <p>They had no tests at all before this. The assembler's tests inject their own mapper and reducer lambdas, so
 * they exercise the topology wiring and never these bridges - which meant sabotaging the aggregate on its way out
 * of {@link ForeignReducer} broke nothing, and the coverage gap was invisible until someone tried.
 *
 * <p>What is asserted here is the one thing the assembler tests structurally cannot see: WHAT reaches the sink.
 */
class ForeignBridgeTest {

    private static final Duration GENEROUS = Duration.ofSeconds(5);

    /** Captures what was emitted and answers immediately, so neither bridge blocks. */
    private static final class CapturingSink implements InvocationSink {
        private final InvocationRegistry registry;
        private final List<String> aggregates = new ArrayList<>();
        private final List<String> values = new ArrayList<>();

        private CapturingSink(InvocationRegistry registry) {
            this.registry = registry;
        }

        @Override
        public void emit(long correlation, long functionToken, byte[] key, byte[] value, byte[] aggregate) {
            aggregates.add(aggregate == null ? "<absent>" : new String(aggregate, StandardCharsets.UTF_8));
            values.add(value == null ? "<absent>" : new String(value, StandardCharsets.UTF_8));
            registry.complete(correlation, "answered".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void theReducerSendsTheStoredAggregateToTheHost() {
        InvocationRegistry registry = new InvocationRegistry();
        CapturingSink sink = new CapturingSink(registry);

        new ForeignReducer(registry, sink, 7, GENEROUS)
                .apply("running".getBytes(StandardCharsets.UTF_8), "next".getBytes(StandardCharsets.UTF_8));

        // The aggregate is the whole point of a reduction. If it does not reach the sink the host cannot combine,
        // and the topology-level tests cannot see this because they supply their own reducer.
        assertThat(sink.aggregates).containsExactly("running");
        assertThat(sink.values).containsExactly("next");
    }

    @Test
    void theMapperSendsNoAggregateAtAll() {
        InvocationRegistry registry = new InvocationRegistry();
        CapturingSink sink = new CapturingSink(registry);

        new ForeignValueMapper(registry, sink, 7, GENEROUS)
                .apply("k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));

        // Absence is the signal that tells the host to map rather than combine. A mapper that sent an empty
        // aggregate would be asking for a reduction against empty bytes, which is a different operation.
        assertThat(sink.aggregates).containsExactly("<absent>");
    }

    @Test
    void theReducerReturnsWhatTheHostAnswered() {
        InvocationRegistry registry = new InvocationRegistry();
        CapturingSink sink = new CapturingSink(registry);

        byte[] result = new ForeignReducer(registry, sink, 7, GENEROUS)
                .apply("a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8));

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("answered");
    }
}
