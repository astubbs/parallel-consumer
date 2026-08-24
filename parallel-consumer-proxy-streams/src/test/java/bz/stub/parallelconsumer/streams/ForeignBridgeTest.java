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
 * The three classes that actually reach the wire, tested directly.
 *
 * <p>They had no tests at all before this. The assembler's tests inject their own mapper and reducer lambdas, so
 * they exercise the topology wiring and never these bridges - which meant sabotaging the aggregate on its way out
 * of {@link ForeignReducer} broke nothing, and the coverage gap was invisible until someone tried.
 *
 * <p>What is asserted here is the one thing the assembler tests structurally cannot see: WHAT reaches the sink.
 * That matters most for the joiner, whose two arguments are both bytes: transposing them still compiles, still
 * runs, and still produces output - so only an assertion on which one landed where can tell the difference.
 */
class ForeignBridgeTest {

    private static final Duration GENEROUS = Duration.ofSeconds(5);

    /** Captures what was emitted and answers immediately, so neither bridge blocks. */
    private static final class CapturingSink implements InvocationSink {
        private final InvocationRegistry registry;
        private final List<String> aggregates = new ArrayList<>();
        private final List<String> values = new ArrayList<>();
        private final List<String> rights = new ArrayList<>();

        private CapturingSink(InvocationRegistry registry) {
            this.registry = registry;
        }

        private final List<String> kinds = new ArrayList<>();

        @Override
        public void emit(long correlation, long functionToken, ForeignCall call) {
            kinds.add(call.kind().name());
            aggregates.add(call.aggregate() == null
                    ? "<absent>" : new String(call.aggregate(), StandardCharsets.UTF_8));
            values.add(call.value() == null ? "<absent>" : new String(call.value(), StandardCharsets.UTF_8));
            rights.add(call.right() == null ? "<absent>" : new String(call.right(), StandardCharsets.UTF_8));
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

    @Test
    void theJoinerSendsTheStreamValueAsTheValueAndTheTableValueAsTheRight() {
        InvocationRegistry registry = new InvocationRegistry();
        CapturingSink sink = new CapturingSink(registry);

        new ForeignJoiner(registry, sink, 7, GENEROUS)
                .apply("event".getBytes(StandardCharsets.UTF_8), "fact".getBytes(StandardCharsets.UTF_8));

        // Both sides are bytes, so a transposition here is invisible to the compiler and to the topology tests.
        // The host writes joiners assuming the stream record comes first; this is the assertion that holds us to it.
        assertThat(sink.values).containsExactly("event");
        assertThat(sink.rights).containsExactly("fact");
        assertThat(sink.aggregates).containsExactly("<absent>");
    }

    @Test
    void aJoinAndAReduceAreDistinguishableOnTheWireDespiteBothCarryingTwoValues() {
        InvocationRegistry registry = new InvocationRegistry();
        CapturingSink sink = new CapturingSink(registry);

        new ForeignReducer(registry, sink, 7, GENEROUS)
                .apply("a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8));
        new ForeignJoiner(registry, sink, 8, GENEROUS)
                .apply("c".getBytes(StandardCharsets.UTF_8), "d".getBytes(StandardCharsets.UTF_8));

        // Field presence alone stopped being enough the moment a third shape arrived: a reducer's pair and a
        // joiner's pair are the same two byte arrays. The kind is what the host dispatches on.
        assertThat(sink.kinds).containsExactly("INVOCATION_KIND_REDUCE", "INVOCATION_KIND_JOIN").inOrder();
    }

    @Test
    void theJoinerReturnsWhatTheHostAnswered() {
        InvocationRegistry registry = new InvocationRegistry();
        CapturingSink sink = new CapturingSink(registry);

        byte[] result = new ForeignJoiner(registry, sink, 7, GENEROUS)
                .apply("a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8));

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("answered");
    }
}
