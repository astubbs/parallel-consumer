package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.RecordContext;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import bz.stub.parallelconsumer.state.WorkContainer;
import ch.qos.logback.classic.Level;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The two engine details park rides on, pinned as control arms so that an engine change fails here rather than
 * turning every parked record into a one-second retry in silence (KTD4, Risks and Dependencies).
 *
 * <h2>What the facade is relying on</h2>
 * <ol>
 *     <li>The retry-delay provider is called <b>synchronously, inside the failure path</b>, on the thread that
 *     threw - which is what makes the wrapper's thread-local intent reach it at all.</li>
 *     <li>A provider that throws, returns null, returns a negative delay, or returns one that cannot be added to
 *     the failure time is <b>replaced by the engine's own default, with a warning and nothing else</b> - so a
 *     facade fault of any of those four shapes is invisible except as records retrying every second.</li>
 * </ol>
 * The second is the dangerous one: it is a safety net for the user's provider and would be a silent trap for the
 * facade's. The facade's provider is now a pure function of the topic - it answers the route's retry delay and
 * nothing else - so it cannot present any of the four shapes; this pins that presenting one would in fact cost the
 * backoff, which is why a park is a state rather than a very long delay.
 * <p>
 * These are deliberately written against {@link WorkContainer} and a test module rather than a running engine:
 * what is under test is the provider contract, and a full instance would add a scheduler to the evidence without
 * adding anything to the claim.
 */
class EngineRetryDelayProviderContractTest {

    private static final Duration ENGINE_DEFAULT = ParallelConsumerOptions.DEFAULT_STATIC_RETRY_DELAY;

    private static WorkContainer<String, String> containerWithProvider(
            Function<RecordContext<String, String>, Duration> provider) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .retryDelayProvider(provider)
                .build();
        var module = new PCModuleTestEnv(options);
        return new WorkContainer<>(0, new ConsumerRecord<>("orders", 0, 5, "key", "value"), module);
    }

    /**
     * The baseline: a provider that behaves gets exactly what it asked for, so the arms below are measured against
     * a mechanism that is known to work.
     */
    @Test
    void aHealthyProviderGetsTheDelayItAskedFor() {
        var container = containerWithProvider(context -> Duration.ofMinutes(7));

        container.onUserFunctionFailure(new FakeRuntimeException("failed"));

        assertThat(container.getDelayUntilRetryDue()).isEqualTo(Duration.ofMinutes(7));
    }

    /**
     * The provider is not how a park is expressed, and this is the arm that says so: a park is a state the throw
     * carries, so it never reaches the provider at all and no arithmetic over a delay can turn it back into a
     * retry (KTD14). {@code WorkContainerHandbackTest} owns the park's own behaviour.
     */
    @Test
    void aParkIsNotExpressedAsADelayAndDoesNotConsultTheProvider() {
        var asked = new java.util.concurrent.atomic.AtomicInteger();
        var container = containerWithProvider(context -> {
            asked.incrementAndGet();
            return Duration.ofMinutes(7);
        });

        container.onUserFunctionFailure(new PCRetriableException("out of attempts").park("it ran out of attempts"));

        assertThat(asked.get()).isEqualTo(0);
        assertThat(container.isParked()).isTrue();
        assertThat(container.isDelayPassed()).isFalse();
    }

    // ------------------------------------------------------------------ the four broken shapes

    @Test
    void aProviderThatThrowsIsReplacedByTheDefaultDelayAndWarns() {
        assertReplacedByDefaultWithWarning(context -> {
            throw new FakeRuntimeException("the provider itself is broken");
        }, "threw");
    }

    @Test
    void aProviderThatReturnsNullIsReplacedByTheDefaultDelayAndWarns() {
        assertReplacedByDefaultWithWarning(context -> null, "returned null");
    }

    @Test
    void aProviderThatReturnsANegativeDelayIsReplacedByTheDefaultDelayAndWarns() {
        assertReplacedByDefaultWithWarning(context -> Duration.ofSeconds(-30), "negative");
    }

    /**
     * The trap that makes {@code ChronoUnit.FOREVER} and its neighbours unusable as a park delay: the engine reads
     * the delay happily and then fails to <em>apply</em> it, so this one is caught one layer further in than the
     * other three and still ends as a one-second retry.
     */
    @Test
    void aProviderThatReturnsAnUnrepresentableDelayIsReplacedByTheDefaultDelayAndWarns() {
        assertReplacedByDefaultWithWarning(context -> ChronoUnitForeverIsNotRepresentable.FOREVER_ISH,
                "cannot be applied");
    }

    private void assertReplacedByDefaultWithWarning(Function<RecordContext<String, String>, Duration> broken,
                                                    String expectedInTheWarning) {
        var container = containerWithProvider(broken);

        try (LogCapture logs = LogCapture.of(WorkContainer.class, Level.WARN)) {
            container.onUserFunctionFailure(new FakeRuntimeException("failed"));

            assertWithMessage("the delay the engine actually applied")
                    .that(container.getDelayUntilRetryDue()).isEqualTo(ENGINE_DEFAULT);
            // The warning is the only signal a user gets, and it is rate limited to once per thirty seconds - so
            // a broken provider costs the backoff and then goes quiet.
            assertThat(logs.messagesAt(Level.WARN, "retryDelayProvider", expectedInTheWarning)).isNotEmpty();
        }
    }

    /**
     * A delay too large for {@code failedAt.plus(delay)} - the shape a "park forever" expressed as a duration
     * reaches for, and the reason a park is a state on the container instead.
     */
    private static final class ChronoUnitForeverIsNotRepresentable {
        static final Duration FOREVER_ISH = Duration.ofDays(Long.MAX_VALUE / 100_000);
    }
}
