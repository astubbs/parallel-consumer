package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.lincheck.LincheckAssertionError;
import org.jetbrains.lincheck.datastructures.ModelCheckingOptions;
import org.jetbrains.lincheck.datastructures.Operation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The controlled experiment behind "Lincheck's MODEL CHECKING strategy cannot be pointed at
 * {@link ShardManager} under KEY ordering". Two arms, identical but for one term.
 * <p>
 * <b>The mechanism.</b> Lincheck's {@code ConstantHashCodeTransformer} rewrites <em>every</em>
 * {@code hashCode()I} call site into {@code Injections.hashCodeDeterministic(receiver)}, so that identity hash
 * codes cannot make a replay diverge. It does not distinguish {@code INVOKESPECIAL} from {@code INVOKEVIRTUAL},
 * and {@code hashCodeDeterministic} dispatches virtually - so {@code super.hashCode()} inside an overriding
 * {@code hashCode} becomes a call to <em>itself</em>. Lombok's
 * {@code @EqualsAndHashCode(callSuper = true)} generates exactly that shape
 * ({@code int result = super.hashCode();}), which is why {@code ShardKey.KeyOrderedKey} - the shard map's key
 * under KEY ordering - cannot be model-checked at all.
 * <p>
 * <b>Why this test exists rather than a sentence in a document.</b> It is the tripwire: the crash arm asserts
 * that Lincheck still has the defect. When a future Lincheck fixes it, THIS test goes red, and that is the
 * signal to re-enable model checking on the shard classes. A note in a plan document would never fire.
 *
 * @author Antony Stubbs
 * @see LincheckHarness#withoutValueTypeAnalysis a guarantee that does NOT help - transformation happens before
 *         analysis sections are consulted, which is itself worth knowing
 */
@Slf4j
@Tag("lincheck")
public class LincheckSuperHashCodeProbeTest {

    /**
     * The one term under test, present in one arm and absent in the other.
     */
    public static class Base {
        @Override
        public int hashCode() {
            return 42;
        }
    }

    /**
     * TEST ARM: {@code hashCode} delegates upwards, the shape Lombok emits for {@code callSuper = true}.
     */
    public static class KeyCallingSuperHashCode extends Base {

        private final Map<KeyCallingSuperHashCode, Integer> map = new ConcurrentHashMap<>();

        @Operation
        public Integer put(int value) {
            return map.put(new KeyCallingSuperHashCode(), value);
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode();
        }
    }

    /**
     * CONTROL ARM: identical in every respect except that {@code hashCode} does not delegate upwards.
     */
    public static class KeyNotCallingSuperHashCode extends Base {

        private final Map<KeyNotCallingSuperHashCode, Integer> map = new ConcurrentHashMap<>();

        @Operation
        public Integer put(int value) {
            return map.put(new KeyNotCallingSuperHashCode(), value);
        }

        @Override
        public int hashCode() {
            return 31 * 42;
        }
    }

    @Test
    void modelCheckingStillCrashesOnSuperHashCode() {
        var error = assertThrows(LincheckAssertionError.class, () -> smallModelCheck().check(KeyCallingSuperHashCode.class),
                "Lincheck 3.7's super.hashCode() recursion appears to be FIXED. Re-enable model checking on "
                        + "ShardManagerLincheckTest and delete this arm.");

        assertThat(error).hasMessageThat().contains("You've caught a bug in Lincheck");
        assertThat(error).hasMessageThat().contains("StackOverflowError");
        log.info("Lincheck model checking still recurses on super.hashCode():\n{}", error.getMessage());
    }

    @Test
    void modelCheckingIsFineWithoutTheSuperCall() {
        // No assertThrows: the control arm must complete normally. If this one ever starts throwing, the
        // finding above is about something other than the super call and the diagnosis needs redoing.
        smallModelCheck().check(KeyNotCallingSuperHashCode.class);
    }

    /**
     * Bounds kept tiny on purpose - neither arm is looking for a concurrency bug, only for whether the
     * instrumentation survives the first invocation.
     */
    private static ModelCheckingOptions smallModelCheck() {
        return new ModelCheckingOptions()
                .threads(2)
                .actorsPerThread(1)
                .actorsBefore(0)
                .actorsAfter(0)
                .iterations(1)
                .invocationsPerIteration(10);
    }
}
