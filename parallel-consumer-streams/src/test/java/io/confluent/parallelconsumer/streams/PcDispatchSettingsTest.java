package io.confluent.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Precedence, parsing, and the fall-through - the three things that decide whether a task runs PC dispatch.
 * <p>
 * The fall-through case is the one worth reading twice. This module's build sets
 * {@code -Dpc.streams.dispatch.enabled=false} on the surefire execution that runs Apache Kafka's own 188
 * tests, and those tests construct tasks from a {@code StreamsConfig} that says nothing about PC dispatch. If
 * a silent config meant "use the default" instead of "ask {@link PcDispatchSwitch}", that execution would run
 * with the seam <b>on</b> and the module's behaviour-preservation claim would quietly become vacuous - with
 * no test turning red anywhere to say so. {@link #aSilentConfigDefersToTheJvmWideSwitch()} is that guard.
 *
 * @author Antony Stubbs
 * @see PerInstanceDispatchConfigTest for the same precedence proved through two real {@code StreamTask}s
 */
// Mutates the process-wide PcDispatchSwitch and system properties, and this module inherits concurrent JUnit
// execution from core's test jar. Same reasoning as PcTaskDispatcherTest.
@Execution(ExecutionMode.SAME_THREAD)
@Isolated
class PcDispatchSettingsTest {

    private static final int JVM_DEFAULT_POOL_SIZE = 4;

    private static final String[] DISPATCH_PROPERTIES = {
            PcDispatchSwitch.ENABLED_PROPERTY,
            PcDispatchSwitch.POOL_SIZE_PROPERTY,
            PcDispatchSettings.POOL_SIZE_CONFIG,
    };

    private final Map<String, String> propertiesTheForkStartedWith = new HashMap<>();

    /**
     * Establish the unset state rather than assume it, and remember what was there.
     * <p>
     * Not defensive padding: this module's build really does fork a surefire execution with
     * {@code -Dpc.streams.dispatch.enabled=false} set, so "the JVM default" is not the same thing in every
     * fork. A test that asserts on a default it did not itself establish is asserting on its surefire
     * execution, which is not what any assertion here claims.
     */
    @BeforeEach
    void startFromNothingConfigured() {
        for (String property : DISPATCH_PROPERTIES) {
            propertiesTheForkStartedWith.put(property, System.getProperty(property));
            System.clearProperty(property);
        }
        PcDispatchSwitch.resetToDefault();
    }

    /** Put the fork back exactly as it was found, so a sibling class inherits its own conditions and not ours. */
    @AfterEach
    void handTheForkBackAsItWasFound() {
        for (String property : DISPATCH_PROPERTIES) {
            String original = propertiesTheForkStartedWith.get(property);
            if (original == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, original);
            }
        }
        propertiesTheForkStartedWith.clear();
        PcDispatchSwitch.resetToDefault();
    }

    // ---------------------------------------------------------------------------------------------------
    // Precedence
    // ---------------------------------------------------------------------------------------------------

    /**
     * The headline: an instance's own config decides for that instance, and it decides in <em>both</em>
     * directions. Only asserting that a config can turn the seam off would leave the "on" direction resting
     * on the default rather than on the config, and the two are not the same claim.
     */
    @Test
    void theStreamsConfigPropertyOutranksTheJvmWideSwitchInBothDirections() {
        PcDispatchSwitch.resetToDefault();
        assertThat(PcDispatchSwitch.isEnabled())
                .as("precondition: the JVM-wide switch is on, so 'off' below can only have come from the config")
                .isTrue();
        assertThat(PcDispatchSettings.resolve(config(PcDispatchSettings.ENABLED_CONFIG, "false")).isEnabled())
                .as("a config saying 'false' beats a JVM-wide switch saying 'on'")
                .isFalse();

        PcDispatchSwitch.disable();
        assertThat(PcDispatchSettings.resolve(config(PcDispatchSettings.ENABLED_CONFIG, "true")).isEnabled())
                .as("and a config saying 'true' beats a JVM-wide switch saying 'off' - the config wins, it "
                        + "does not merely veto")
                .isTrue();
    }

    /**
     * The guard described in this class's javadoc: silence must reach {@link PcDispatchSwitch}, not the
     * default. Both directions again, because a fall-through that only worked when the switch happened to
     * agree with the default would not be a fall-through at all.
     */
    @Test
    void aSilentConfigDefersToTheJvmWideSwitch() {
        PcDispatchSwitch.disable();
        assertThat(PcDispatchSettings.resolve(new HashMap<>()).isEnabled())
                .as("an empty config must reach the switch - this is what keeps the pom's "
                        + "-Dpc.streams.dispatch.enabled=false effective for Kafka's own 188 tests, which "
                        + "configure nothing")
                .isFalse();
        assertThat(PcDispatchSettings.resolve(null).isEnabled())
                .as("and a null config, which is what the no-config createIfEnabled overload passes")
                .isFalse();

        PcDispatchSwitch.enable(3);
        PcDispatchSettings viaSwitch = PcDispatchSettings.resolve(new HashMap<>());
        assertThat(viaSwitch.isEnabled()).isTrue();
        assertThat(viaSwitch.getPoolSize())
                .as("the pool size falls through the same way")
                .isEqualTo(3);
    }

    @Test
    void withNeitherConfigNorSwitchSetTheSeamIsOnWithTheDefaultPool() {
        PcDispatchSwitch.resetToDefault();

        PcDispatchSettings settings = PcDispatchSettings.resolve(new HashMap<>());

        assertThat(settings.isEnabled())
                .as("depending on this artifact IS the opt-in - a second, hidden opt-in step buys nobody "
                        + "anything")
                .isTrue();
        assertThat(settings.getPoolSize()).isEqualTo(JVM_DEFAULT_POOL_SIZE);
    }

    // ---------------------------------------------------------------------------------------------------
    // Parsing - values arrive as whatever the user put in their Properties or Map
    // ---------------------------------------------------------------------------------------------------

    @Test
    void theEnabledValueIsAcceptedAsABooleanOrAStringInAnyCase() {
        PcDispatchSwitch.resetToDefault();

        assertThat(PcDispatchSettings.resolve(config(PcDispatchSettings.ENABLED_CONFIG, Boolean.FALSE)).isEnabled())
                .as("props.put(key, false) is the natural thing to write, so a Boolean must work")
                .isFalse();
        assertThat(PcDispatchSettings.resolve(config(PcDispatchSettings.ENABLED_CONFIG, "FALSE")).isEnabled())
                .as("and case must not matter")
                .isFalse();
        assertThat(PcDispatchSettings.resolve(config(PcDispatchSettings.ENABLED_CONFIG, " true ")).isEnabled())
                .as("nor surrounding whitespace, which a properties file will happily carry")
                .isTrue();
    }

    /**
     * The single most important failure in this class. The property whose entire job is to turn the seam off
     * is the last place a typo may be read as {@code false}: the run would look like a control arm while
     * being nothing of the kind, and nothing anywhere would say so.
     */
    @Test
    void anEnabledValueThatCannotBeUnderstoodFailsLoudlyRatherThanReadingAsOff() {
        assertThatThrownBy(() -> PcDispatchSettings.resolve(config(PcDispatchSettings.ENABLED_CONFIG, "flase")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PcDispatchSettings.ENABLED_CONFIG)
                .hasMessageContaining("flase")
                .hasMessageContaining("StreamsConfig");
    }

    @Test
    void thePoolSizeIsAcceptedAsANumberOrAString() {
        PcDispatchSwitch.resetToDefault();

        assertThat(PcDispatchSettings.resolve(config(PcDispatchSettings.POOL_SIZE_CONFIG, 7)).getPoolSize())
                .isEqualTo(7);
        assertThat(PcDispatchSettings.resolve(config(PcDispatchSettings.POOL_SIZE_CONFIG, "7")).getPoolSize())
                .isEqualTo(7);
    }

    @Test
    void aPoolSizeThatCannotBeUnderstoodOrCannotWorkFailsLoudly() {
        assertThatThrownBy(() -> PcDispatchSettings.resolve(config(PcDispatchSettings.POOL_SIZE_CONFIG, "eight")))
                .as("silently defaulting would attribute the run's numbers to a configuration that never ran")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PcDispatchSettings.POOL_SIZE_CONFIG);
        assertThatThrownBy(() -> PcDispatchSettings.resolve(config(PcDispatchSettings.POOL_SIZE_CONFIG, 0)))
                .as("a pool of zero would accept records and never run one")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1");
        assertThatThrownBy(() -> PcDispatchSettings.resolve(config(PcDispatchSettings.POOL_SIZE_CONFIG, -1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1");
    }

    // ---------------------------------------------------------------------------------------------------
    // The system properties, which are the published contract and must keep working
    // ---------------------------------------------------------------------------------------------------

    @Test
    void bothPoolSizeSystemPropertySpellingsAreHonouredWithTheDottedOneWinning() {
        System.setProperty(PcDispatchSwitch.POOL_SIZE_PROPERTY, "5");
        PcDispatchSwitch.resetToDefault();
        assertThat(PcDispatchSwitch.getPoolSize())
                .as("the camelCase spelling is documented in the module README, so it stays honoured")
                .isEqualTo(5);

        System.setProperty(PcDispatchSettings.POOL_SIZE_CONFIG, "6");
        PcDispatchSwitch.resetToDefault();
        assertThat(PcDispatchSwitch.getPoolSize())
                .as("but the dot-separated name is the documented one and outranks it")
                .isEqualTo(6);
    }

    @Test
    void aPoolSizeSystemPropertyThatCannotBeParsedFailsRatherThanSilentlyDefaulting() {
        System.setProperty(PcDispatchSettings.POOL_SIZE_CONFIG, "eight");

        assertThatThrownBy(PcDispatchSwitch::resetToDefault)
                .as("Integer.getInteger would have returned 4 here and the run would have been reported as "
                        + "having used a pool of 'eight'")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PcDispatchSettings.POOL_SIZE_CONFIG)
                .hasMessageContaining("System property");
    }

    /**
     * Two settings objects, resolved from two different configs, holding different answers at the same time.
     * The value object carries no shared state, which is the property that makes per-instance dispatch
     * possible at all.
     */
    @Test
    void twoSettingsResolvedFromDifferentConfigsCoexist() {
        PcDispatchSwitch.resetToDefault();

        PcDispatchSettings on = PcDispatchSettings.resolve(config(
                PcDispatchSettings.ENABLED_CONFIG, "true",
                PcDispatchSettings.POOL_SIZE_CONFIG, 2));
        PcDispatchSettings off = PcDispatchSettings.resolve(config(
                PcDispatchSettings.ENABLED_CONFIG, "false"));

        assertThat(on.isEnabled()).isTrue();
        assertThat(on.getPoolSize()).isEqualTo(2);
        assertThat(off.isEnabled()).isFalse();
        assertThat(on.isEnabled())
                .as("and resolving the second must not have disturbed the first")
                .isTrue();
    }

    private static Map<String, Object> config(final String key, final Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private static Map<String, Object> config(final String firstKey, final Object firstValue,
                                              final String secondKey, final Object secondValue) {
        Map<String, Object> map = config(firstKey, firstValue);
        map.put(secondKey, secondValue);
        return map;
    }
}
