package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The demo's dials are the interface ten other languages copy, so the precedence rule between flags,
 * environment and defaults is asserted rather than assumed.
 *
 * @author Antony Stubbs
 */
class DemoOptionsTest {

    private static final Map<String, String> NO_ENV = Collections.emptyMap();

    private static final String[] NO_ARGS = {};

    /**
     * The double-click case, and the one this branch has already been bitten by once: a previous
     * runner aborted given no arguments at all, which is exactly how a first-time reader starts it.
     */
    @Test
    void noArgumentsAtAllIsAValidRun() {
        var options = DemoOptions.parse(NO_ARGS, NO_ENV);

        assertThat(options.records()).isEqualTo(2_000);
        assertThat(options.delayMs()).isEqualTo(2);
        assertThat(options.maxConcurrency()).isEqualTo(100);
        assertThat(options.partitions()).isEqualTo(10);
        assertThat(options.replayFactor()).isEqualTo(20);
        assertThat(options.bootstrap()).isEmpty();
        assertThat(options.topic()).isEmpty();
    }

    @Test
    void flagsOverrideTheDefaults() {
        var options = DemoOptions.parse(new String[]{
                "--records", "50", "--delay-ms", "7", "--concurrency", "8",
                "--partitions", "3", "--replay-factor", "2",
                "--bootstrap", "broker:9092", "--topic", "chosen"}, NO_ENV);

        assertThat(options.records()).isEqualTo(50);
        assertThat(options.delayMs()).isEqualTo(7);
        assertThat(options.maxConcurrency()).isEqualTo(8);
        assertThat(options.partitions()).isEqualTo(3);
        assertThat(options.replayFactor()).isEqualTo(2);
        assertThat(options.bootstrap()).contains("broker:9092");
        assertThat(options.topic()).contains("chosen");
    }

    @Test
    void theEnvironmentOverridesTheDefaults() {
        var env = new HashMap<String, String>();
        env.put("PC_DEMO_RECORDS", "77");
        env.put("PC_DEMO_BOOTSTRAP", "sibling:9092");

        var options = DemoOptions.parse(NO_ARGS, env);

        assertThat(options.records()).isEqualTo(77);
        assertThat(options.bootstrap()).contains("sibling:9092");
        // untouched dials keep their defaults
        assertThat(options.delayMs()).isEqualTo(2);
    }

    /**
     * The precedence that matters in practice: compose passes the environment, and a person passing
     * a flag on top of it must win.
     */
    @Test
    void aFlagBeatsTheEnvironment() {
        var env = new HashMap<String, String>();
        env.put("PC_DEMO_RECORDS", "77");

        var options = DemoOptions.parse(new String[]{"--records", "5"}, env);

        assertThat(options.records()).isEqualTo(5);
    }

    @Test
    void blankEnvironmentValuesAreIgnoredRatherThanParsed() {
        var env = new HashMap<String, String>();
        // compose substitutes an unset variable as the empty string, which must not read as zero
        env.put("PC_DEMO_ARGS_UNUSED", "");
        env.put("PC_DEMO_RECORDS", "   ");

        assertThat(DemoOptions.parse(NO_ARGS, env).records()).isEqualTo(2_000);
    }

    /**
     * run.sh is not the only way in - `docker compose run demo --help` reaches the Java entry point
     * directly, where an unrecognised flag would otherwise be an error rather than an answer.
     */
    @Test
    void helpIsRecognisedInEitherSpelling() {
        assertThat(DemoOptions.isHelpRequested(new String[]{"--help"})).isTrue();
        assertThat(DemoOptions.isHelpRequested(new String[]{"-h"})).isTrue();
        assertThat(DemoOptions.isHelpRequested(new String[]{"--records", "5", "--help"})).isTrue();
        assertThat(DemoOptions.isHelpRequested(NO_ARGS)).isFalse();
        assertThat(DemoOptions.isHelpRequested(new String[]{"--records", "5"})).isFalse();
    }

    @Test
    void aMisspelledFlagIsRefusedRatherThanIgnored() {
        assertThatThrownBy(() -> DemoOptions.parse(new String[]{"--recrods", "5"}, NO_ENV))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--recrods");
    }

    @Test
    void aFlagWithNoValueIsRefused() {
        assertThatThrownBy(() -> DemoOptions.parse(new String[]{"--records"}, NO_ENV))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs a value");
    }

    @Test
    void aValueThatIsNotANumberIsRefused() {
        assertThatThrownBy(() -> DemoOptions.parse(new String[]{"--records", "lots"}, NO_ENV))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("whole number");
    }

    @Test
    void aConcurrencyBelowOneIsRefused() {
        assertThatThrownBy(() -> DemoOptions.parse(new String[]{"--concurrency", "0"}, NO_ENV))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1");
    }

    @Test
    void aNegativeDelayIsRefusedButZeroIsNot() {
        assertThatThrownBy(() -> DemoOptions.parse(new String[]{"--delay-ms", "-1"}, NO_ENV))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");

        assertThat(DemoOptions.parse(new String[]{"--delay-ms", "0"}, NO_ENV).delayMs()).isZero();
    }

    @Test
    void aReplayFactorOfOneOrLessSkipsTheBigReplay() {
        assertThat(DemoOptions.parse(new String[]{"--replay-factor", "1"}, NO_ENV).bigReplayWanted())
                .isFalse();
        assertThat(DemoOptions.parse(new String[]{"--replay-factor", "0"}, NO_ENV).bigReplayWanted())
                .isFalse();
        assertThat(DemoOptions.parse(new String[]{"--replay-factor", "2"}, NO_ENV).bigReplayWanted())
                .isTrue();
    }

    /**
     * The product is an int, so a large pair silently wraps - and a wrapped value would still print
     * a confident throughput figure for a run that never happened.
     */
    @Test
    void aBigReplayTooLargeToCountIsRefusedRatherThanWrapped() {
        assertThatThrownBy(() -> DemoOptions.parse(
                new String[]{"--records", "2000000", "--replay-factor", "2000"}, NO_ENV))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more records than the demo can count");
    }

    @Test
    void theBigReplayIsAMultipleOfTheSmallOne() {
        var options = DemoOptions.parse(new String[]{"--records", "100", "--replay-factor", "20"}, NO_ENV);

        assertThat(options.bigReplayRecords()).isEqualTo(2_000);
    }

    /**
     * Own-cluster mode puts a user's real broker address in here, and the credential-hygiene rule
     * that binds the proxy binds a demo too. The effective-configuration line is printed on every
     * run, so it is the one place an address would leak by accident.
     */
    @Test
    void theEffectiveConfigurationNeverEchoesTheBrokerAddress() {
        var options = DemoOptions.parse(
                new String[]{"--bootstrap", "secret-broker.internal:9092"}, NO_ENV);

        assertThat(options.toString()).doesNotContain("secret-broker.internal");
    }
}
