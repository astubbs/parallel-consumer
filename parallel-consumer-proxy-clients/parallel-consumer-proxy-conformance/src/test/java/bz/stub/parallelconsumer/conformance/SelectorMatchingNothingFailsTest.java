package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The negative control on the selector: a name nobody registered must FAIL, never select nothing.
 * <p>
 * <b>This is the most dangerous shape in the whole suite.</b> CI runs one language per matrix row, so every
 * conformance run there is passes through {@code -Dpc.conformance.language=<name>}; a misspelling that
 * selected zero bindings would evaluate zero scenarios, take two seconds, and report the row green - a
 * client nobody tested, indistinguishable from a client that passed. The repository has seven recorded
 * instances of checks that reported success without ever having run, and a per-row selector is the cheapest
 * way yet invented to add an eighth.
 * <p>
 * The selection is asserted through {@link ConformanceBindings#select}, the pure function, rather than by
 * setting the system property: a JVM-wide property set by one test is read by every scenario running
 * concurrently beside it.
 *
 * @author Antony Stubbs
 */
class SelectorMatchingNothingFailsTest {

    @Test
    void aMisspelledLanguageFailsRatherThanSelectingNothing() {
        var thrown = assertThrows(IllegalArgumentException.class, () -> ConformanceBindings.select("pyhton"));

        assertWithMessage("the failure must quote what was asked for, or a typo is a puzzle")
                .that(thrown).hasMessageThat().contains("pyhton");
        assertWithMessage("and must list what IS registered, so the fix is in the message")
                .that(thrown).hasMessageThat().contains("python");
        assertWithMessage("and must say why selecting nothing is not the alternative")
                .that(thrown).hasMessageThat().contains("read as a pass");
    }

    @Test
    void oneGoodNameBesideOneBadStillFails() {
        // The half-right selector is the realistic mistake: a row that renames one language and leaves a
        // second behind would otherwise run the surviving one and report the whole row green.
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> ConformanceBindings.select("python,rusty"));

        assertWithMessage("the failure names the unmatched half").that(thrown).hasMessageThat().contains("rusty");
    }

    @Test
    void anEmptySelectorMeansEverything() {
        var everything = ConformanceBindings.select(null).size();

        assertWithMessage("an absent selector drives every registered binding, and the blank one must not "
                + "mean something different - a blank -D value is what an unset CI variable expands to")
                .that(ConformanceBindings.select("  ")).hasSize(everything);
        assertWithMessage("every registered binding - the JVM clients and the spawned languages - plus the "
                + "core control arm")
                .that(everything).isEqualTo(JvmClientBindings.all().size() + LanguageRunners.all().size() + 1);
    }

    @Test
    void aJvmClientIsSelectableByNameLikeAnyOtherBinding() {
        var names = ConformanceBindings.select("java-direct").stream().map(ConformanceBinding::name).toList();

        assertWithMessage("a binding whose wire is a function call is selected the same way as one whose "
                + "wire is a process - a client excused from the selector is a client excused from the suite")
                .that(names).containsExactly(CoreBinding.NAME, "java-direct").inOrder();
    }

    @Test
    void theControlArmIsInEverySelection() {
        var names = ConformanceBindings.select("go").stream().map(ConformanceBinding::name).toList();

        assertWithMessage("selecting one language still runs the engine beside it: a scenario that fails "
                + "against core is a WRONG SCENARIO, and that answer is worthless if it arrives in a "
                + "different CI job from the client that went red")
                .that(names).containsExactly(CoreBinding.NAME, "go").inOrder();
    }

    @Test
    void theControlArmCanBeSelectedOnItsOwn() {
        var names = ConformanceBindings.select(CoreBinding.NAME).stream().map(ConformanceBinding::name).toList();

        assertWithMessage("a row that wants only the control arm - no toolchain to install - names it")
                .that(names).containsExactly(CoreBinding.NAME);
    }
}
