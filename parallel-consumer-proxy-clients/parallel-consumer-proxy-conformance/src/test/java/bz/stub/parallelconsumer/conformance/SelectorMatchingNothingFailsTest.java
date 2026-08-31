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
    void aMisspelledBindingFailsRatherThanSelectingNothing() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> ConformanceBindings.select("java-dircet"));

        assertWithMessage("the failure must quote what was asked for, or a typo is a puzzle")
                .that(thrown).hasMessageThat().contains("java-dircet");
        assertWithMessage("and must list what IS registered, so the fix is in the message")
                .that(thrown).hasMessageThat().contains("java-direct");
        assertWithMessage("and must say why selecting nothing is not the alternative")
                .that(thrown).hasMessageThat().contains("read as a pass");
    }

    @Test
    void oneGoodNameBesideOneBadStillFails() {
        // The half-right selector is the realistic mistake: a row that renames one binding and leaves a
        // second behind would otherwise run the surviving one and report the whole row green.
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> ConformanceBindings.select("java-direct,rusty"));

        assertWithMessage("the failure names the unmatched half").that(thrown).hasMessageThat().contains("rusty");
    }

    /**
     * The deferred cell is deferred from the SELECTOR too, which is the half that would otherwise rot
     * silently: a CI row naming a binding nobody registered must fail rather than run the control arm alone
     * and report the row green.
     */
    @Test
    void theDeferredGrpcCellIsNotQuietlySelectable() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> ConformanceBindings.select(JvmClientBindings.JAVA_GRPC));

        assertWithMessage("naming the cell this rung does not have must fail, not select the control arm "
                + "alone and read as a java-grpc run that passed")
                .that(thrown).hasMessageThat().contains(JvmClientBindings.JAVA_GRPC);
    }

    /**
     * A deferred FOREIGN cell fails too - and the failure has to read differently from a typo's, because the
     * reader's next move differs. {@code go} is not a misspelling of anything: the client exists, its runner
     * exists, and what is missing is an engine for the runner to reach. Telling a CI row's operator that
     * {@code go} is unrecognised would send them hunting a spelling mistake that is not there.
     */
    @Test
    void aDeferredLanguageFailsAsDeferredRatherThanAsATypo() {
        for (var runner : LanguageRunners.all()) {
            var thrown = assertThrows(IllegalArgumentException.class,
                    () -> ConformanceBindings.select(runner.language()),
                    () -> runner.language() + " is not registered, so selecting it must fail");

            assertWithMessage("the failure names the language that was asked for")
                    .that(thrown).hasMessageThat().contains(runner.language());
            assertWithMessage("and says it is waiting on the engine rather than misspelled, or the reader "
                    + "goes looking for a typo that is not there")
                    .that(thrown).hasMessageThat().contains("deferred rather than");
        }
    }

    @Test
    void anEmptySelectorMeansEverything() {
        var everything = ConformanceBindings.select(null).size();

        assertWithMessage("an absent selector drives every registered binding, and the blank one must not "
                + "mean something different - a blank -D value is what an unset CI variable expands to")
                .that(ConformanceBindings.select("  ")).hasSize(everything);
        assertWithMessage("every registered binding - the JVM clients, and the spawned languages when they "
                + "arrive - plus the core control arm")
                .that(everything).isEqualTo(JvmClientBindings.all().size() + 1);
    }

    @Test
    void aJvmClientIsSelectableByNameLikeAnyOtherBinding() {
        var names = ConformanceBindings.select(" java-direct ").stream().map(ConformanceBinding::name).toList();

        assertWithMessage("a binding whose wire is a function call is selected the same way as one whose "
                + "wire is a process, whitespace and all - a client excused from the selector is a client "
                + "excused from the suite")
                .that(names).containsExactly(CoreBinding.NAME, "java-direct").inOrder();
    }

    @Test
    void theControlArmIsInEverySelection() {
        var names = ConformanceBindings.select("java-direct").stream().map(ConformanceBinding::name).toList();

        assertWithMessage("selecting one client still runs the engine beside it: a scenario that fails "
                + "against core is a WRONG SCENARIO, and that answer is worthless if it arrives in a "
                + "different CI job from the client that went red")
                .that(names).containsExactly(CoreBinding.NAME, "java-direct").inOrder();
    }

    @Test
    void theControlArmCanBeSelectedOnItsOwn() {
        var names = ConformanceBindings.select(CoreBinding.NAME).stream().map(ConformanceBinding::name).toList();

        assertWithMessage("a row that wants only the control arm - no toolchain to install - names it")
                .that(names).containsExactly(CoreBinding.NAME);
    }
}
