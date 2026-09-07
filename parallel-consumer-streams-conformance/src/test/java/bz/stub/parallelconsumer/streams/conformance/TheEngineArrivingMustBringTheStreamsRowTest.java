package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The guard on this rung's missing cell: the wrapper's Streams engine is not a binding here, and it must become one
 * the moment there is an engine for a binding to reach (R9, KTD6, AE3).
 * <p>
 * <b>An absent cell is the failure shape this whole module exists to refuse, so it is not left to a comment.</b> A
 * class reaches this module's test classpath only through a dependency the module declares, and this module declares
 * none on the wrapper - so the binding cannot be written without first declaring that dependency, and a stub in its
 * place would turn "the binding agrees with the oracle" into a statement about a mock.
 *
 * <h2>The assertion is an equality in both directions, which is what makes it self-retiring</h2>
 *
 * Today both halves are false, and that is a pass. Put the wrapper on this module's classpath without registering a
 * row and it goes red naming what to do; register a row without the wrapper and it goes red the other way. Nobody
 * has to remember, and nothing about it needs deleting by hand - it stops being interesting the day both halves are
 * true, and the reviewer of that change decides whether it has anything left to say.
 *
 * <h2>Why the guard logic is a function and not just the test body</h2>
 *
 * {@link #guard(List, String)} takes the registry and the class name as arguments, so AE3's three cells can each be
 * driven with a registry and a class name of their own. The real, no-argument test then runs the identical function
 * on the real registry and the real constant. Written the other way round - the assertion inlined in one test - the
 * two failing directions could only be proven by editing the registry and running the suite by hand, which is
 * exactly the kind of proof that gets claimed rather than performed.
 *
 * @see BindingRows#WRAPPER_ASSEMBLER
 */
class TheEngineArrivingMustBringTheStreamsRowTest {

    /**
     * A class name nothing will ever put on a classpath, for the cells that need the probe to answer "absent". Not
     * {@link BindingRows#WRAPPER_ASSEMBLER}: that one becomes present the day the driver rung lands, and a cell
     * asserting the absent case against it would then invert its own meaning without anybody noticing.
     */
    private static final String NEVER_PRESENT = "bz.stub.parallelconsumer.streams.conformance.NoSuchClassEverX";

    /** A class that IS on every classpath there is - the other half of the probe's contract. */
    private static final String ALWAYS_PRESENT = "java.lang.String";

    // ------------------------------------------------------------------------------- the guard, for real

    /**
     * The guard itself, on the real registry and the real constant. Green today because the wrapper is not on this
     * module's classpath and no row is registered - and it is the run of this cell, not the three below, that will
     * turn red when either half moves.
     */
    @Test
    void theStreamsRowIsRegisteredExactlyWhenThereIsAWrapperForItToDrive() {
        guard(BindingRows.registered(), BindingRows.WRAPPER_ASSEMBLER);
    }

    // ---------------------------------------------------------------------------------- AE3's three cells

    /** AE3, first half: no wrapper on the classpath and no row registered - the state this rung is actually in. */
    @Test
    void withNoWrapperAndNoRowTheGuardPasses() {
        guard(Collections.emptyList(), NEVER_PRESENT);
    }

    /**
     * AE3, second half: a row written before the dependency that would make it real. The row must be NAMED, or a
     * maintainer reading the failure cannot tell which of the two halves moved.
     */
    @Test
    void aRowRegisteredWithoutTheWrapperFailsNamingTheRow() {
        AssertionError thrown = assertThrows(AssertionError.class,
                () -> guard(Collections.singletonList(new BindingRows.Row("phantom")), NEVER_PRESENT));

        assertWithMessage("the failure names the registered row, or the reader cannot tell which half moved")
                .that(thrown).hasMessageThat().contains("phantom");
        assertWithMessage("and names the class it probed for, since that is the other half of the equality")
                .that(thrown).hasMessageThat().contains(NEVER_PRESENT);
        assertWithMessage("and says the class is NOT there, so the direction of the disagreement is unambiguous")
                .that(thrown).hasMessageThat().contains("NOT on");
    }

    /**
     * AE3, third half: the wrapper has arrived and nobody registered a row. Driven by pointing the guard at a class
     * that really is present, which is the only way to reach this direction without the wrapper existing.
     */
    @Test
    void theWrapperArrivingWithoutARowFailsTheOtherWay() {
        AssertionError thrown = assertThrows(AssertionError.class,
                () -> guard(Collections.emptyList(), ALWAYS_PRESENT));

        assertWithMessage("the failure names the class that turned up")
                .that(thrown).hasMessageThat().contains(ALWAYS_PRESENT);
        assertWithMessage("and says it IS on the classpath, so the direction is unambiguous")
                .that(thrown).hasMessageThat().contains(" is ON ");
        assertWithMessage("and says what to do about it - a guard that only reports a mismatch makes the reader "
                + "reconstruct the intent")
                .that(thrown).hasMessageThat().contains("register");
    }

    // ------------------------------------------------------------------------------------------ the probe

    /** The probe answers true for a class that is there. Half of what makes an equality assertion meaningful. */
    @Test
    void theProbeFindsAClassThatIsOnTheClasspath() {
        assertWithMessage("a probe that could never answer true would make the guard's equality vacuous")
                .that(onClasspath(ALWAYS_PRESENT)).isTrue();
    }

    /** And false for one that is not - without initialising anything, and without throwing. */
    @Test
    void theProbeReadsAnAbsentClassAsAbsenceRatherThanThrowing() {
        assertWithMessage("an absent class is an answer, not an error - a guard that threw here would fail the "
                + "run for the state this rung is deliberately in")
                .that(onClasspath(NEVER_PRESENT)).isFalse();
    }

    // ------------------------------------------------------------------------------------- the guard logic

    /**
     * The guard, as a pure function of a registry and a class name: a row is registered exactly when the class is
     * reachable.
     *
     * @param registry  the binding rows to hold against the classpath
     * @param className the wrapper class whose presence means the engine has arrived
     * @throws AssertionError if the two halves disagree - the message names both and says what to do
     */
    static void guard(List<BindingRows.Row> registry, String className) {
        boolean wrapperReachable = onClasspath(className);
        boolean rowRegistered = !registry.isEmpty();

        assertWithMessage("the Streams binding row is deferred only for as long as there is no wrapper to drive. "
                        + "%s is %s this module's test classpath, and the registry holds %s. If the wrapper has "
                        + "arrived, register a row in BindingRows and drive the corpus through it beside the "
                        + "oracle; if the row arrived first, either declare the wrapper module as a test "
                        + "dependency or take the row out - a row that quietly cannot run reads exactly like a "
                        + "row that passed. If the wrapper was RENAMED, the constant to correct is "
                        + "BindingRows.WRAPPER_ASSEMBLER, and this guard is what noticed.",
                className,
                wrapperReachable ? "ON" : "NOT on",
                registry.isEmpty() ? "no rows" : "rows " + names(registry))
                .that(rowRegistered).isEqualTo(wrapperReachable);
    }

    /**
     * Loads a class by name <em>without initialising it</em>, and reads every failure to reach it as absence.
     * <p>
     * {@code initialize = false} because running a foreign class's static initialisers to answer "is it there" is a
     * side effect nobody asked for. A {@link LinkageError} counts as absence for the same reason - a class present
     * but unlinkable (its own dependencies missing, a version skew) cannot be driven either, so treating it as
     * arrival would register a row against something that cannot run. {@link NoClassDefFoundError} is a
     * {@code LinkageError} and so is covered by the same catch; Java forbids naming both in one multi-catch, which
     * is why only the supertype appears.
     */
    static boolean onClasspath(String className) {
        try {
            Class<?> found = Class.forName(className, false,
                    TheEngineArrivingMustBringTheStreamsRowTest.class.getClassLoader());
            // Named rather than discarded: the class object is not wanted, only the fact that loading it succeeded.
            return found != null;
        } catch (ClassNotFoundException | LinkageError absent) {
            return false;
        }
    }

    private static String names(List<BindingRows.Row> registry) {
        StringBuilder rendered = new StringBuilder();
        for (BindingRows.Row row : registry) {
            if (rendered.length() > 0) {
                rendered.append(", ");
            }
            rendered.append(row.name());
        }
        return "[" + rendered + "]";
    }
}
