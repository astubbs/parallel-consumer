package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The negative control on the selector: a name nobody registered must FAIL, never select nothing (R10, AE4, KTD7).
 * <p>
 * <b>This is the most dangerous shape in the module.</b> A CI row that drives one binding passes its name through
 * {@code -Dpc.streams.conformance.binding=<name>}; a misspelling that selected zero rows would execute nothing, take
 * two seconds and report the row green - a binding nobody tested, indistinguishable from a binding that passed. This
 * repository has recorded instances of checks that reported success without ever having run, and a per-row selector
 * is the cheapest way yet invented to add another.
 *
 * <h2>Nothing here sets the property, deliberately</h2>
 *
 * Every assertion goes through {@link BindingRows#select(String, List)}, the pure function, with a registry supplied
 * by the test. A JVM-wide property set by one test is read by every test running beside it, and the thing worth
 * proving is the <em>function</em>, not that {@code System.getProperty} works. {@link BindingRows#fromSystemProperty()}
 * is the one caller that reads the property, and nothing in this module calls it.
 *
 * <h2>Two registries, and both are the point</h2>
 *
 * A hand-built registry proves what selection does once rows exist - the behaviour the driver rung inherits. The real
 * registry, which is empty, proves the shape this rung actually ships: naming <em>anything</em> by name fails, because
 * there is nothing to name.
 */
class SelectorMatchingNothingFailsTest {

    private static final BindingRows.Row JAVA_WRAPPER = new BindingRows.Row("java-wrapper");

    private static final BindingRows.Row RUSTY = new BindingRows.Row("rusty");

    private static final List<BindingRows.Row> TWO_ROWS = Collections.unmodifiableList(
            Arrays.asList(JAVA_WRAPPER, RUSTY));

    // ------------------------------------------------------------------- AE4: a typo cannot read as a pass

    @Test
    void aMisspelledRowFailsRatherThanSelectingNothing() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> BindingRows.select("java-wrappre", TWO_ROWS));

        assertWithMessage("the failure quotes what was asked for, or a typo is a puzzle")
                .that(thrown).hasMessageThat().contains("java-wrappre");
        assertWithMessage("and lists what IS registered, so the fix is in the message")
                .that(thrown).hasMessageThat().contains("java-wrapper");
        assertWithMessage("and names the other registered row too - a partial list is a second puzzle")
                .that(thrown).hasMessageThat().contains("rusty");
        assertWithMessage("and says why selecting nothing is not the alternative")
                .that(thrown).hasMessageThat().contains("read as a pass");
        assertWithMessage("and names the property, since that is where the typo actually is")
                .that(thrown).hasMessageThat().contains(BindingRows.BINDING_PROPERTY);
    }

    /**
     * The half-right selector is the realistic mistake: a row renamed on one side and left behind on the other would
     * otherwise run the surviving row and report the whole thing green.
     */
    @Test
    void oneGoodNameBesideOneBadStillFails() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> BindingRows.select("java-wrapper,rustty", TWO_ROWS));

        assertWithMessage("the failure names the unmatched half, not the one that resolved")
                .that(thrown).hasMessageThat().contains("rustty");
    }

    /** A selector that is punctuation without a name is a typo, and must not quietly mean "everything". */
    @Test
    void aSelectorThatIsAllSeparatorsFails() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> BindingRows.select(" , ", TWO_ROWS));

        assertWithMessage("naming nothing at all is a typo, and reading it as 'everything' would run rows the "
                + "author did not choose")
                .that(thrown).hasMessageThat().contains("names no binding at all");
    }

    // ------------------------------------------------------------------ AE4: nothing requested means everything

    @Test
    void anAbsentSelectorTakesEveryRowPlusTheOracle() {
        assertWithMessage("an absent selector drives every registered row, and the oracle beside them")
                .that(BindingRows.select(null, TWO_ROWS))
                .containsExactly(BindingRows.ORACLE, "java-wrapper", "rusty").inOrder();
    }

    @Test
    void aBlankSelectorMeansTheSameAsAnAbsentOne() {
        // A blank -D value is what an unset CI variable expands to; if blank meant something else, a missing
        // variable would silently change what the run covered.
        assertThat(BindingRows.select("   ", TWO_ROWS)).isEqualTo(BindingRows.select(null, TWO_ROWS));
    }

    @Test
    void theOracleIsInEverySelectionAndCannotBeSelectedAway() {
        assertWithMessage("selecting one row still runs the oracle beside it: a case that fails against the "
                + "oracle is a WRONG CASE, and that answer is worthless if it arrives in a different CI job "
                + "from the binding that went red")
                .that(BindingRows.select("rusty", TWO_ROWS))
                .containsExactly(BindingRows.ORACLE, "rusty").inOrder();
    }

    @Test
    void theOracleCanBeNamedOnItsOwnLikeAnyOtherName() {
        assertWithMessage("a row that wants only the control arm - no foreign toolchain to install - names it, "
                + "and naming it must not be an unknown-name failure")
                .that(BindingRows.select(BindingRows.ORACLE, TWO_ROWS))
                .containsExactly(BindingRows.ORACLE);
    }

    @Test
    void selectionKeepsTheRegistrysOrderRatherThanTheSelectorsOrder() {
        // The registry order is the order a run drives the rows, so a red is read in the same order every time,
        // whatever order a CI row happened to spell the names in.
        assertThat(BindingRows.select("rusty,java-wrapper", TWO_ROWS))
                .containsExactly(BindingRows.ORACLE, "java-wrapper", "rusty").inOrder();
    }

    // -------------------------------------------------------------- the real registry, which is empty today

    @Test
    void withTheRealRegistryNamingAnythingByNameFails() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> BindingRows.select("java-wrapper", BindingRows.registered()));

        assertWithMessage("this rung registers no binding row, so every name is unknown - and it must fail "
                + "rather than run the oracle alone and read as a java-wrapper run that passed")
                .that(thrown).hasMessageThat().contains("java-wrapper");
        assertWithMessage("and the message says the registry holds only the oracle, which is the actual state "
                + "the reader needs")
                .that(thrown).hasMessageThat().contains("no binding row to name yet");
    }

    @Test
    void withTheRealRegistryAnAbsentSelectorSelectsTheOracleAlone() {
        assertWithMessage("the corpus still runs: the oracle is the control arm, and on this rung it is the "
                + "whole selection")
                .that(BindingRows.select(null, BindingRows.registered()))
                .containsExactly(BindingRows.ORACLE);
    }

    /**
     * The property name is fixed, and distinct from the proxy conformance suite's (KTD7). Two registries answering
     * one property would let a name select a row in one suite while selecting nothing in the other.
     */
    @Test
    void thePropertyIsThisSuitesOwnAndNotTheProxySuites() {
        assertThat(BindingRows.BINDING_PROPERTY).isEqualTo("pc.streams.conformance.binding");
        assertWithMessage("sharing pc.conformance.language with the proxy suite would let one CI matrix row "
                + "name a binding that exists in the other suite only")
                .that(BindingRows.BINDING_PROPERTY).isNotEqualTo("pc.conformance.language");
    }
}
