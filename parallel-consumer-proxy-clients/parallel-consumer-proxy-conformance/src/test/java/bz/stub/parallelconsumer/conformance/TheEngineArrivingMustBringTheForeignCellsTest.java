package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The guard on the ten cells this rung registers and cannot run: no foreign language is a binding here, and
 * every one of them must become one the moment there is an engine for its runner to reach.
 * <p>
 * <b>Ten absent cells are ten chances for a suite to report a clean run for clients nobody tested</b>, which
 * is the exact failure this whole module exists to refuse - so their absence is asserted rather than
 * described. Each language has a client library and a conformance runner in this tree, and
 * {@link LanguageRunners} says where each runner is built; what none of them has is anything to connect to,
 * because the sidecar on this stack hosts no engine and answers every session {@code UNIMPLEMENTED}
 * (astubbs/parallel-consumer#384). A binding could not be written without writing the engine, and a stub
 * behind it would turn "every binding agrees" into a statement about the stub.
 * <p>
 * <b>The assertion is an equality in both directions, which is what makes it self-retiring</b> - the same
 * shape {@link TheEngineArrivingMustBringTheGrpcBindingTest} uses one registry along, and for the same
 * reason. Today both halves are false. Put the engine on this module's classpath without registering the
 * foreign bindings and it goes red naming what to do; register them without an engine and it goes red the
 * other way. Nobody has to remember, and nothing needs deleting by hand.
 * <p>
 * <b>It asserts about EVERY registered language rather than one of them</b>, because the realistic way this
 * rots is not "the engine landed and nobody noticed" but "the engine landed, six languages were wired up,
 * and four were left" - and four silent languages read exactly like four that passed.
 *
 * @author Antony Stubbs
 * @see LanguageRunners
 * @see ConformanceBindings
 */
class TheEngineArrivingMustBringTheForeignCellsTest {

    /**
     * The dispatch engine a spawned runner would need behind the sidecar. Named as a string rather than
     * imported for the obvious reason - importing it is exactly what this module cannot do today - and it is
     * the engine rather than the transport because {@code parallel-consumer-proxy} is already a real module
     * here, hosting no engine.
     */
    private static final String THE_ENGINE = "bz.stub.parallelconsumer.proxy.engine.ProxyProcessor";

    @Test
    void everyRegisteredLanguageIsABindingExactlyWhenThereIsAnEngineForItToReach() {
        boolean engineReachable = onClasspath(THE_ENGINE);

        for (var runner : LanguageRunners.all()) {
            boolean registered = ConformanceBindings.select(null).stream()
                    .anyMatch(binding -> binding.name().equals(runner.language()));

            assertWithMessage("the %s cell is deferred only for as long as there is no engine. %s is %s this "
                            + "module's classpath and the %s binding is %sregistered - if the engine has "
                            + "arrived, register it beside the JVM clients in ConformanceBindings and drive "
                            + "it through the same scenarios; a cell that quietly does not run reads exactly "
                            + "like a cell that passed",
                    runner.language(), THE_ENGINE, engineReachable ? "ON" : "NOT on", runner.language(),
                    registered ? "" : "NOT ")
                    .that(registered).isEqualTo(engineReachable);
        }
    }

    /**
     * The other half of the same duty, and the one an engine-rung author is likelier to forget: a language
     * that is not a binding must not be quietly selectable either, or a CI row naming it runs the control arm
     * alone and reports a green row for a client that was never started.
     */
    @Test
    void everyLanguageThatIsNotABindingIsDeferredRatherThanUnknown() {
        var deferred = ConformanceBindings.deferredUntilTheEngineArrives();
        var registered = ConformanceBindings.select(null).stream().map(ConformanceBinding::name).toList();

        for (var runner : LanguageRunners.all()) {
            assertWithMessage("%s is neither a registered binding nor a deferred name, so naming it on the "
                            + "selector would fail as a typo - which sends the reader hunting a misspelling "
                            + "that is not there", runner.language())
                    .that(registered.contains(runner.language()) || deferred.contains(runner.language()))
                    .isTrue();
        }
    }

    private static boolean onClasspath(String className) {
        try {
            Class.forName(className, false, TheEngineArrivingMustBringTheForeignCellsTest.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError absent) {
            return false;
        }
    }
}
