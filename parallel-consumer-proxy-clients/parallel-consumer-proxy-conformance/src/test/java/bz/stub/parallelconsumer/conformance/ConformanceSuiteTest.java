package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/**
 * THE SUITE: every selected binding, through every wired scenario, asserted identically.
 * <p>
 * One test method, parameterised over the whole matrix, because the alternative - a method per language -
 * is how ten clients end up with ten slightly different definitions of the same scenario. A language
 * appears here by being registered in {@link LanguageRunners}; nothing else about this file changes when
 * the next one arrives.
 * <p>
 * <b>The engine itself is one of the rows</b> ({@link CoreBinding}), and it is the control arm: a scenario
 * red against a plain Java function is a wrong scenario rather than a broken client. Which rows a given run
 * drives is {@link ConformanceBindings}', including the failure when a selector matches nothing.
 * <p>
 * <b>This is not a replacement for a client's own tests.</b> It answers "does every client behave
 * identically on the protocol"; a client's own suite answers "does this client's idiom betray it" - a
 * blocked transport thread, a swallowed cancellation, a floating promise - which is invisible from out
 * here and has already produced real defects in this fan-out. Both layers are load-bearing.
 *
 * @author Antony Stubbs
 */
@Execution(ExecutionMode.CONCURRENT)
class ConformanceSuiteTest {

    static Stream<Arguments> matrix() {
        return ConformanceBindings.selected().stream()
                .flatMap(binding -> ConformanceScenarios.all().stream()
                        .map(scenario -> Arguments.of(binding, scenario.name(), scenario)));
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("matrix")
    void conforms(ConformanceBinding binding, String scenarioName, ConformanceScenario scenario) {
        ConformanceDriver.drive(binding, scenario);
    }
}
