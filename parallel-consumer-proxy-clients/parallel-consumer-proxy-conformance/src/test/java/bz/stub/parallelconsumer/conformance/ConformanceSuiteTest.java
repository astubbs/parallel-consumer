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
 * THE SUITE: every registered language, through every wired scenario, asserted identically.
 * <p>
 * One test method, parameterised over the whole matrix, because the alternative - a method per language -
 * is how ten clients end up with ten slightly different definitions of the same scenario. A language
 * appears here by being registered in {@link LanguageRunners}; nothing else about this file changes when
 * the next four arrive.
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
        return LanguageRunners.registered().stream()
                .flatMap(runner -> ConformanceScenarios.all().stream()
                        .map(scenario -> Arguments.of(runner, scenario.name(), scenario)));
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("matrix")
    void conforms(LanguageRunner runner, String scenarioName, ConformanceScenario scenario) {
        ConformanceDriver.drive(runner, scenario);
    }
}
