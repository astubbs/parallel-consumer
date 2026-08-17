package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An ordered list of {@link ScenarioPhase}s, and nothing else. A scenario is a DECLARATION - it names
 * what the run should look like and in what order - and knows nothing about the driver that executes it,
 * which is what lets a scenario be declared entirely outside this package (R30).
 * <p>
 * The Chaos Pain Suite's own W1 and W4 configurations are single-phase scenarios (see
 * {@link ChaosScenarios}), so the chaos suite is one consumer of this framework rather than a special
 * case inside it.
 */
@Getter
@ToString
public class Scenario {

    private final String name;
    private final List<ScenarioPhase> phases;

    @Builder(toBuilder = true)
    public Scenario(String name, @Singular List<ScenarioPhase> phases) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("a scenario needs a name - it goes in the run log");
        }
        if (phases == null || phases.isEmpty()) {
            throw new IllegalArgumentException("scenario '" + name + "' has no phases");
        }
        this.name = name;
        this.phases = Collections.unmodifiableList(new ArrayList<>(phases));
    }

    public static Scenario of(String name, ScenarioPhase... phases) {
        return new Scenario(name, Arrays.asList(phases));
    }

    /** True if any phase runs until stopped rather than for a declared duration. */
    public boolean hasUnboundedPhase() {
        return phases.stream().anyMatch(p -> p.getDuration() == null);
    }
}
