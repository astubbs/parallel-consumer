package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.github.bsideup.jabel.Desugar;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Everything one runner process produced: its exit status - the verdict channel - its parsed observations,
 * and its raw output, kept so a failure message can show what actually happened rather than only what was
 * expected.
 *
 * @author Antony Stubbs
 */
@Desugar // Jabel requires the annotation on every record, even in this module where release=17 makes it a no-op
public record RunnerTranscript(String language, String commandLine, int exitCode,
                               List<DispatchObservation> dispatches, String stdout, String stderr) {

    /** The observations for one key, in delivery order - the per-shard view the ordering scenarios assert on. */
    public List<DispatchObservation> dispatchesForKey(String key) {
        return dispatches.stream().filter(d -> d.key().equals(key)).collect(Collectors.toList());
    }

    /**
     * A block of diagnostic text for a failure message. Assertions attach this rather than paraphrasing,
     * because the interesting half of a foreign-runner failure is usually on its stderr.
     */
    public String diagnostics() {
        return "\n  runner   : " + language
                + "\n  command  : " + commandLine
                + "\n  exit     : " + exitCode
                + "\n  observed : " + dispatches
                + "\n  --- stdout ---\n" + stdout
                + "\n  --- stderr ---\n" + stderr;
    }
}
