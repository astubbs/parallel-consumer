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
                               List<DispatchObservation> observations, String stdout, String stderr) {

    /** The deliveries, in arrival order - what every scenario written before overlap mattered asserts on. */
    public List<DispatchObservation> dispatches() {
        return byKind(DispatchObservation.Kind.DISPATCH);
    }

    /** The outcomes the runner decided, in the order it decided them. */
    public List<DispatchObservation> settlements() {
        return byKind(DispatchObservation.Kind.SETTLED);
    }

    /**
     * The largest number of records this client held UNRESOLVED at any one instant - dispatched to it, and
     * not yet reported.
     * <p>
     * <b>Read from the line order, not from a clock.</b> A runner's stdout is one serialized stream, so a
     * dispatch line opens a record's unresolved window and its settled line closes it; sweeping the two in
     * order and keeping the running maximum is the whole computation. That is the number the proxy's
     * {@code max_concurrency} is a bound on, and counting anything narrower - a queue's length, say - is the
     * exact mistake the client-authoring guide's §3 rule 2 exists to forbid.
     *
     * @throws IllegalStateException if a settled line has no dispatch to close, which is a runner reporting
     *                               an outcome for a record it was never handed
     */
    public int peakUnresolved() {
        int peak = 0;
        int outstanding = 0;
        for (var observation : observations) {
            if (observation.kind() == DispatchObservation.Kind.DISPATCH) {
                outstanding++;
                peak = Math.max(peak, outstanding);
            } else {
                outstanding--;
                if (outstanding < 0) {
                    throw new IllegalStateException("the " + language + " runner settled a record it was never "
                            + "dispatched, at offset " + observation.offset() + diagnostics());
                }
            }
        }
        return peak;
    }

    /** The observations for one key, in delivery order - the per-shard view the ordering scenarios assert on. */
    public List<DispatchObservation> dispatchesForKey(String key) {
        return dispatches().stream().filter(d -> d.key().equals(key)).collect(Collectors.toList());
    }

    private List<DispatchObservation> byKind(DispatchObservation.Kind kind) {
        return observations.stream().filter(o -> o.kind() == kind).collect(Collectors.toList());
    }

    /**
     * A block of diagnostic text for a failure message. Assertions attach this rather than paraphrasing,
     * because the interesting half of a foreign-runner failure is usually on its stderr.
     */
    public String diagnostics() {
        return "\n  runner   : " + language
                + "\n  command  : " + commandLine
                + "\n  exit     : " + exitCode
                + "\n  observed : " + observations
                + "\n  --- stdout ---\n" + stdout
                + "\n  --- stderr ---\n" + stderr;
    }
}
