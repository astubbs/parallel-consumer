package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ExceptionInUserFunctionException;
import bz.stub.parallelconsumer.RecordContext;
import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * The engine itself, as a binding: Parallel Consumer driven by a plain Java function, with no client, no
 * proxy, no wire and no child process between the scenario and the code it is a claim about.
 * <p>
 * <b>This is the suite's control arm, and it is why the arm exists at all.</b> Every other binding puts a
 * client - and usually a protocol and a language runtime - between the scenario and the engine, so a red run
 * has several suspects and the client is always the first one looked at. If a scenario is red HERE, the
 * scenario is wrong - there is nothing else left for it to be - and nobody spends an afternoon debugging an
 * innocent Ruby client over a bad assertion. It also makes this suite the single place "correct" is defined
 * for the product, rather than a client-only harness that happens to share some names with it.
 * <p>
 * <b>It implements the same prescription as a foreign runner, in the same terms</b>, because it implements it
 * with the same code: {@link PrescribedRun} is the runner contract carried out in this JVM, and this class is
 * only the adapter between it and core's own user function. Nothing about the assertions can therefore be
 * written to suit this binding - they cannot tell it from the others.
 *
 * @author Antony Stubbs
 * @see ConformanceBinding
 * @see PrescribedRun
 */
@Slf4j
public final class CoreBinding implements ConformanceBinding {

    /** The name this binding answers to in the matrix and on the selector's command line. */
    public static final String NAME = "core";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Run execute(ProxyHarness harness, ConformanceScenario scenario) {
        var run = new PrescribedRun(NAME, scenario);
        harness.start(record -> process(run, record));
        harness.seed();
        run.awaitPrescribedBehaviour();
        return run;
    }

    @Override
    public String toString() {
        return NAME;
    }

    /**
     * The user function: hand the delivery to the prescription, and say what it decided in core's own
     * vocabulary - a return is success, a throw is a failure whose message is the reason. It is the same
     * shape as every runner's processor, deliberately; a binding that took a shortcut here would be
     * conforming to a contract nobody else implements.
     */
    private static void process(PrescribedRun run, RecordContext<String, String> record) {
        var failure = run.deliver(record.key(), record.offset(), record.getNumberOfFailedAttempts() + 1,
                lastFailureReason(record));
        if (failure.isPresent()) {
            throw new IllegalStateException(failure.get());
        }
    }

    /**
     * The reason text a foreign client would have been handed, derived from the Throwable core kept.
     * <p>
     * <b>Two normalisations, and neither is this binding's invention.</b> Core's
     * {@code getLastFailureReason} returns a NULL Optional before the first failure, because
     * {@code WorkContainer#lastFailureReason} has no initializer; and core wraps whatever the user function
     * threw in an {@link ExceptionInUserFunctionException} whose own message is the fixed "Error occurred in
     * code supplied by user", so the reason the scenario prescribed is the CAUSE's. The engine's serializer
     * meets both and answers them the same way - {@code RecordCodec}'s {@code lastFailureReasonText} owns
     * that rule, and this is the in-process side of the same translation. Skipping the unwrap here would
     * have made the control arm red on a scenario every foreign client passes, which is the control arm
     * failing at its own job.
     */
    private static String lastFailureReason(RecordContext<String, String> record) {
        return Optional.ofNullable(record.getLastFailureReason()).flatMap(reason -> reason)
                .map(CoreBinding::unwrap)
                .map(cause -> cause.getMessage() != null ? cause.getMessage() : cause.toString())
                .orElse("");
    }

    private static Throwable unwrap(Throwable recorded) {
        Throwable cause = recorded;
        while (cause instanceof ExceptionInUserFunctionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
