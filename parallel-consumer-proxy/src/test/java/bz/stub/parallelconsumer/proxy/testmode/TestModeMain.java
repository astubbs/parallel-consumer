package bz.stub.parallelconsumer.proxy.testmode;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.harness.HarnessScenario;
import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.function.Consumer;

/**
 * The test-mode sidecar's entry point: boots the proxy with {@code MockConsumer} and {@code MockProducer} in
 * place of real Kafka clients, so a NON-JVM test can spawn this process over the ordinary child-process path
 * (KTD4) and then drive the {@link ProxyHarness} fixture through the SAME gRPC protocol as production. No
 * bridge, no test-only transport: the foreign client exercises the real wire, which is the whole fidelity
 * argument for this artifact's existence.
 * <p>
 * <b>Fixture selection is a flag, deliberately.</b> The {@code --mock} flag (and {@code --scenario} beside it)
 * is a named, recorded exception to R39's config-over-protocol rule - see KTD5 in the plan. Routing fixture
 * selection through the wire would put a test-only field into a frozen schema that ten client authors must
 * implement and none may use in production; a flag on a binary that is not the shipped artifact is the better
 * trade. The same exception covers the mock clients themselves: they replace the Kafka clients the credential
 * map of {@code Configure} would build (R48), and everything else - options, subscription, capabilities -
 * still arrives connect-time over the protocol.
 * <p>
 * <b>This artifact never ships inside a client package.</b> No wheel, crate or gem may contain it - a test-only
 * entry point inside a published client library is an attack surface and a support burden in one. It MAY ship
 * inside a demo container, whose mock mode is exactly what it is for. Structurally, living in the module's test
 * tree keeps it out of the main jar; it travels only in this module's {@code tests}-classifier jar.
 * <p>
 * <b>The spawning contract:</b> the bound ephemeral loopback port is the first line on stdout
 * ({@code port: <n>}); the process then serves until its parent dies, observed as stdin EOF - the simple half
 * of the parent-death watch whose full machinery (grace periods, drain) is the lifecycle unit's (U10). The
 * spawning test connects to the port, sends {@code Configure} naming the scenario as its topic, and the
 * scenario's records are seeded the moment the engine is up - so the first dispatch follows the handshake with
 * nothing else to call.
 *
 * @author Antony Stubbs
 * @see ProxyHarness
 */
public final class TestModeMain {

    /** Argument or usage error: the caller asked for something this binary does not do. */
    public static final int EXIT_USAGE = 2;

    public static final String MOCK_FLAG = "--mock";

    public static final String SCENARIO_FLAG = "--scenario";

    public static final String PORT_LINE_PREFIX = "port: ";

    private TestModeMain() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err, System.in, harness -> {
        }));
    }

    /** The testable core of {@link #main}, with the process's ambient stdin as the parent lifeline. */
    static int run(String[] args, PrintStream out, PrintStream err) {
        return run(args, out, err, System.in, harness -> {
        });
    }

    /**
     * The fully seam-injected form: parses the fixture flags, boots the harness's engine over an ephemeral
     * loopback port, reports the port on stdout line one, and serves until {@code parentLifeline} reaches EOF -
     * returning the process exit code rather than calling {@code System.exit} itself.
     *
     * @param parentLifeline  the parent-death signal: the sidecar serves until this stream ends ({@code
     *                        System.in} in the real process)
     * @param harnessObserver hands an in-JVM test the live harness, for engine-side assertions a foreign test
     *                        cannot make (e.g. the committed offset); the real process passes a no-op
     */
    static int run(String[] args, PrintStream out, PrintStream err, InputStream parentLifeline,
                   Consumer<ProxyHarness> harnessObserver) {
        boolean mock = false;
        HarnessScenario scenario = HarnessScenario.A_PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case MOCK_FLAG -> mock = true;
                case SCENARIO_FLAG -> {
                    if (i + 1 >= args.length) {
                        return usage(err, SCENARIO_FLAG + " needs a scenario name");
                    }
                    var name = args[++i];
                    var resolved = HarnessScenario.byName(name);
                    if (resolved.isEmpty()) {
                        return usage(err, "unknown scenario '" + name + "'");
                    }
                    scenario = resolved.get();
                }
                default -> {
                    return usage(err, "unknown argument '" + args[i] + "'");
                }
            }
        }

        if (!mock) {
            return usage(err, MOCK_FLAG + " is required: the test-mode sidecar only runs mock fixtures");
        }

        try (var harness = new ProxyHarness(scenario)) {
            int port = harness.startEngine();
            out.println(PORT_LINE_PREFIX + port);
            harnessObserver.accept(harness);
            awaitParentDeath(parentLifeline);
            return 0;
        }
    }

    /** Blocks until the lifeline ends. An IOException on it reads the same as EOF: the parent is gone. */
    private static void awaitParentDeath(InputStream parentLifeline) {
        try {
            while (parentLifeline.read() != -1) {
                // the parent has nothing to say; only its death (EOF) matters
            }
        } catch (IOException parentGone) {
            // fall through to exit
        }
    }

    private static int usage(PrintStream err, String problem) {
        err.println("test-mode sidecar: " + problem);
        err.println();
        err.println("usage: TestModeMain " + MOCK_FLAG + " [" + SCENARIO_FLAG + " <name>]");
        err.println();
        err.println("  " + MOCK_FLAG + "             boot with MockConsumer/MockProducer instead of real Kafka clients.");
        err.println("                     Fixture selection by flag is a named exception (KTD5) to the");
        err.println("                     config-over-protocol rule (R39): this binary is not the shipped");
        err.println("                     artifact, and a test-only protocol field would burden every client.");
        err.println("  " + SCENARIO_FLAG + " <name>  which conformance scenario to seed. One of:");
        for (var scenario : HarnessScenario.conformanceScenarios()) {
            err.println("                       " + scenario.name());
        }
        err.println();
        err.println("Prints '" + PORT_LINE_PREFIX + "<n>' on stdout line one, then serves the scenario over gRPC");
        err.println("until stdin reaches EOF (parent death). Connect and send Configure naming the scenario as");
        err.println("the topic.");
        err.println();
        err.println("This entry point must never ship inside a client package (wheel, crate, gem); demo");
        err.println("containers may carry it.");
        return EXIT_USAGE;
    }
}
