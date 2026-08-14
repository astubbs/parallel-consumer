package bz.stub.parallelconsumer.proxy.testmode;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.harness.HarnessScenario;
import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;

import java.io.PrintStream;

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
 * trade.
 * <p>
 * <b>This artifact never ships inside a client package.</b> No wheel, crate or gem may contain it - a test-only
 * entry point inside a published client library is an attack surface and a support burden in one. It MAY ship
 * inside a demo container, whose mock mode is exactly what it is for. Structurally, living in the module's test
 * tree keeps it out of the main jar; it travels only in this module's {@code tests}-classifier jar.
 * <p>
 * <b>ENGINE SEAM - STUBBED.</b> The engine this boots does not exist yet: {@link ProxyHarness#startEngine()}
 * throws until U5 (transport), U6 (engine) and U7 (connect-time configuration) land, and this process then
 * exits with {@link #EXIT_ENGINE_PENDING}. Once those units fill the seam, the flow already scaffolded here is:
 * boot the engine on an ephemeral loopback port, report the port to the spawning parent on stdout, seed the
 * selected scenario, serve until the parent dies (stdin EOF - the parent-death watch the engine units' lifecycle
 * work owns), and exit.
 *
 * @author Antony Stubbs
 * @see ProxyHarness
 */
public final class TestModeMain {

    /** Argument or usage error: the caller asked for something this binary does not do. */
    public static final int EXIT_USAGE = 2;

    /** The engine seam is still stubbed - the sidecar cannot serve yet. Removed when U5-U7 land. */
    public static final int EXIT_ENGINE_PENDING = 3;

    public static final String MOCK_FLAG = "--mock";

    public static final String SCENARIO_FLAG = "--scenario";

    private TestModeMain() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /**
     * The testable core of {@link #main}: parses the fixture flags, boots the harness's engine seam, and
     * returns the process exit code rather than calling {@code System.exit} itself.
     */
    static int run(String[] args, PrintStream out, PrintStream err) {
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
            int port = harness.startEngine(); // ENGINE SEAM - throws until U5-U7 land
            // Unreachable until the seam is filled. The contract with the spawning parent, ready for the
            // engine units: the bound ephemeral loopback port is the first line on stdout, then the process
            // serves until the parent dies.
            out.println("port: " + port);
            harness.seed();
            return 0;
        } catch (UnsupportedOperationException enginePending) {
            err.println(enginePending.getMessage());
            return EXIT_ENGINE_PENDING;
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
        err.println("This entry point must never ship inside a client package (wheel, crate, gem); demo");
        err.println("containers may carry it.");
        return EXIT_USAGE;
    }
}
