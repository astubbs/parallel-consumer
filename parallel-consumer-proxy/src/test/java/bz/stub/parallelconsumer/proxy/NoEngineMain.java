package bz.stub.parallelconsumer.proxy;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.io.InputStream;
import java.io.PrintStream;

/**
 * A spawnable sidecar that hosts <b>no</b> engine - the same binary lifecycle as {@link Main}, answering
 * every session {@code UNIMPLEMENTED}.
 *
 * <h2>Why this exists rather than {@code Main} simply not having an engine</h2>
 *
 * Eight cross-language {@code SidecarHandshakeTest}s (Kotlin, Scala, Go, Python, TypeScript, Rust, Ruby, C#)
 * spawn a sidecar and assert the refusal arrives <b>as {@code UNIMPLEMENTED} specifically</b> -
 * {@code PERMISSION_DENIED} and {@code RESOURCE_EXHAUSTED} are what the two interceptors raise before the
 * service method runs, so the status code is the assertion rather than "it failed". Each carries a permanent
 * control arm pointed at a dead port, and together they are the only cross-language evidence that the
 * client-side path up to the engine works.
 * <p>
 * When unit U10 gave {@link Main} its engine, those tests would have lost their subject. So the no-engine
 * build moved here instead of disappearing, and each language's sidecar-main constant was re-pointed at this
 * class - the outcome the sidecar-shell rung (astubbs/parallel-consumer#384) predicted when it deferred the
 * engine, rather than a discovery made at merge time. It is in the <b>test</b> tree, and therefore in the
 * proxy module's test jar, which every client's sidecar classpath already includes for
 * {@code TestModeMain} - so nothing about how they spawn changed except the class name.
 *
 * <h2>It is this module's {@code Main}, minus one supplier</h2>
 *
 * Everything a spawning client depends on - the port line, the argument refusal, the admission rules, the
 * parent-death lifecycle, the exit code - is {@link Main}'s, reached through the same package-private seam a
 * test uses. Reimplementing the lifecycle here would be a second copy that could drift from the one the
 * production sidecar actually runs, which is the failure this class is arranged to avoid.
 *
 * @author Antony Stubbs
 * @see NoEngineSessionService
 */
public final class NoEngineMain {

    /** An ephemeral port, exactly as the production entry point asks for. */
    private static final int EPHEMERAL_PORT = 0;

    private NoEngineMain() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err, System.in));
    }

    /**
     * The testable core, mirroring {@link Main#run(String[], PrintStream, PrintStream, InputStream)}: it
     * returns the exit code instead of calling {@code System.exit}, so a test in another module can drive the
     * no-engine sidecar in-process rather than spawning a JVM for it. The Java client's own handshake test is
     * the caller that needs it - it is the one language whose handshake runs in the same JVM.
     */
    public static int run(String[] args, PrintStream out, PrintStream err, InputStream parentLifeline) {
        return Main.run(args, out, err, parentLifeline, NoEngineSessionService::new, EPHEMERAL_PORT);
    }
}
