package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The guard on this rung's one missing cell: {@code java-grpc} is not a binding here, and it must become one
 * the moment there is an engine for it to reach.
 * <p>
 * <b>An absent cell is the failure shape this suite exists to refuse, so it is not left to a comment.</b> The
 * gRPC transport talks to the sidecar, and the sidecar on this stack hosts no engine - it answers every
 * session {@code UNIMPLEMENTED} (astubbs/parallel-consumer#384) - so the binding cannot be written without
 * writing the engine, and a stub in its place would turn "every binding agrees" into a statement about a
 * mock. It is left out rather than disabled: a {@code @Disabled} test still has to COMPILE, and the classes
 * its fixture needs (the harness's engine lane, the configure handler, the dispatch engine) do not exist on
 * this branch at all.
 * <p>
 * <b>The assertion is an equality in both directions, which is what makes it self-retiring.</b> Today both
 * halves are false. Put the engine on this module's classpath without registering the binding and it goes
 * red naming what to do; register the binding without an engine and it goes red the other way. Nobody has to
 * remember, and nothing about it needs deleting by hand - it stops being interesting the day both halves are
 * true, and the reviewer of that change is the one who decides whether it has anything left to say.
 *
 * @author Antony Stubbs
 * @see JvmClientBindings
 */
class TheEngineArrivingMustBringTheGrpcBindingTest {

    /**
     * The dispatch engine the gRPC binding would need behind the sidecar. Named as a string rather than
     * imported for the obvious reason - importing it is exactly what this module cannot do today - and it is
     * the engine rather than the transport because {@code parallel-consumer-proxy} is already a real module
     * here, hosting no engine.
     */
    private static final String THE_ENGINE = "bz.stub.parallelconsumer.proxy.engine.ProxyProcessor";

    @Test
    void theGrpcBindingIsRegisteredExactlyWhenThereIsAnEngineForItToReach() {
        boolean engineReachable = onClasspath(THE_ENGINE);
        boolean grpcRegistered = JvmClientBindings.all().stream()
                .anyMatch(binding -> JvmClientBindings.JAVA_GRPC.equals(binding.name()));

        assertWithMessage("the java-grpc cell is deferred only for as long as there is no engine. %s is %s "
                        + "this module's classpath and the java-grpc binding is %sregistered - if the engine "
                        + "has arrived, add the binding to JvmClientBindings and drive it through the same "
                        + "scenarios; a cell that quietly does not run reads exactly like a cell that passed",
                THE_ENGINE, engineReachable ? "ON" : "NOT on", grpcRegistered ? "" : "NOT ")
                .that(grpcRegistered).isEqualTo(engineReachable);
    }

    private static boolean onClasspath(String className) {
        try {
            Class.forName(className, false, TheEngineArrivingMustBringTheGrpcBindingTest.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError absent) {
            return false;
        }
    }
}
