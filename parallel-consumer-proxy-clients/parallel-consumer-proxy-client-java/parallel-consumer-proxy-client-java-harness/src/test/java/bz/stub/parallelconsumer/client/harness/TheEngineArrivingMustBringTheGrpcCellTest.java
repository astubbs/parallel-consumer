package bz.stub.parallelconsumer.client.harness;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The guard on the transport-parameterised suite's missing cell: the shared {@code SpikeConformanceTest} runs
 * under the direct transport and does not run under gRPC, and that must stop being true the moment the
 * sidecar hosts an engine.
 * <p>
 * <b>An absent cell reads exactly like a cell that passed, which is why this is a test rather than a
 * comment.</b> The gRPC subclass on {@code feats/proxy-requirements} builds its fixture from the harness's
 * engine lane - a real gRPC server with the configure handler and the dispatch engine behind it - and none
 * of that exists here: the sidecar this module depends on answers every session {@code UNIMPLEMENTED}
 * (astubbs/parallel-consumer#384). It is left out rather than disabled because a {@code @Disabled} test still
 * has to COMPILE, and the classes its fixture names are not on any classpath on this branch.
 * <p>
 * <b>This module is the right place for the guard because it is the one JVM module that can see the
 * sidecar</b> - that is what it exists for. The equality runs both ways, so it goes red whether the engine
 * arrives without the cell or the cell is written without an engine, and neither requires anyone to remember.
 *
 * @author Antony Stubbs
 */
class TheEngineArrivingMustBringTheGrpcCellTest {

    /** The dispatch engine the gRPC fixture would drive, behind the sidecar this module already depends on. */
    private static final String THE_ENGINE = "bz.stub.parallelconsumer.proxy.engine.ProxyProcessor";

    /** The subclass that binds the shared suite to the gRPC transport, absent for as long as the engine is. */
    private static final String THE_GRPC_CELL = "bz.stub.parallelconsumer.client.harness.GrpcSpikeConformanceTest";

    @Test
    void theGrpcConformanceCellExistsExactlyWhenTheSidecarHasAnEngine() {
        boolean engineReachable = onClasspath(THE_ENGINE);
        boolean cellExists = onClasspath(THE_GRPC_CELL);

        assertWithMessage("the gRPC conformance cell is deferred only for as long as the sidecar hosts no "
                        + "engine. %s is %s the classpath and %s is %s - if the engine has arrived, add the "
                        + "subclass and run the shared suite over the wire; the whole point of the suite is "
                        + "that the same scenarios hold for both transports, and one transport quietly not "
                        + "answering them is indistinguishable from one that did",
                THE_ENGINE, engineReachable ? "ON" : "NOT on", THE_GRPC_CELL, cellExists ? "present" : "absent")
                .that(cellExists).isEqualTo(engineReachable);
    }

    private static boolean onClasspath(String className) {
        try {
            Class.forName(className, false, TheEngineArrivingMustBringTheGrpcCellTest.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError absent) {
            return false;
        }
    }
}
