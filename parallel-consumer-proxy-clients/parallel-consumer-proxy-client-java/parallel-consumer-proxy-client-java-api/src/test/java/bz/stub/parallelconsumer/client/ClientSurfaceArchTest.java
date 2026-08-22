package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * The rule the direct transport's pom asks for by name: <em>"The Java reference work adds an ArchUnit rule
 * covering the API SURFACE; this ban covers the CLASSPATH"</em> (the {@code ban-transport-dependencies}
 * enforcer execution in {@code parallel-consumer-proxy-client-java-direct/pom.xml}).
 * <p>
 * The two checks are complements, not duplicates, and neither subsumes the other. The enforcer ban reads the
 * dependency <em>tree</em> - it fires when a jar arrives. This reads the <em>bytecode</em> - it fires when a
 * type is referenced, which catches the case the enforcer cannot see at all: a transport or engine type
 * reaching the surface through a dependency that was already legitimately present.
 * <p>
 * Why it matters more here than in an ordinary module: this surface is the one nine other languages mirror.
 * A {@code ByteString} or a {@code ConsumerRecord} on a signature here is not a Java problem, it is a
 * specification problem - the mirroring languages have no such type, so the shape stops being expressible
 * and the fan-out silently diverges. Do not relax these to make a build green; a red here means the leak is
 * real and belongs in a transport module.
 * <p>
 * Deliberately name-based, so it holds without any banned artifact on this module's classpath - the module
 * has no compile dependencies at all, which is exactly the state the rules protect.
 * <p>
 * This module's <em>test</em> conventions are NOT checked by the shared {@code TestConventionRules}, unlike
 * every other module in the repo: that rule library ships in core's test-jar, and this pom forbids a
 * dependency on core in any scope. The gap is recorded in {@code docs/client-static-analysis.md} rather than
 * closed by weakening the pom.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer.client", importOptions = ImportOption.DoNotIncludeTests.class)
class ClientSurfaceArchTest {

    /**
     * No transport type may appear anywhere in the shared surface: not on a signature, not in a field, not in
     * a method body. gRPC and protobuf belong to the grpc transport; the protocol module owns the wire types
     * generated from {@code proxy.proto}.
     */
    @ArchTest
    static final ArchRule the_shared_surface_names_no_transport_type =
            noClasses()
                    .should().dependOnClassesThat().resideInAnyPackage(
                            "io.grpc..",
                            "com.google.protobuf..",
                            "bz.stub.parallelconsumer.proxy..")
                    .because("the shared client API is the surface nine other languages mirror, so a gRPC, "
                            + "protobuf or generated-protocol type here would make the shape inexpressible in "
                            + "languages that have no such type - it belongs in the grpc transport module "
                            + "(complements the ban-transport-dependencies enforcer rule in the direct "
                            + "module's pom, which reads the dependency tree rather than the bytecode)");

    /**
     * No engine type either. The direct transport binds this surface to {@code parallel-consumer-core}
     * in-process, and the grpc transport reaches the same engine over the wire - but the surface between them
     * must describe records and outcomes in its own terms, never in Kafka's or the engine's.
     */
    @ArchTest
    static final ArchRule the_shared_surface_names_no_engine_or_kafka_type =
            noClasses()
                    .should().dependOnClassesThat().resideInAnyPackage(
                            "org.apache.kafka..",
                            "bz.stub.parallelconsumer.internal..",
                            "bz.stub.parallelconsumer.state..",
                            "bz.stub.parallelconsumer.offsets..")
                    .because("a client speaks to Parallel Consumer, it does not embed it: a ConsumerRecord or "
                            + "an engine internal on this surface would tie every mirroring language to the "
                            + "JVM Kafka client, which is the entire problem the language proxy exists to "
                            + "remove");
}
