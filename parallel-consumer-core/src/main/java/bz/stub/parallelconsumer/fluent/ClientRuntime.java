package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Optional;

/**
 * Where a started definition gets its Kafka clients (KTD9).
 * <p>
 * This is the one seam between a definition and the world it runs against. The default implementation builds real
 * clients from the connection properties; the sandbox module implements it over the mock consumer that already ships
 * in this artefact, so <b>the definition does not change between sandbox and broker - only the start call does</b>
 * (R33). Both methods are handed the validated {@link DefinitionView}, which is enough to know which topics to serve
 * and what each route consumes, so a fake can generate records of the right types.
 * <p>
 * Both clients are typed {@code byte[]}: the engine below the facade consumes and produces raw bytes and each route
 * applies its own serialisers (KTD2). A consumer configured for anything else is a definition fault that cannot be
 * detected here, because its type parameters are erased - see {@link ParallelConsumerDefinition#consumer}.
 * <p>
 * Neither method's return value is ever held in a field by the facade: it goes straight into the options builder, so
 * core's raw-client architecture rule stays intact (KTD3).
 */
@InterfaceStability.Unstable
public interface ClientRuntime {

    /**
     * The consumer to run this definition with. Called once, at start, and only when the caller supplied no
     * pre-built consumer.
     * <p>
     * It must not be subscribed or assigned: the engine manages the subscription and refuses one that is not clean.
     */
    Consumer<byte[], byte[]> consumer(DefinitionView definition);

    /**
     * The producer to run this definition with, or empty to let Parallel Consumer build its own from the
     * definition's properties.
     * <p>
     * <b>Called only when {@link DefinitionView#requiresProducer()} is true</b> - a definition with no producing
     * route, no dead-letter destination and a consumer commit mode opens no producer at all (R4).
     * <p>
     * Empty is the better answer wherever a real broker is involved: an instance built from a producer
     * <em>config</em> can rebuild its producer, while one handed a finished producer instance cannot, and so forgoes
     * producer recovery (R1, astubbs#410). A fake, which has no recovery to lose, returns its instance.
     */
    Optional<Producer<byte[], byte[]>> producer(DefinitionView definition);

    /**
     * A short-lived admin client for the one question the facade asks the cluster before it starts: whether the
     * topics this definition's routes name are there (see {@link MissingTopic}). Called once, at start, and the
     * facade closes what it is given - it is built for that question and nothing else holds it.
     * <p>
     * <b>Empty means there is no cluster to ask</b>, and the check is then skipped rather than faked: that is the
     * right answer for a runtime that serves records from memory, where every topic a definition names exists by
     * construction. It is the default for the same reason - a runtime written before this existed has no cluster
     * this method could reach.
     *
     * @param definition the validated definition, the same view the client methods above are handed
     * @return an admin client for this definition's cluster, or empty when this runtime has no cluster
     */
    default Optional<Admin> admin(DefinitionView definition) {
        // A runtime that fabricates records has nothing to describe and nothing to create.
        return Optional.empty();
    }

    /**
     * Called once the instance is running, with the handle its caller is about to be given. Does nothing by
     * default, and a runtime that only builds clients never needs it.
     * <p>
     * It exists because a fake needs a moment that a client factory method cannot give it: <b>after</b> the engine
     * has subscribed, so a mock consumer's partitions can be assigned to a listener that now exists, and with the
     * handle in hand, so a generator with a bound can close the instance when it reaches one. Without it the
     * sandbox would need its own entry point and {@code definition.start(runtime)} would silently run unbounded
     * (R33, KTD9).
     * <p>
     * It runs on the thread that called {@code start}, before that call returns, so an implementation that blocks
     * blocks the caller.
     */
    default void started(ConsumerHandle handle) {
        // Most runtimes hand over clients and take no further part.
    }

    /**
     * The default: a real Kafka consumer built from the definition's connection properties with the raw-bytes
     * deserialisers the facade requires, and no producer instance - so Parallel Consumer builds its own and
     * producer recovery stays available.
     */
    static ClientRuntime kafka() {
        return new KafkaClientRuntime();
    }
}
