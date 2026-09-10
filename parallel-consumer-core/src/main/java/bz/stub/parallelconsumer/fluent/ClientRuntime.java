package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

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
     * The default: a real Kafka consumer built from the definition's connection properties with the raw-bytes
     * deserialisers the facade requires, and no producer instance - so Parallel Consumer builds its own and
     * producer recovery stays available.
     */
    static ClientRuntime kafka() {
        return new KafkaClientRuntime();
    }
}
