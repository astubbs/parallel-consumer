package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * What a start does about a route naming a topic the cluster does not have.
 *
 * <h2>Why there is a policy here at all</h2>
 * Kafka's consumer sets {@code allow.auto.create.topics} to true by default and this package never sets it, so a
 * misspelled topic name is <b>created</b> by a permissive broker rather than refused: the definition starts, the
 * route is given an empty topic with a partition count nobody chose, and the only symptom is silence. The
 * alternative symptom is not much better - on a broker with auto-creation off, the route is assigned nothing and
 * the instance warns that a routed topic was assigned no partition, which names two possible causes and cannot say
 * which one it is.
 *
 * @see ParallelConsumerDefinition#whenTopicMissing(MissingTopic)
 */
@InterfaceStability.Unstable
public enum MissingTopic {

    /**
     * <b>The default.</b> Refuse the start, naming every topic that is not there.
     * <p>
     * A topic a route names is part of the definition, and a definition that cannot be satisfied is a fault of the
     * same kind as a route with no function - not a condition to wait out. It is the default because the failure it
     * replaces is silent, and because the two states it distinguishes are ones a running instance cannot tell
     * apart: a topic that does not exist and a topic whose every partition is held by another member of the group
     * both leave a route assigned nothing.
     */
    FAIL,

    /**
     * Create what is missing, with the broker's own defaults, and carry on.
     * <p>
     * For a definition that owns its topics - a test fixture, a single-writer pipeline, a first deployment. The
     * shape is deliberately not declared here: the topic is created with neither a partition count nor a
     * replication factor, so the broker applies its {@code num.partitions} and {@code default.replication.factor}.
     * That is the only shape this can honestly pick, because a partition count is a capacity decision about a topic
     * the definition merely reads, and inventing one here would be a number nobody chose - which is exactly the
     * failure mode auto-creation has.
     */
    CREATE,

    /**
     * Say what is missing and start anyway.
     * <p>
     * The behaviour before this setting existed, minus the silence: nothing is refused and nothing is created, and
     * a warning names the topics so that the empty route has an explanation beside it. It is also the only policy
     * under which a cluster that cannot answer the question does not stop the start - asking is best-effort here,
     * where under the other two an unanswerable cluster is a startup fault, because neither refusing nor creating
     * can be decided without an answer.
     */
    IGNORE
}
