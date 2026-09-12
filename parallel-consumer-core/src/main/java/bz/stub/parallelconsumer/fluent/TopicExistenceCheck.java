package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Asks the cluster, once at start, whether the topics this definition's routes name are there - and applies the
 * definition's {@link MissingTopic} policy to the answer.
 * <p>
 * It runs before any client of the instance itself is built, so a refusal arrives as a fault of the definition
 * rather than as a consumer that joined a group and then sat idle.
 *
 * <h2>Where the AdminClient comes from, and why this is temporary</h2>
 * From {@link ClientRuntime#admin(DefinitionView)}, alongside the consumer and the producer, because that is the
 * one seam this package has between a definition and the world it runs against - so a runtime with no cluster
 * behind it (the sandbox, a test over mock clients) declines and the check is skipped rather than faked. The engine
 * does not construct clients from configuration yet; when it does, this construction moves there with the rest.
 *
 * <h2>Missing, and unanswerable, are not the same thing</h2>
 * A describe that fails with {@link UnknownTopicOrPartitionException} is an answer: the topic is not there. A
 * describe that fails any other way - no route to the broker, no authorisation to describe - is the cluster
 * declining to answer, and treating that as "missing" would refuse a healthy definition under {@link
 * MissingTopic#FAIL} and create topics on a half-reachable cluster under {@link MissingTopic#CREATE}. So it is
 * raised as its own startup fault under both, and swallowed under {@link MissingTopic#IGNORE}, which is the policy
 * that says this question may not stop a start.
 */
@Slf4j
final class TopicExistenceCheck {

    private TopicExistenceCheck() {
        // Static: it holds nothing between calls, and there is exactly one call per started definition.
    }

    /**
     * Applies the policy to this definition's topics.
     *
     * @param policy     what to do about a topic that is not there
     * @param topics     every topic the routes name - the same set the instance is about to subscribe to
     * @param runtime    where the AdminClient comes from; one that declines skips the check entirely
     * @param definition the validated definition, handed to the runtime as the other client calls hand it
     * @throws MissingTopicsException under {@link MissingTopic#FAIL}, naming every topic that is not there
     */
    static void enforce(MissingTopic policy, Set<String> topics, ClientRuntime runtime, DefinitionView definition) {
        if (topics.isEmpty()) {
            return;
        }
        Optional<Admin> admin = runtime.admin(definition);
        if (!admin.isPresent()) {
            log.debug("This runtime supplies no AdminClient, so the {} topic-existence policy is not applied to {}",
                    policy, topics);
            return;
        }
        try (Admin client = admin.get()) {
            apply(policy, topics, client);
        }
    }

    /**
     * The check itself, over a client this method does not own - {@link #enforce} closes it, because it was built
     * for this one question.
     */
    private static void apply(MissingTopic policy, Set<String> topics, Admin client) {
        Set<String> missing;
        try {
            missing = describeAndCollectMissing(topics, client);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while asking the cluster which of " + topics + " exist",
                    interrupted);
        } catch (RuntimeException clusterDeclinedToAnswer) {
            if (policy == MissingTopic.IGNORE) {
                log.warn("Could not ask the cluster whether {} exist, and the topic-existence policy is {}, so the "
                        + "start carries on without an answer", topics, policy, clusterDeclinedToAnswer);
                return;
            }
            throw clusterDeclinedToAnswer;
        }
        if (missing.isEmpty()) {
            log.debug("Every routed topic exists: {}", topics);
            return;
        }
        switch (policy) {
            case FAIL:
                throw new MissingTopicsException(msg("These topics are named by this definition's routes and do not "
                        + "exist on the cluster: {}. Create them, or declare withMissingTopicPolicy({}) to have this "
                        + "start create them, or withMissingTopicPolicy({}) to start anyway. A start that carried on "
                        + "would leave those routes processing nothing.",
                        missing, MissingTopic.CREATE, MissingTopic.IGNORE), missing);
            case CREATE:
                create(missing, client);
                return;
            case IGNORE:
                log.warn("These topics are named by this definition's routes and do not exist on the cluster: {}. "
                        + "The topic-existence policy is {}, so the instance starts and those routes process "
                        + "nothing until the topics appear.", missing, policy);
                return;
            default:
                throw new IllegalStateException("Unhandled topic-existence policy " + policy);
        }
    }

    /**
     * One describe for every routed topic, read per topic because that is where the answer is: the whole call does
     * not fail because one name is unknown, each name's own future does.
     *
     * @return the topics the cluster does not have, in the order the routes named them
     */
    private static Set<String> describeAndCollectMissing(Set<String> topics, Admin client)
            throws InterruptedException {
        Map<String, KafkaFuture<?>> described = new java.util.LinkedHashMap<>(
                client.describeTopics(new ArrayList<>(topics)).topicNameValues());
        Set<String> missing = new LinkedHashSet<>();
        for (Map.Entry<String, KafkaFuture<?>> answer : described.entrySet()) {
            try {
                Object ignoredDescription = answer.getValue().get();
            } catch (ExecutionException failed) {
                if (failed.getCause() instanceof UnknownTopicOrPartitionException) {
                    missing.add(answer.getKey());
                    continue;
                }
                throw new IllegalStateException(msg("Could not find out whether the topic {} exists, so this "
                        + "definition's topic-existence policy cannot be applied", answer.getKey()),
                        failed.getCause());
            }
        }
        return missing;
    }

    /**
     * Creates the missing topics with the broker's own {@code num.partitions} and {@code default.replication.factor}
     * - see {@link MissingTopic#CREATE} for why no shape is declared here.
     * <p>
     * A topic that appeared between the describe and this call is not an error: somebody else created what this was
     * about to, which is the outcome either way.
     */
    private static void create(Set<String> missing, Admin client) {
        List<NewTopic> toCreate = new ArrayList<>();
        for (String topic : missing) {
            toCreate.add(new NewTopic(topic, Optional.empty(), Optional.empty()));
        }
        for (Map.Entry<String, KafkaFuture<Void>> created : client.createTopics(toCreate).values().entrySet()) {
            try {
                Void ignoredResult = created.getValue().get();
                log.info("Created the topic {}, which this definition's routes name and the cluster did not have, "
                        + "with the broker's default partition count and replication factor", created.getKey());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while creating the topic " + created.getKey(),
                        interrupted);
            } catch (ExecutionException failed) {
                if (failed.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                    log.debug("The topic {} was created by somebody else between the describe and this create, "
                            + "which is the outcome this wanted", created.getKey());
                    continue;
                }
                throw new IllegalStateException(msg("Could not create the topic {}, which this definition's routes "
                        + "name and the cluster does not have", created.getKey()), failed.getCause());
            }
        }
    }
}
