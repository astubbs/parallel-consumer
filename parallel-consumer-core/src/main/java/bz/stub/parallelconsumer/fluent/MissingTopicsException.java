package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A start refused because routes name topics the cluster does not have, under {@link MissingTopic#FAIL}.
 * <p>
 * <b>It is a fault of the definition, not a transient one.</b> Nothing about retrying the start makes the topics
 * appear, so this is thrown where a caller is writing their program rather than handling an outage: the names are
 * on the exception as well as in the message, so a caller that catches it can create them and start again without
 * parsing prose.
 *
 * @see MissingTopic
 */
@InterfaceStability.Unstable
public class MissingTopicsException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    /**
     * The topics that were not there, in the order the routes named them.
     */
    private final Set<String> missingTopics;

    MissingTopicsException(String message, Collection<String> missingTopics) {
        super(message);
        this.missingTopics = new LinkedHashSet<>(missingTopics);
    }

    /**
     * The topics this start was refused for, so a caller acting on them does not have to read the message.
     *
     * @return an unmodifiable set of the topic names the cluster did not have
     */
    public Set<String> missingTopics() {
        return java.util.Collections.unmodifiableSet(missingTopics);
    }
}
