package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Derives the {@code transactional.id} PC sets on every producer it builds, and the prefix a TransactionalId ACL can
 * be granted on.
 * <p>
 * The id is {@code pc-<L>-<group.id>-<uuid>}: {@code <L>} is the decimal length of the group id, the UUID is
 * generated once per running PC instance and reused by every replacement producer, so that re-initialising the
 * replacement fences the producer it replaces and nothing else.
 * <p>
 * <b>Why the length field.</b> Kafka matches a {@code PREFIXED} ACL literally, so an ACL granted on one group's
 * prefix must not authorise another group's ids. A plain {@code pc-<group.id>-} scheme cannot promise that, because a
 * group id may contain the delimiter: {@code app} yields a literal prefix of {@code app-x}. With the length in front,
 * two prefixes of equal length are equal or neither prefixes the other, and two of different lengths differ inside
 * the length field itself - decimal digits closed by a {@code -}, which is not a digit - so no group's prefix is a
 * string prefix of another's. The cost is that the group id is no longer the leading token, which the prefix's
 * documentation makes up for.
 */
@Slf4j
@UtilityClass
public class TransactionalIdDerivation {

    private static final String SCHEME = "pc";

    /**
     * The prefix every id derived for this group starts with - what a prefixed TransactionalId ACL is granted on.
     */
    public static String prefixFor(String groupId) {
        return SCHEME + "-" + groupId.length() + "-" + groupId + "-";
    }

    /**
     * The id for one running instance of the group.
     */
    public static String derive(String groupId, UUID instanceId) {
        return prefixFor(groupId) + instanceId;
    }

    /**
     * Resolves the caller's producer configuration into the map the factory receives: with the derived id when the
     * producer will be transactional, and with no id at all otherwise. A caller-set id never takes effect on this path
     * and is reported once at WARN, naming both values, because it is the value operational tooling may have been
     * keyed on.
     *
     * @return a new map; the caller's is not modified
     */
    public static Map<String, Object> resolve(Map<String, Object> producerConfig, boolean transactional, String groupId, UUID instanceId) {
        Map<String, Object> resolved = new LinkedHashMap<>(producerConfig);
        Object callerSet = resolved.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        if (transactional) {
            String derived = derive(groupId, instanceId);
            resolved.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, derived);
            if (callerSet != null) {
                log.warn("{} was set to '{}' in the producer configuration, but PC sets it itself where it builds the " +
                                "producer: using '{}' instead. The id is unique per instance and reused by every " +
                                "replacement producer, which is what lets a replacement fence the producer it " +
                                "replaces; the ACL prefix for this group is '{}'.",
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, callerSet, derived, prefixFor(groupId));
            }
        } else if (callerSet != null) {
            log.warn("{} was set to '{}' in the producer configuration, but the commit mode is not transactional, so " +
                            "PC builds a non-transactional producer and the id is removed: a transactional id forces " +
                            "the producer into transactional mode, which this commit mode cannot use.",
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG, callerSet);
        }
        return resolved;
    }
}
