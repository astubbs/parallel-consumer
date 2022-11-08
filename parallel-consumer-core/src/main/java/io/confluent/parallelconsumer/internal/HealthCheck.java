package io.confluent.parallelconsumer.internal;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;

import java.util.Collection;

import static io.confluent.parallelconsumer.internal.HealthCheck.Health.BAD;
import static io.confluent.parallelconsumer.internal.HealthCheck.Health.GOOD;

/**
 * Simple health check for the connection to the Kafka cluster
 *
 * @author Antony Stubbs
 */
@Slf4j
@RequiredArgsConstructor
public class HealthCheck {

    private final AdminClient adminClient;

    /**
     * Makes a simple request toward the cluster for a straight forward test of connectivity
     *
     * @return
     */
    public Health perform() {
        try {
            Collection<Node> nodes = adminClient.describeCluster().nodes().get();
        } catch (Exception e) {
            log.error("Failure describing cluster in health check", e);
            return BAD;
        }
        return GOOD;
    }

    @RequiredArgsConstructor
    public enum Health {
        GOOD, BAD;
    }
}
