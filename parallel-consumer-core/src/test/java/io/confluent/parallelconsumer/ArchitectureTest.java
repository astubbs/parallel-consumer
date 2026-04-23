package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import io.confluent.parallelconsumer.internal.ConsumerManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.tngtech.archunit.core.domain.JavaAccess;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.fields;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * ArchUnit rules enforcing the architecture of the Parallel Consumer.
 * <p>
 * These rules prevent regressions in thread-safety and encapsulation.
 * See <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a>.
 */
@AnalyzeClasses(
        packages = "io.confluent.parallelconsumer",
        importOptions = ImportOption.DoNotIncludeTests.class
)
class ArchitectureTest {

    // Classes allowed to hold a Consumer<K,V> field. Use getName() to avoid hardcoded strings.
    // ThreadConfinedConsumer is package-private so we reference it by name.
    private static final Set<String> ALLOWED_CONSUMER_HOLDERS = new HashSet<>(Arrays.asList(
            ConsumerManager.class.getName(),
            "io.confluent.parallelconsumer.internal.ThreadConfinedConsumer",
            ParallelConsumerOptions.class.getName(),
            // Lombok @Builder generates this inner class which also holds the consumer field
            ParallelConsumerOptions.class.getName() + "$ParallelConsumerOptionsBuilder"
    ));

    /**
     * Only the designated wrapper/options classes may hold a Consumer or KafkaConsumer field.
     * This prevents accidental raw consumer access that bypasses the thread-confinement wrapper.
     */
    @ArchTest
    static final ArchRule noRawConsumerFieldsOutsideWrappers =
            fields()
                    .that().haveRawType(Consumer.class)
                    .or().haveRawType(KafkaConsumer.class)
                    .should(beInAllowedClasses(ALLOWED_CONSUMER_HOLDERS))
                    .as("Only " + ALLOWED_CONSUMER_HOLDERS + " may hold a Consumer<K,V> field. " +
                            "All other consumer access must go through ConsumerManager. See #857.");

    /**
     * Only ProducerWrapper should hold a raw Producer field.
     * ProducerManager holds ProducerWrapper, not raw Producer.
     */
    @ArchTest
    static final ArchRule noRawProducerFieldsOutsideWrapper =
            fields()
                    .that().haveRawType("org.apache.kafka.clients.producer.Producer")
                    .or().haveRawType("org.apache.kafka.clients.producer.KafkaProducer")
                    .should(beInAllowedClasses(new HashSet<>(Arrays.asList(
                            "io.confluent.parallelconsumer.internal.ProducerWrapper",
                            ParallelConsumerOptions.class.getName(),
                            ParallelConsumerOptions.class.getName() + "$ParallelConsumerOptionsBuilder"
                    ))))
                    .as("Only ProducerWrapper and ParallelConsumerOptions may hold a Producer<K,V> field. " +
                            "All other producer access must go through ProducerWrapper/ProducerManager.");

    // Future: add rule that ConsumerManager is only constructed by PCModule.
    // Requires DescribedPredicate API which is verbose — defer for now.

    private static ArchCondition<JavaField> beInAllowedClasses(Set<String> allowedClassNames) {
        return new ArchCondition<>("be declared in an allowed class") {
            @Override
            public void check(JavaField field, ConditionEvents events) {
                String ownerName = field.getOwner().getName();
                if (!allowedClassNames.contains(ownerName)) {
                    events.add(SimpleConditionEvent.violated(field,
                            "Field " + field.getFullName() + " holds a Consumer/Producer reference but " +
                                    ownerName + " is not in the allowed list: " + allowedClassNames));
                }
            }
        };
    }
}
