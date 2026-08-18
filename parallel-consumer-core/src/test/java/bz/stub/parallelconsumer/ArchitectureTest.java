package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.lang.ArchRule;

import java.util.ArrayDeque;
import java.util.Deque;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import bz.stub.parallelconsumer.internal.ConsumerManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.tngtech.archunit.core.domain.JavaAccess;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.fields;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * ArchUnit rules enforcing the architecture of the Parallel Consumer.
 * <p>
 * These rules prevent regressions in thread-safety and encapsulation.
 * See <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a>.
 */
@AnalyzeClasses(
        packages = "bz.stub.parallelconsumer",
        importOptions = ImportOption.DoNotIncludeTests.class
)
class ArchitectureTest {

    // Classes allowed to hold a Consumer<K,V> field. Use getName() to avoid hardcoded strings.
    // ThreadConfinedConsumer is package-private so we reference it by name.
    private static final Set<String> ALLOWED_CONSUMER_HOLDERS = new HashSet<>(Arrays.asList(
            ConsumerManager.class.getName(),
            "bz.stub.parallelconsumer.internal.ThreadConfinedConsumer",
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
                            "All other consumer access must go through ConsumerManager. See confluentinc#857.");

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
                            "bz.stub.parallelconsumer.internal.ProducerWrapper",
                            ParallelConsumerOptions.class.getName(),
                            ParallelConsumerOptions.class.getName() + "$ParallelConsumerOptionsBuilder"
                    ))))
                    .as("Only ProducerWrapper and ParallelConsumerOptions may hold a Producer<K,V> field. " +
                            "All other producer access must go through ProducerWrapper/ProducerManager.");

    // Future: add rule that ConsumerManager is only constructed by PCModule.
    // Requires DescribedPredicate API which is verbose — defer for now.


    /**
     * Nothing reachable from a Kafka rebalance callback may block.
     *
     * <p>A rebalance callback runs on the poll thread <em>inside</em> {@code consumer.poll()}, and the
     * whole consumer group waits while it runs - overrunning {@code max.poll.interval.ms} evicts the
     * member. So it is a hot, group-blocking context by definition, and anything it cannot get
     * immediately it must decline rather than wait for.
     *
     * <p><b>This rule exists because the same seam has produced two deadlocks between the same two
     * threads.</b> confluentinc#548 (2023) and confluentinc#857 both came from a rebalance callback
     * waiting on something the control thread held. Each was fixed by hand and the invariant was
     * written down; a rule fires on its own. See
     * {@code docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md}.
     *
     * <p><b>The exemption list is the known-open debt, not a way to pass.</b> Each entry is a real
     * violation with an owner; adding to it should feel like taking on a defect, because it is.
     */
    @ArchTest
    static final ArchRule rebalanceCallbacksMustNotBlock =
            methods()
                    .that(areRebalanceCallbacks())
                    .should(notReachBlockingCalls())
                    .as("No method reachable from a rebalance callback may block: it runs inside poll(), " +
                            "so waiting there burns max.poll.interval.ms and can evict the member. " +
                            "Decline (tryLock) rather than wait. See confluentinc#857.");

    /** Blocking calls a rebalance callback must never reach, transitively. */
    private static final Set<String> BLOCKING_CALLS = new HashSet<>(Arrays.asList(
            "java.lang.Thread.sleep(long)",
            "java.util.concurrent.locks.Lock.lock()",
            "java.util.concurrent.locks.Lock.lockInterruptibly()",
            "java.util.concurrent.locks.ReentrantLock.lock()",
            "java.util.concurrent.locks.ReentrantLock.lockInterruptibly()",
            "java.util.concurrent.CountDownLatch.await()"
    ));

    /**
     * Known violations, each an open defect rather than an accepted design.
     *
     * <p>{@code onPartitionsRevoked}'s {@code while (isTransactionCommittingInProgress())
     * Thread.sleep(100)} is unbounded and transactional-mode only. It arrived as confluentinc#548's
     * fix and is now the defect behind astubbs/parallel-consumer#44 - the only issue upstream ever
     * labelled a verified bug. Tracked in {@code docs/inflight/bug-857-transactional-revoke-wait.md};
     * remove this entry when that lands.
     */
    private static final Set<String> KNOWN_BLOCKING_VIOLATIONS = new HashSet<>(Arrays.asList(
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsRevoked(java.util.Collection)"
    ));

    private static DescribedPredicate<JavaMethod> areRebalanceCallbacks() {
        return new DescribedPredicate<>("are Kafka rebalance callbacks") {
            @Override
            public boolean test(JavaMethod method) {
                String name = method.getName();
                return (name.equals("onPartitionsRevoked")
                        || name.equals("onPartitionsAssigned")
                        || name.equals("onPartitionsLost"))
                        && method.getOwner().getPackageName().startsWith("bz.stub.parallelconsumer");
            }
        };
    }

    private static ArchCondition<JavaMethod> notReachBlockingCalls() {
        return new ArchCondition<>("not reach a blocking call, transitively") {
            @Override
            public void check(JavaMethod root, ConditionEvents events) {
                if (KNOWN_BLOCKING_VIOLATIONS.contains(root.getFullName())) {
                    return; // open defect, tracked - see KNOWN_BLOCKING_VIOLATIONS
                }
                Set<String> visited = new HashSet<>();
                Deque<JavaMethod> queue = new ArrayDeque<>();
                queue.add(root);
                while (!queue.isEmpty()) {
                    JavaMethod current = queue.poll();
                    if (!visited.add(current.getFullName())) {
                        continue;
                    }
                    for (JavaMethodCall call : current.getMethodCallsFromSelf()) {
                        String target = call.getTarget().getFullName();
                        if (BLOCKING_CALLS.contains(target)) {
                            events.add(SimpleConditionEvent.violated(root,
                                    root.getFullName() + " reaches blocking call " + target
                                            + " via " + current.getFullName()
                                            + " - a rebalance callback runs inside poll() and must not wait. "
                                            + "Decline instead (tryLock), or move the work off the poll thread."));
                        }
                        // only walk our own code; the JDK and Kafka client are the boundary
                        call.getTarget().resolveMember().ifPresent(m -> {
                            if (m instanceof JavaMethod
                                    && m.getOwner().getPackageName().startsWith("bz.stub.parallelconsumer")) {
                                queue.add((JavaMethod) m);
                            }
                        });
                    }
                }
            }
        };
    }

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
