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
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.domain.JavaModifier;
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


import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.fields;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;

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
     *
     * <p><b>WHAT THIS RULE CANNOT SEE, and it is not a detail.</b> ArchUnit matches method CALLS.
     * A {@code synchronized} block compiles to a {@code MONITORENTER} instruction, which is not a
     * call, so <b>no {@code synchronized} block is visible to this rule at any depth</b>. The
     * consequence is worth stating without softening: <b>this rule would NOT have caught
     * confluentinc#857</b>, whose defect was {@code synchronized (commitCommand)} inside
     * {@code onPartitionsRevoked}. It fires today only because the remaining violation happens to
     * use {@code Thread.sleep}. A green run therefore means "reaches none of the calls named in
     * {@link #BLOCKING_CALLS}", never "nothing here blocks" - and reading it as the latter is
     * exactly the false green this rule exists to prevent elsewhere.
     *
     * <p>Two narrower limits follow from the same mechanism. Synchronized <em>methods</em> ARE
     * detectable, because the modifier survives into the class file, and this rule now flags them -
     * so the blind spot is blocks specifically, not monitors in general. And the walk
     * follows statically resolvable calls only, so a monitor or a wait behind dynamic dispatch through
     * an interface - a user-supplied {@code ConsumerRebalanceListener}, for instance - is out of reach
     * whatever the deny list says.
     *
     * <p>Closing the block gap needs bytecode inspection rather than ArchUnit. Until someone wants
     * that, the honest position is that this rule covers a named, enumerable set and says so.
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
            "java.util.concurrent.CountDownLatch.await()",
            // Added 2026-08-31 by the astubbs/parallel-consumer#29 defect-class sweep. The original
            // list named only what the two known defects used, which meant the rule answered a
            // narrower question than its own description claimed.
            "java.lang.Object.wait()",
            "java.lang.Object.wait(long)",
            "java.lang.Thread.join()",
            "java.lang.Thread.join(long)",
            "java.util.concurrent.Semaphore.acquire()",
            "java.util.concurrent.locks.Condition.await()",
            "java.util.concurrent.Future.get()",
            "java.util.concurrent.CompletableFuture.get()",
            "java.util.concurrent.CompletableFuture.join()",
            "java.util.concurrent.BlockingQueue.take()",
            "java.util.concurrent.BlockingQueue.put(java.lang.Object)",
            "java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock.lock()",
            "java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            // Added 2026-09-01 by astubbs/parallel-consumer#44. A TIMED acquire is still a wait, and this
            // rule's own advice - "decline (tryLock) rather than wait" - reads as if tryLock were the cure,
            // which is how the five-minute tryLock(commitLockAcquisitionTimeout) that confluentinc#803's
            // stack trace actually threw from sat inside a rebalance callback with the gate green. The
            // no-arg tryLock() IS the cure and is deliberately absent from this list; the timed overloads
            // are not, because a callback inside poll() has no budget to spend waiting for anything.
            "java.util.concurrent.locks.Lock.tryLock(long, java.util.concurrent.TimeUnit)",
            "java.util.concurrent.locks.ReentrantLock.tryLock(long, java.util.concurrent.TimeUnit)",
            "java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock.tryLock(long, java.util.concurrent.TimeUnit)",
            "java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.tryLock(long, java.util.concurrent.TimeUnit)",
            "java.util.concurrent.CountDownLatch.await(long, java.util.concurrent.TimeUnit)",
            "java.util.concurrent.Future.get(long, java.util.concurrent.TimeUnit)",
            "java.util.concurrent.BlockingQueue.poll(long, java.util.concurrent.TimeUnit)"
    ));

    /**
     * Known violations, each an open defect rather than an accepted design.
     *
     * <p><b>Keyed on {@code root => via => target}.</b> The {@code via} was added 2026-09-02: without it, two
     * different paths from one callback to the SAME blocking target share a key, so exempting one silently
     * exempts the other. That is not theoretical - astubbs#44 deleted {@code onPartitionsRevoked}'s
     * {@code Thread.sleep} spin and the callback still reaches {@code Thread.sleep} through
     * {@code ConsumerManager.retryBackOff}, so a two-part key would have re-blinded the rule to the very spin
     * that was just removed while looking like tracked debt.
     *
     * <p><b>Not keyed on the root alone either.</b> A root-keyed exemption silences
     * that callback for EVERY blocking call, so accepting one known defect would hide the next,
     * unrelated one - a gate that goes quiet exactly where it has already found something is worse
     * than one that never looked. The pair form exempts the one reach that is tracked and leaves the
     * callback under inspection for everything else.
     *
     * <p><b>The {@code Thread.sleep} entry is gone, which is the point.</b> It exempted
     * {@code onPartitionsRevoked}'s {@code while (isTransactionCommittingInProgress()) Thread.sleep(100)},
     * arriving with confluentinc#548 and tracked as astubbs/parallel-consumer#44. That spin has been
     * removed, so the exemption went with it - which is what this list is for: open debt, deleted when
     * paid, never an accepted design.
     */
    private static final Set<String> KNOWN_BLOCKING_VIOLATIONS = new HashSet<>(Arrays.asList(
            // The RetryQueue write lock on the revoke/lost path. Pre-existing on master; owner:
            // docs/inflight/bug-retry-queue-write-lock-on-the-rebalance-path.md
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsRevoked(java.util.Collection) => "
                    + "bz.stub.parallelconsumer.state.RetryQueue.remove(bz.stub.parallelconsumer.state.WorkContainer) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsLost(java.util.Collection) => "
                    + "bz.stub.parallelconsumer.state.RetryQueue.remove(bz.stub.parallelconsumer.state.WorkContainer) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.state.PartitionStateManager.onPartitionsRevoked(java.util.Collection) => "
                    + "bz.stub.parallelconsumer.state.RetryQueue.remove(bz.stub.parallelconsumer.state.WorkContainer) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.state.PartitionStateManager.onPartitionsLost(java.util.Collection) => "
                    + "bz.stub.parallelconsumer.state.RetryQueue.remove(bz.stub.parallelconsumer.state.WorkContainer) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.state.WorkManager.onPartitionsRevoked(java.util.Collection) => "
                    + "bz.stub.parallelconsumer.state.RetryQueue.remove(bz.stub.parallelconsumer.state.WorkContainer) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.state.WorkManager.onPartitionsLost(java.util.Collection) => "
                    + "bz.stub.parallelconsumer.state.RetryQueue.remove(bz.stub.parallelconsumer.state.WorkContainer) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",

            // SURFACED 2026-09-02 by teaching the walk to follow interface hops (astubbs#44). All three
            // are PRE-EXISTING and were invisible while the walk stopped at the declared member - the rule
            // was blind to the very defect class it was written for. Owner:
            // docs/inflight/static-archunit-walk-was-blind-through-interface-hops.md
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsRevoked(java.util.Collection) => "
                    + "bz.stub.parallelconsumer.internal.ConsumerManager.retryBackOff(long) => java.lang.Thread.sleep(long)",
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsRevoked(java.util.Collection) => "
                    + "bz.stub.parallelconsumer.internal.ConsumerOffsetCommitter.commitAndWait() => java.util.concurrent.BlockingQueue.poll(long, java.util.concurrent.TimeUnit)",
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsRevoked(java.util.Collection) => "
                    + "bz.stub.parallelconsumer.internal.ProducerManager.lazyMaybeBeginTransaction() => bz.stub.parallelconsumer.internal.ProducerManager.syncBeginTransaction()"
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
                        if (BLOCKING_CALLS.contains(target)
                                && !KNOWN_BLOCKING_VIOLATIONS.contains(
                                        root.getFullName() + " => " + current.getFullName() + " => " + target)) {
                            events.add(SimpleConditionEvent.violated(root,
                                    root.getFullName() + " reaches blocking call " + target
                                            + " via " + current.getFullName()
                                            + " - a rebalance callback runs inside poll() and must not wait. "
                                            + "Decline instead (tryLock), or move the work off the poll thread."));
                        }
                        // only walk our own code; the JDK and Kafka client are the boundary
                        // resolveMember() on a method call already yields a JavaMethod, so an instanceof
                        // here is a null check wearing a type check - which is what BadInstanceof flagged.
                        // FOLLOW INTERFACE HOPS. resolveMember() yields the DECLARED target, so a call through an
                        // interface-typed field lands on the abstract method - which has no body - and the walk
                        // stops there. That is not hypothetical: `committer` is declared as OffsetCommitter, so
                        // `committer.retrieveOffsetsAndCommit()` never reached ProducerManager.acquireCommitLock()
                        // and its timed tryLock. The deny entries above would have been decorative on exactly the
                        // path astubbs/parallel-consumer#44 fixed. Fan out to PC-owned implementations so the
                        // rule sees what it claims to.
                        call.getTarget().resolveMember().ifPresent(declared -> {
                            if (declared.getOwner().isInterface() || declared.getModifiers().contains(JavaModifier.ABSTRACT)) {
                                declared.getOwner().getAllSubclasses().stream()
                                        .filter(impl -> impl.getPackageName().startsWith("bz.stub.parallelconsumer"))
                                        .forEach(impl -> impl.tryGetMethod(declared.getName(), declared.getRawParameterTypes().stream()
                                                        .map(JavaClass::getName).toArray(String[]::new))
                                                .ifPresent(queue::add));
                            }
                        });
                        call.getTarget().resolveMember().ifPresent(reached -> {
                            if (reached.getOwner().getPackageName().startsWith("bz.stub.parallelconsumer")) {
                                // A synchronized METHOD keeps its modifier in the class file, so unlike a
                                // synchronized block it is visible here. Entering one from a rebalance
                                // callback is an unbounded wait on whoever holds the monitor.
                                if (reached.getModifiers().contains(JavaModifier.SYNCHRONIZED)
                                        && !KNOWN_BLOCKING_VIOLATIONS.contains(
                                                root.getFullName() + " => " + current.getFullName() + " => " + reached.getFullName())) {
                                    events.add(SimpleConditionEvent.violated(root,
                                            root.getFullName() + " reaches synchronized method "
                                                    + reached.getFullName() + " via " + current.getFullName()
                                                    + " - entering a monitor from a rebalance callback waits "
                                                    + "for whoever holds it, inside poll()."));
                                }
                                queue.add(reached);
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
