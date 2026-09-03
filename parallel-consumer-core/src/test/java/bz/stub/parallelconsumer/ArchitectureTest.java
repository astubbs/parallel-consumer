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
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.domain.JavaMethodReference;
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
import java.util.Optional;
import java.util.Set;

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
     *
     * <p><b>WHAT THIS RULE CANNOT SEE, and it is not a detail.</b> ArchUnit matches ACCESSES - a call, a
     * reference, a field read. A {@code synchronized} block compiles to a {@code MONITORENTER} instruction,
     * which is none of those, so <b>no {@code synchronized} block is visible to this rule at any depth</b>. The
     * consequence is worth stating without softening: <b>this rule would NOT have caught
     * confluentinc#857</b>, whose defect was {@code synchronized (commitCommand)} inside
     * {@code onPartitionsRevoked}. It fires today only because the remaining violation happens to
     * use {@code Thread.sleep}. A green run therefore means "reaches none of the calls named in
     * {@link #BLOCKING_CALLS}", never "nothing here blocks" - and reading it as the latter is
     * exactly the false green this rule exists to prevent elsewhere.
     *
     * <p><b>A METHOD REFERENCE is not a method call either - walked since 2026-09-03, and it cost a path
     * before it was.</b> ArchUnit models {@code retryQueue::remove} as a method REFERENCE, which
     * {@code getMethodCallsFromSelf()} does not return, so a walk built on that accessor alone never followed
     * one. {@code ShardManager.removeStaleContainers} reached {@code RetryQueue.remove}'s write lock exactly
     * that way, from {@code onPartitionsAssigned} as well as from the revoke and lost callbacks, and this rule
     * reported none of it: measured on 2026-09-02, with every exemption deleted the unfixed tree reported six
     * violations, all through the one DIRECT call and nothing at all on {@code onPartitionsAssigned}, while
     * rewriting that reference as a lambda over a direct call took the same tree to nine.
     * {@code notReachBlockingCalls()} now follows {@code getMethodReferencesFromSelf()} beside the calls;
     * re-measured on 2026-09-03, restoring {@code .map(retryQueue::remove)} takes this rule from green to a
     * report naming all three callbacks. {@code RebalanceCallbackRuleControlTest} is the standing proof of
     * that, so the hop cannot be dropped again without something going red.
     *
     * <p><b>The general lesson survives the fix, because the next blind spot will not be this one:</b> an
     * exemption list that looks complete is evidence about what the walk can see, never about what the
     * callback reaches.
     *
     * <p>Two narrower limits follow from the same mechanism. Synchronized <em>methods</em> ARE
     * detectable, because the modifier survives into the class file, and this rule now flags them -
     * so the blind spot is blocks specifically, not monitors in general. And the walk
     * follows statically resolvable accesses only, so a monitor or a wait behind dynamic dispatch through
     * an interface - a user-supplied {@code ConsumerRebalanceListener}, for instance - is out of reach
     * whatever the deny list says.
     *
     * <p><b>And what walking references cannot tell you is WHEN the reference runs.</b> A reference passed to
     * a stream stage is invoked on this thread before the statement finishes; one handed to a metrics registry
     * or an executor is invoked later, somewhere else. The model has the same shape for both, so this rule
     * treats every reference as an immediate reach - which is right for the defect it was widened to catch and
     * conservative everywhere else. It is also why constructor calls are not walked; see
     * {@link #notReachBlockingCalls()} for that measurement.
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
            "java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()"
    ));

    /**
     * Known violations, each an open defect rather than an accepted design.
     *
     * <p><b>Keyed on {@code root => target}, not on the root alone.</b> A root-keyed exemption silences
     * that callback for EVERY blocking call, so accepting one known defect would hide the next,
     * unrelated one - a gate that goes quiet exactly where it has already found something is worse
     * than one that never looked. The pair form exempts the one reach that is tracked and leaves the
     * callback under inspection for everything else.
     *
     * <p>{@code onPartitionsRevoked}'s {@code while (isTransactionCommittingInProgress())
     * Thread.sleep(100)} is unbounded and transactional-mode only. It arrived as confluentinc#548's
     * fix and is now the defect behind astubbs/parallel-consumer#44 - which holds upstream's
     * {@code verified bug} label - one of a couple of dozen that carry it. Tracked in
     * {@code docs/inflight/bug-857-transactional-revoke-wait.md};
     * remove this entry when that lands.
     *
     * <p><b>The six {@code ReentrantReadWriteLock$WriteLock.lock()} entries that sat here are gone,
     * on merit rather than by exemption</b> - the rebalance callbacks now decline that lock through
     * {@code RetryQueue.tryRemove} instead of waiting for it. {@code tryLock()} is deliberately NOT in
     * {@link #BLOCKING_CALLS}: it returns immediately whether or not it succeeds, which is the whole
     * point of it. Adding it there temporarily is how the fix was checked - the rule then reports all
     * nine reaches, so a green run here is "no callback reaches a WAITING acquire", not "the rule
     * cannot see the retry queue any more".
     */
    private static final Set<String> KNOWN_BLOCKING_VIOLATIONS = new HashSet<>(Arrays.asList(
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsRevoked"
                    + "(java.util.Collection) => java.lang.Thread.sleep(long)"
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

    /**
     * The walk. Three access kinds are followed, and the second is the one that had to be added.
     * <p>
     * <b>A method CALL and a method REFERENCE are different accesses in ArchUnit's model</b>, returned by
     * different accessors and never by each other's. {@code getMethodCallsFromSelf()} alone therefore misses
     * {@code retryQueue::remove} entirely, which is how the retry queue's write lock stayed off this rule's
     * report while three callbacks reached it - see the rule's own javadoc. Both kinds carry the same
     * {@code getTarget().getFullName()} and both resolve to a {@link JavaMethod}, so one deny-list check and
     * one enqueue serve both.
     * <p>
     * <b>CONSTRUCTOR calls are deliberately NOT walked, and that was measured rather than assumed.</b>
     * {@code getConstructorCallsFromSelf()} exists and enqueuing what it returns is a two-line change; doing it
     * on 2026-09-03 turned this rule red on a reach no callback makes.
     * {@code OffsetMapCodecManager.loadPartitionStateForAssignment} calls {@code PCModule.workManager()}, a lazy
     * singleton accessor whose body contains {@code new WorkManager(..)}, whose constructor calls
     * {@code initMetrics()}, which registers {@code WorkManager::getNumberOfWorkQueuedInShardsAwaitingSelection}
     * as a gauge - and that reads the retry queue under its read lock. In production the accessor returns an
     * object built long before, and the gauge runs on a metrics scrape; statically it is a path from
     * {@code onPartitionsAssigned} to {@code ReadLock.lock()}. Walking constructors makes every factory call a
     * reach into whatever the constructed object wires up, so the widening buys a shape nobody has hit and
     * costs a false red today. Revisit it with a way to tell an invoked reference from a stored one.
     */
    private static ArchCondition<JavaMethod> notReachBlockingCalls() {
        return new ArchCondition<>("not reach a blocking call, transitively") {
            @Override
            public void check(JavaMethod root, ConditionEvents events) {
                Set<String> visited = new HashSet<>();
                // JavaCodeUnit rather than JavaMethod, so a constructor can be enqueued alongside a method
                Deque<JavaCodeUnit> queue = new ArrayDeque<>();
                queue.add(root);
                while (!queue.isEmpty()) {
                    JavaCodeUnit current = queue.poll();
                    if (!visited.add(current.getFullName())) {
                        continue;
                    }
                    for (JavaMethodCall call : current.getMethodCallsFromSelf()) {
                        inspectReach(root, current, "call", call.getTarget().getFullName(),
                                call.getTarget().resolveMember(), events, queue);
                    }
                    for (JavaMethodReference reference : current.getMethodReferencesFromSelf()) {
                        inspectReach(root, current, "method reference", reference.getTarget().getFullName(),
                                reference.getTarget().resolveMember(), events, queue);
                    }
                }
            }
        };
    }

    /**
     * One reach, whatever kind of access produced it: check the target against the deny list, then walk into it
     * if it is our own code.
     *
     * @param kind     how {@code target} was reached, for the violation message - a reader has to be able to
     *                 tell a {@code foo::bar} reach from a {@code foo.bar()} one, because the fix differs
     * @param resolved the reached code unit, absent when the target is outside the imported classes; the JDK and
     *                 the Kafka client are the boundary and are deliberately not walked
     */
    private static void inspectReach(JavaMethod root,
                                     JavaCodeUnit from,
                                     String kind,
                                     String target,
                                     Optional<? extends JavaCodeUnit> resolved,
                                     ConditionEvents events,
                                     Deque<JavaCodeUnit> queue) {
        if (BLOCKING_CALLS.contains(target)
                && !KNOWN_BLOCKING_VIOLATIONS.contains(root.getFullName() + " => " + target)) {
            events.add(SimpleConditionEvent.violated(root,
                    root.getFullName() + " reaches blocking " + kind + " " + target
                            + " via " + from.getFullName()
                            + " - a rebalance callback runs inside poll() and must not wait. "
                            + "Decline instead (tryLock), or move the work off the poll thread."));
        }
        // resolveMember() already yields a code unit, so an instanceof here is a null check wearing a type
        // check - which is what BadInstanceof flagged.
        resolved.ifPresent(reached -> {
            if (reached.getOwner().getPackageName().startsWith("bz.stub.parallelconsumer")) {
                // A synchronized METHOD keeps its modifier in the class file, so unlike a synchronized block
                // it is visible here. Entering one from a rebalance callback is an unbounded wait on whoever
                // holds the monitor.
                if (reached.getModifiers().contains(JavaModifier.SYNCHRONIZED)) {
                    events.add(SimpleConditionEvent.violated(root,
                            root.getFullName() + " reaches synchronized method "
                                    + reached.getFullName() + " via " + from.getFullName()
                                    + " - entering a monitor from a rebalance callback waits "
                                    + "for whoever holds it, inside poll()."));
                }
                queue.add(reached);
            }
        });
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
