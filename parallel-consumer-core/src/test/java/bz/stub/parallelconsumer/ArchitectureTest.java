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
import bz.stub.parallelconsumer.state.ControllerThreadOnly;
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
     * {@link #BLOCKING_CALLS} and no method the codebase itself declares as waiting", never "nothing here
     * blocks" - and reading it as the latter is
     * exactly the false green this rule exists to prevent elsewhere.
     *
     * <p><b>Since 2026-09-07 this rule enforces a CODEBASE-DECLARED contract as well as the JDK deny list.</b>
     * A reach into a method annotated {@link ControllerThreadOnly} is reported exactly as a deny-listed call
     * is - same {@code root => target} exemption key, same message shape, calls and method references alike.
     * The deny list can only name waits it recognises by JDK signature, and that is a narrower question than
     * the rule asks. A {@code tryLock()}-based sibling of {@code RetryQueue.remove} would take the SAME lock and
     * be correctly absent from the list, so once both live on one class no entry in it can tell a waiting
     * acquire from a declining one - only the method's own declared contract can.
     * astubbs/parallel-consumer#431 is the change that creates that pair, and this half of the rule is what
     * keeps it honest afterwards. {@code RebalanceCallbackRuleControlTest} holds the standing proof, on a
     * fixture whose annotated method waits for nothing at all - a field write - so nothing in
     * {@link #BLOCKING_CALLS} can match it and the annotation check is the only thing that can report it.
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
     * <p><b>One entry has been retired, and what replaced it is a bounded wait this list does not
     * name - deliberately.</b> {@code onPartitionsRevoked}'s {@code while (isTransactionCommittingInProgress())
     * Thread.sleep(100)} spin - confluentinc#548's fix, unbounded, transactional-mode only, the defect
     * behind astubbs/parallel-consumer#44 - is gone: in transactional mode the callback now hands its
     * commit to the control thread and waits on a {@code CompletableFuture} with a deadline
     * ({@code commitOnRevokeViaTheControlThread}, which carries the reasoning). That is
     * {@code CompletableFuture.get(long, TimeUnit)}, and {@link #BLOCKING_CALLS} names the untimed
     * {@code get()} and {@code join()} but not the timed form. Keep it that way, or add the timed form
     * together with a {@code root => target} exemption for that one reach: the wait is safe there
     * because a transactional commit needs nothing from the poll thread, which is the opposite of the
     * edge both known deadlocks ran along, and it is bounded by the same timeout the inline commit's
     * own lock acquisition had.
     *
     * <p><b>The twelve entries below are debt this rule's own widening MADE VISIBLE, not debt it created.</b>
     * The reaches were always there; the walk could not see them, so the list read as complete while three
     * callbacks sat on an unbounded write-lock acquire - which is the exact false green
     * {@link #rebalanceCallbacksMustNotBlock()} exists to prevent. Every one of them is owned by
     * astubbs/parallel-consumer#431 and is deleted by it. Recording them here rather than shipping the rule
     * green is deliberate: a gate that arrives green has measured nothing, and an exemption with a named owner
     * is a defect on the books, which a reach nobody can see is not.
     */
    private static final Set<String> KNOWN_BLOCKING_VIOLATIONS = new HashSet<>(Arrays.asList(
            // The RetryQueue write lock on the revoke/lost path. Pre-existing on master, surfaced by the
            // astubbs/parallel-consumer#29 defect-class sweep once the deny list learned about
            // ReentrantReadWriteLock. Owner: docs/inflight/bug-retry-queue-write-lock-on-the-rebalance-path.md
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsRevoked"
                    + "(java.util.Collection) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsLost"
                    + "(java.util.Collection) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.state.PartitionStateManager.onPartitionsLost"
                    + "(java.util.Collection) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.state.PartitionStateManager.onPartitionsRevoked"
                    + "(java.util.Collection) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.state.WorkManager.onPartitionsLost"
                    + "(java.util.Collection) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.state.WorkManager.onPartitionsRevoked"
                    + "(java.util.Collection) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",

            // ---- Debt this commit MADE VISIBLE rather than introduced. Twelve entries, all one defect. ----
            // The widened walk and the @ControllerThreadOnly check each report reaches that were always there
            // and that the old rule could not see. Owner for every entry below:
            // astubbs/parallel-consumer#431, which routes these sweeps onto a declining tryLock() path - when
            // it lands, all twelve go with it, together with the six WriteLock entries above.

            // 1. The METHOD REFERENCE the old walk could not follow: ShardManager.removeStaleContainers does
            // `.map(retryQueue::remove)`, so all three callbacks reach the write lock through it. The revoke and
            // lost roots collide with the six pre-existing keys above and are already covered; the ASSIGNED
            // roots are new here, because nothing had ever reported a reach on that callback at all.
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsAssigned"
                    + "(java.util.Collection) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.state.PartitionStateManager.onPartitionsAssigned"
                    + "(java.util.Collection) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",
            "bz.stub.parallelconsumer.state.WorkManager.onPartitionsAssigned"
                    + "(java.util.Collection) => java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()",

            // 2. The DECLARED contract: every rebalance callback reaches RetryQueue.remove, which is
            // @ControllerThreadOnly - by direct call through ShardManager.removeWorkFromShardFor on the revoke
            // and lost paths, and by method reference through ShardManager.removeStaleContainers on all three.
            // Keyed on the annotated method rather than on the JDK lock, so these are separate keys from the
            // three above even where the root is the same - which is the point: the two halves of the rule
            // answer different questions and neither one silences the other.
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsAssigned"
                    + "(java.util.Collection) => bz.stub.parallelconsumer.state.RetryQueue"
                    + ".remove(bz.stub.parallelconsumer.state.WorkContainer)",
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsRevoked"
                    + "(java.util.Collection) => bz.stub.parallelconsumer.state.RetryQueue"
                    + ".remove(bz.stub.parallelconsumer.state.WorkContainer)",
            "bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.onPartitionsLost"
                    + "(java.util.Collection) => bz.stub.parallelconsumer.state.RetryQueue"
                    + ".remove(bz.stub.parallelconsumer.state.WorkContainer)",
            "bz.stub.parallelconsumer.state.PartitionStateManager.onPartitionsAssigned"
                    + "(java.util.Collection) => bz.stub.parallelconsumer.state.RetryQueue"
                    + ".remove(bz.stub.parallelconsumer.state.WorkContainer)",
            "bz.stub.parallelconsumer.state.PartitionStateManager.onPartitionsRevoked"
                    + "(java.util.Collection) => bz.stub.parallelconsumer.state.RetryQueue"
                    + ".remove(bz.stub.parallelconsumer.state.WorkContainer)",
            "bz.stub.parallelconsumer.state.PartitionStateManager.onPartitionsLost"
                    + "(java.util.Collection) => bz.stub.parallelconsumer.state.RetryQueue"
                    + ".remove(bz.stub.parallelconsumer.state.WorkContainer)",
            "bz.stub.parallelconsumer.state.WorkManager.onPartitionsAssigned"
                    + "(java.util.Collection) => bz.stub.parallelconsumer.state.RetryQueue"
                    + ".remove(bz.stub.parallelconsumer.state.WorkContainer)",
            "bz.stub.parallelconsumer.state.WorkManager.onPartitionsRevoked"
                    + "(java.util.Collection) => bz.stub.parallelconsumer.state.RetryQueue"
                    + ".remove(bz.stub.parallelconsumer.state.WorkContainer)",
            "bz.stub.parallelconsumer.state.WorkManager.onPartitionsLost"
                    + "(java.util.Collection) => bz.stub.parallelconsumer.state.RetryQueue"
                    + ".remove(bz.stub.parallelconsumer.state.WorkContainer)"
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
     * One reach, whatever kind of access produced it: check the target against the deny list, then - once it is
     * resolved as our own code - against {@link ControllerThreadOnly} and the synchronized-method modifier,
     * then walk into it.
     * <p>
     * The deny-list check runs on the target's NAME and needs no resolution, which is what lets it cover the JDK.
     * The annotation check necessarily runs on the resolved member, so it can only ever fire on this codebase -
     * which is the whole point of it: it is the half of the rule the JDK deny list cannot express.
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
                // A method the codebase itself declares as "may wait", reported exactly like a deny-listed JDK
                // call: same root => target exemption key, same message shape. The owner is checked too because
                // the annotation targets TYPE as well as METHOD, and a class-level declaration that the walk
                // ignored would be a silent gap rather than a narrower rule.
                if ((reached.isAnnotatedWith(ControllerThreadOnly.class)
                        || reached.getOwner().isAnnotatedWith(ControllerThreadOnly.class))
                        && !KNOWN_BLOCKING_VIOLATIONS.contains(root.getFullName() + " => " + target)) {
                    events.add(SimpleConditionEvent.violated(root,
                            root.getFullName() + " reaches @ControllerThreadOnly " + kind + " " + target
                                    + " via " + from.getFullName()
                                    + " - that method declares that it may wait, and a rebalance callback runs "
                                    + "inside poll() and must not wait. "
                                    + "Decline instead (tryLock), or move the work off the poll thread."));
                }
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
