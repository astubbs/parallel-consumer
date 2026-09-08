package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This method may WAIT, so only a thread that is allowed to wait may call it - which in this engine means the
 * controller thread. It must never be reached from a Kafka rebalance callback, because those run on the
 * broker-poll thread inside {@code consumer.poll()} with the whole consumer group waiting on them, so a wait
 * there is spent out of {@code max.poll.interval.ms} and can evict the member;
 * {@code ArchitectureTest.rebalanceCallbacksMustNotBlock} is the check, and it reports a reach into an
 * annotated method exactly as it reports a reach into a deny-listed JDK blocking call - for a direct call and
 * for a method reference alike.
 * <p>
 * <b>What the check cannot see, taken unchanged from that rule's own javadoc</b>, because an annotation that
 * implies more coverage than exists is the false green the rule was written against: a {@code synchronized}
 * BLOCK is a {@code MONITORENTER} instruction rather than an access, so it is invisible at any depth; a
 * reference that is STORED rather than invoked (a metrics gauge, an executor task) has the same shape in
 * ArchUnit's model as one invoked immediately, so the walk is conservative in one direction and blind in the
 * other about WHEN a reach happens; and dynamic dispatch through an interface - a user-supplied
 * {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener}, above all - is out of reach whatever is
 * annotated. A green run means "no callback statically reaches an annotated method or a deny-listed call",
 * never "nothing here waits".
 * <p>
 * <b>This is NOT Infer's {@code @ThreadConfined}, and the difference is the reason both exist.</b> The engine's
 * own rule - {@code parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md}, "Declare thread
 * confinement with {@code @ThreadConfined}, and assert it at the entry point" - requires that one to be paired
 * with a runtime assertion, because RacerD CONSUMES it: an unpaired declaration silences a detector and is
 * worse than no annotation at all. This marker is read by no analyser. It silences nothing, so the pairing
 * rationale does not apply to it; the only thing that acts on it is
 * {@code ArchitectureTest.rebalanceCallbacksMustNotBlock}, which reports a reach INTO an annotated method and
 * so fires on the wrong CALLER rather than trusting the callee. It is the static half of the contract, and it
 * is deliberately a marker with no {@code value()}: the thread it names is the one the engine has exactly one
 * of.
 * <p>
 * <b>The runtime half is not implemented</b> - the astubbs/parallel-consumer#393-style ownership guard, which
 * is {@code @ThreadConfined} with a NAMED thread plus an {@code assertOnOwningThread} at each entry point, in
 * the shape astubbs/parallel-consumer#433 established for {@link RetryQueue.RetryQueueIterator} and
 * {@code ThreadConfinedConsumer}. It is tracked in
 * {@code docs/inflight/core-retry-queue-needs-a-runtime-controller-ownership-guard.md}; when it lands, this
 * marker may fold into it.
 * <p>
 * <b>Why it lives in this package.</b> Main code declares no annotation TYPES of its own - this is the
 * first; the Infer annotations it uses are a third party's - so rather than invent a home for it, it sits
 * beside the class it governs, {@link RetryQueue}.
 * Move it up a package when a second, unrelated class needs it.
 *
 * <b>There is no declining alternative on {@link RetryQueue}, and there is deliberately no need for one.</b>
 * Every annotated method takes the write lock unconditionally, and no rebalance callback reaches any of them:
 * the callbacks remove from the shards alone, and {@link ShardManager#purgeDepartedRetryEntries()} collects the
 * retry-queue entries that leaves, on the controller thread. A {@code tryLock()}-based sibling was the
 * superseded astubbs/parallel-consumer#431 design - and had it landed, this marker would have become the only
 * thing able to tell the two apart, since a declining sibling takes the same lock and would be correctly absent
 * from the rule's JDK deny list.
 *
 * @author Antony Stubbs
 * @see RetryQueue#remove(WorkContainer)
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface ControllerThreadOnly {
}
