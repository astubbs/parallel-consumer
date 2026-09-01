package bz.stub.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Library scratchpads for Reactor's two scheduling operators. They assert nothing, which
 * {@code docs/test-hardening/inactive-tests-audit-2026-08-08.md} looked at and judged defensible for
 * this pair; that remains that document's call to revisit, not this one's.
 * <p>
 * What was NOT defensible was {@code new Thread(...).run()}, which invokes the body on the calling
 * thread and never starts the thread at all - so the one thing these were demonstrating, the
 * subscription happening off the test thread, was not happening. Error Prone's {@code DoNotCall} and
 * fb-contrib's {@code RU_INVOKE_RUN} both report it. Now started and joined, so the subscribe call
 * really is made off the test thread.
 * <p>
 * <b>The join does NOT wait for the flux.</b> {@code publishOn} and {@code subscribeOn} hand the work
 * to a {@link reactor.core.scheduler.Scheduler}, so joining the subscribing thread waits only for the
 * subscribe call to be issued - measured against the pinned reactor-core, nothing has been emitted
 * when {@code join()} returns, and the two values arrive on a scheduler thread some time later. An
 * earlier version of this comment claimed completion before the method returns; it was wrong, and a
 * reviewer disproved it by measurement rather than by reading. Anything added here that needs the
 * values must await them, not assume the join did.
 */
@Slf4j
class ReactorTest {

    @SneakyThrows
    @Test
    void publishOn(){
        Scheduler s = Schedulers.newParallel("parallel-scheduler", 4);

        final Flux<String> flux = Flux
                .range(1, 2)
                .map(i -> 10 + i)
                .publishOn(s)
                .map(i -> "value " + i);

        Thread subscriber = new Thread(() -> flux.subscribe(System.out::println));
        subscriber.start();
        subscriber.join();
    }


    @SneakyThrows
    @Test
    void subscribeOn(){
        Scheduler s = Schedulers.newParallel("parallel-scheduler", 4);

        final Flux<String> flux = Flux
                .range(1, 2)
                .map(i -> 10 + i)
                .subscribeOn(s)
                .map(i -> "value " + i);

        Thread subscriber = new Thread(() -> flux.subscribe(System.out::println));
        subscriber.start();
        subscriber.join();
    }

}
