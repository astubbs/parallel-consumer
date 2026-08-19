package bz.stub.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.internal.ExternalEngine;
import bz.stub.parallelconsumer.state.WorkContainer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;
import pl.tlinkowski.unij.api.UniLists;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.internal.UserFunctions.carefullyRun;
import static bz.stub.parallelconsumer.internal.utils.ThrowableUtils.logWithoutEscaping;

/**
 * Adapter for using Project Reactor as the asynchronous execution engine
 */
@Slf4j
public class ReactorProcessor<K, V> extends ExternalEngine<K, V> {

    /**
     * @see WorkContainer#getWorkType()
     */
    private static final String REACTOR_TYPE = "reactor.x-type";

    private final Supplier<Scheduler> schedulerSupplier;
    private final Supplier<Scheduler> defaultSchedulerSupplier = Schedulers::boundedElastic;

    public ReactorProcessor(ParallelConsumerOptions<K, V> options, Supplier<Scheduler> newSchedulerSupplier) {
        super(options);
        this.schedulerSupplier = (newSchedulerSupplier == null) ? defaultSchedulerSupplier : newSchedulerSupplier;
    }

    public ReactorProcessor(ParallelConsumerOptions<K, V> options) {
        this(options, null);
    }

    @Override
    protected boolean isAsyncFutureWork(List<?> resultsFromUserFunction) {
        for (Object object : resultsFromUserFunction) {
            return (object instanceof Disposable);
        }
        return false;
    }

    @SneakyThrows
    @Override
    public void close(Duration timeout, DrainingMode drainMode) {
        super.close(timeout, drainMode);
    }

    /**
     * Register a function to be to polled messages.
     * <p>
     * Make sure that you do any work immediately in a Publisher / Flux - do not block this thread.
     * <p>
     *
     * @param reactorFunction user function that takes a single record, and returns some type of Publisher to process
     *                        their work.
     * @see #react(Function)
     * @see ParallelConsumerOptions
     * @see ParallelConsumerOptions#batchSize
     * @see bz.stub.parallelconsumer.ParallelStreamProcessor#poll
     */
    public void react(Function<PollContext<K, V>, Publisher<?>> reactorFunction) {

        Function<PollContextInternal<K, V>, List<Object>> wrappedUserFunc = pollContext -> {

            if (log.isTraceEnabled()) {
                log.trace("Record list ({}), executing void function...",
                        pollContext.streamConsumerRecords()
                                .map(ConsumerRecord::offset)
                                .collect(Collectors.toList())
                );
            }

            // attach internal handler
            pollContext.streamWorkContainers()
                    .forEach(x -> x.setWorkType(REACTOR_TYPE));

            Disposable flux = Mono.fromCallable(() -> carefullyRun(reactorFunction, pollContext.getPollContext()))
                    .flatMapMany(it -> it)
                    .doOnNext(signal -> log.trace("doOnNext {}", signal))
                    .subscribeOn(getScheduler())
                    .subscribe(ignore -> onComplete(pollContext), throwable -> onError(pollContext, throwable));

            log.trace("asyncPoll - user function finished ok.");
            return UniLists.of(flux);
        };

        //
        Consumer<Object> voidCallBack = (ignore) -> log.trace("Void callback applied.");
        supervisorLoop(wrappedUserFunc, voidCallBack);
    }

    private void onComplete(PollContextInternal<K, V> pollContext) {
        log.debug("Reactor success");
        pollContext.streamWorkContainers().forEach(wc -> {
            wc.onUserFunctionSuccess();
            addToMailbox(pollContext, wc);
        });
    }

    private void onError(PollContextInternal<K, V> pollContext, Throwable throwable) {
        onAsyncFailure(pollContext, throwable, "Reactor fail signal");
    }

    /**
     * Reactor repackages what it propagates, and core cannot name reactor's wrapper types - so peel with reactor's
     * own helper before the retriable classification looks at what is underneath.
     */
    @Override
    protected Throwable unwrapFrameworkWrapper(Throwable throwable) {
        return Exceptions.unwrap(throwable);
    }

    private Scheduler getScheduler() {
        return this.schedulerSupplier.get();
    }

}
