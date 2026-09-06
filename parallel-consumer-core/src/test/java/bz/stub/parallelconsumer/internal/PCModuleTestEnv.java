package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.state.ModelUtils;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.mockito.Mockito;
import org.threeten.extra.MutableClock;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * Version of the {@link PCModule} in test contexts.
 *
 * @author Antony Stubbs
 */
public class PCModuleTestEnv extends PCModule<String, String> {

    ModelUtils mu = new ModelUtils(this);
    Optional<CountDownLatch> workManagerController;
    private WorkManager<String, String> workManager;
    private final DynamicLoadFactor limitedDynamicLoadFactor = new LimitedDynamicExtraLoadFactor();


    /**
     * A module whose processor does <b>not</b> close itself, for a test that drives the control loop by hand and
     * therefore owns the lifecycle - closing would also NPE on the mocked producer's absent transaction manager.
     * <p>
     * Shared because three tests had written the same anonymous subclass out separately, and the resulting
     * whole-file likeness is what pushed {@code dups: similarity} over its increase-vs-base cap. The engine
     * compares files including their comments, so a copied block costs twice: once as structure and again as the
     * prose explaining it.
     *
     * @param options            what the module is built with
     * @param alwaysTimeToCommit forces {@code isTimeToCommitNow()}, for a test that wants a commit attempt on every
     *                           control-loop pass rather than waiting out the real interval
     */
    public static PCModuleTestEnv withHandDrivenProcessor(ParallelConsumerOptions<String, String> options,
                                                          boolean alwaysTimeToCommit) {
        return new PCModuleTestEnv(options) {
            @Override
            protected AbstractParallelEoSStreamProcessor<String, String> pc() {
                if (parallelEoSStreamProcessor == null) {
                    parallelEoSStreamProcessor = new bz.stub.parallelconsumer.ParallelEoSStreamProcessor<String, String>(options(), this) {
                        @Override
                        protected boolean isTimeToCommitNow() {
                            return alwaysTimeToCommit || super.isTimeToCommitNow();
                        }

                        @Override
                        public void close() {
                        }
                    };
                }
                return parallelEoSStreamProcessor;
            }
        };
    }

    @Override
    protected DynamicLoadFactor dynamicExtraLoadFactor() {
        return limitedDynamicLoadFactor;
    }

    public PCModuleTestEnv(final ParallelConsumerOptions<String, String> optionsInstance,
                           final CountDownLatch latch) {
        super(optionsInstance);
        this.workManagerController = Optional.of(latch);
    }

    public PCModuleTestEnv(ParallelConsumerOptions<String, String> optionsInstance) {
        super(optionsInstance);

        ParallelConsumerOptions<String, String> override = enhanceOptions(optionsInstance);

        // overwrite super's with new instance
        super.optionsInstance = override;
        this.workManagerController = Optional.empty();
    }

    private ParallelConsumerOptions<String, String> enhanceOptions(ParallelConsumerOptions<String, String> optionsInstance) {
        var copy = options().toBuilder();

        if (optionsInstance.getConsumer() == null) {
            Consumer<String, String> mockConsumer = Mockito.mock(Consumer.class);
            Mockito.when(mockConsumer.groupMetadata()).thenReturn(mu.consumerGroupMeta());
            copy.consumer(mockConsumer);
        }

        var override = copy
                .producer(Mockito.mock(Producer.class))
                .build();

        return override;
    }

    public PCModuleTestEnv() {
        this(ParallelConsumerOptions.<String, String>builder().build());
    }

    @Override
    protected ProducerWrapper<String, String> producerWrap() {
        return mockProducerWrapTransactional();
    }

    ProducerWrapper<String, String> mockProduceWrap;

    @NonNull
    private ProducerWrapper<String, String> mockProducerWrapTransactional() {
        if (mockProduceWrap == null) {
            mockProduceWrap = Mockito.spy(new ProducerWrapper<>(options(), true, producer()));
        }
        return mockProduceWrap;
    }

    @Override
    public WorkManager<String, String> workManager() {
        if (this.workManager == null) {
            this.workManager = this.workManagerController.isPresent() ?
                    new PausableWorkManager<>(this, dynamicExtraLoadFactor(), workManagerController.get())
                    : super.workManager();
        }
        return workManager;
    }

    @Override
    protected ConsumerManager<String, String> consumerManager() {
        ConsumerManager<String, String> consumerManager = super.consumerManager();

        // force update to set cache, otherwise maybe never called (fake consumer)
        consumerManager.updateCache();

        return consumerManager;
    }

    @Getter
    private final MutableClock mutableClock = MutableClock.epochUTC();

    @Override
    public Clock clock() {
        return mutableClock;
    }

}