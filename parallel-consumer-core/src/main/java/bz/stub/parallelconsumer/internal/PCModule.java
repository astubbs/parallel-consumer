package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.AdmissionController;
import bz.stub.parallelconsumer.internal.utils.TimeUtils;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.Setter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

import java.time.Clock;

/**
 * Minimum dependency injection system, modled on how Dagger works.
 * <p>
 * Note: Not using Dagger as PC has a zero dependency policy, and franky it would be overkill for our needs.
 *
 * @author Antony Stubbs
 */
public class PCModule<K, V> {

    protected ParallelConsumerOptions<K, V> optionsInstance;

    @Setter
    protected AbstractParallelEoSStreamProcessor<K, V> parallelEoSStreamProcessor;

    public PCModule(ParallelConsumerOptions<K, V> options) {
        this.optionsInstance = options;
    }

    public ParallelConsumerOptions<K, V> options() {
        return optionsInstance;
    }

    private ProducerWrapper<K, V> producerWrapper;

    protected ProducerWrapper<K, V> producerWrap() {
        if (this.producerWrapper == null) {
            this.producerWrapper = new ProducerWrapper<>(options());
        }
        return producerWrapper;
    }

    private ProducerManager<K, V> producerManager;

    protected ProducerManager<K, V> producerManager() {
        if (producerManager == null) {
            this.producerManager = new ProducerManager<>(producerWrap(), consumerManager(), workManager(), options());
        }
        return producerManager;
    }

    public Producer<K, V> producer() {
        return optionsInstance.getProducer();
    }

    public Consumer<K, V> consumer() {
        return optionsInstance.getConsumer();
    }

    private ConsumerManager<K, V> consumerManager;

    protected ConsumerManager<K, V> consumerManager() {
        if (consumerManager == null) {
            consumerManager = new ConsumerManager<>(optionsInstance.getConsumer(),
                    optionsInstance.getOffsetCommitTimeout(),
                    optionsInstance.getSaslAuthenticationRetryTimeout(),
                    optionsInstance.getSaslAuthenticationExceptionRetryBackoff());
        }
        return consumerManager;
    }

    @Setter
    private WorkManager<K, V> workManager;

    public WorkManager<K, V> workManager() {
        if (workManager == null) {
            workManager = new WorkManager<>(this, dynamicExtraLoadFactor());
        }
        return workManager;
    }

    protected AbstractParallelEoSStreamProcessor<K, V> pc() {
        if (parallelEoSStreamProcessor == null) {
            parallelEoSStreamProcessor = new ParallelEoSStreamProcessor<>(options(), this);
        }
        return parallelEoSStreamProcessor;
    }

    private DynamicLoadFactor dynamicLoadFactor;

    protected DynamicLoadFactor dynamicExtraLoadFactor() {
        if (dynamicLoadFactor == null) {
            dynamicLoadFactor = initDynamicLoadFactor();
        }
        return dynamicLoadFactor;
    }

    private BrokerPollSystem<K, V> brokerPollSystem;

    protected BrokerPollSystem<K, V> brokerPoller(AbstractParallelEoSStreamProcessor<K, V> pc) {
        if (brokerPollSystem == null) {
            brokerPollSystem = new BrokerPollSystem<>(consumerManager(), workManager(), pc, options());
        }
        return brokerPollSystem;
    }

    public Clock clock() {
        return TimeUtils.getClock();
    }

    private PCMetrics pcMetrics;

    public PCMetrics pcMetrics() {
        if (pcMetrics == null) {
            pcMetrics = new PCMetrics(options().getMeterRegistry(), optionsInstance.getMetricsTags(), optionsInstance.getPcInstanceTag());
        }
        return pcMetrics;
    }

    private AdmissionController admissionController;

    /**
     * The adaptive-admission controller - always constructed, whatever the
     * {@link ParallelConsumerOptions#getAdaptiveConcurrencyMode() mode}: in {@code DISABLED} it is inert (no window,
     * no law, static target), which keeps every downstream read unconditional - the same
     * cheap-always-construct choice {@link #dynamicExtraLoadFactor()} makes. Whether adaptive concurrency is
     * ACTIVE (mode requested AND the engine can serve it) stays the processor's call
     * ({@code AbstractParallelEoSStreamProcessor#isAdaptiveConcurrencyActive()}); the controller's own DISABLED
     * guard is defensive depth.
     */
    public AdmissionController admissionController() {
        if (admissionController == null) {
            admissionController = initAdmissionController();
        }
        return admissionController;
    }

    private AdmissionController initAdmissionController() {
        return new AdmissionController(options(), clock());
    }

    /**
     * Whether the admission controller's INPUT signals should flow at all: adaptive concurrency resolved ACTIVE on
     * the attached processor ({@code AbstractParallelEoSStreamProcessor#isAdaptiveConcurrencyActive()} - true under
     * {@code OBSERVE} too, signals flowing without acting being that mode's whole point). False in {@code DISABLED},
     * on an engine that refused the mode (where the controller is NOT inert, so the DISABLED guard alone would not
     * stop accumulation), and with no processor attached yet (bare-{@code WorkManager} test envs).
     * <p>
     * The gate for every signal tap outside the processor itself - the processor's own taps read its field
     * directly. Reads the processor <em>field</em> for the same reason {@link #admissionTargetRecords()} does.
     */
    public boolean adaptiveSignalsActive() {
        AbstractParallelEoSStreamProcessor<K, V> processor = parallelEoSStreamProcessor;
        return processor != null && processor.isAdaptiveConcurrencyActive();
    }

    /**
     * The record-denominated admission target the dispatch chain and the poller gate consume - the one seam through
     * which the LIVE target reaches the arithmetic (the plan's KTD1,
     * {@code docs/plans/2026-08-18-001-feat-self-scaling-concurrency-plan.md}).
     * <p>
     * When adaptive enforcement is active
     * ({@code AbstractParallelEoSStreamProcessor#adaptiveEnforcementActive()} - the processor stays the single owner
     * of the mode-versus-capability decision) this is the controller's live target in slots times the batch size.
     * Everywhere else - {@code DISABLED}, {@code OBSERVE}, an engine that refused the mode, or no processor attached
     * yet (bare-{@code WorkManager} test envs) - it is exactly today's static derivation,
     * {@link ParallelConsumerOptions#getTargetAmountOfRecordsInFlight()}.
     * <p>
     * Reads the processor <em>field</em> deliberately: {@link #pc()} would construct a whole processor on a module
     * that never had one.
     */
    public int admissionTargetRecords() {
        AbstractParallelEoSStreamProcessor<K, V> processor = parallelEoSStreamProcessor;
        if (processor != null && processor.adaptiveEnforcementActive()) {
            return admissionController().currentTarget() * options().getBatchSize();
        }
        return options().getTargetAmountOfRecordsInFlight();
    }

    private DynamicLoadFactor initDynamicLoadFactor() {
        if (options().getMessageBufferSize() > 0) {
            int staticLoadFactor = (options().getMessageBufferSize() / options().getTargetAmountOfRecordsInFlight()) + (options().getMessageBufferSize() % options().getTargetAmountOfRecordsInFlight() == 0 ? 0 : 1);
            // Initial == maximum on purpose: the user asked for a fixed buffer, so the factor must not drift off it.
            // The consequence is that DynamicLoadFactor#isMaxReached() is true from construction onwards - see
            // DynamicLoadFactor#isStatic(), which is how callers tell "saturated after climbing" apart from "never
            // able to move", and why AbstractParallelEoSStreamProcessor does not warn about this case.
            return new DynamicLoadFactor(staticLoadFactor, staticLoadFactor);
        } else {
            return new DynamicLoadFactor(options().initialLoadFactor, options().maximumLoadFactor);
        }
    }
}