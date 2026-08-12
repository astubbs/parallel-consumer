package bz.stub.parallelconsumer.examples.reactor;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.examples.support.ConcurrencyObserver;
import bz.stub.parallelconsumer.examples.support.RunSummary;
import bz.stub.parallelconsumer.reactor.ReactorProcessor;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.reactivestreams.Publisher;
import pl.tlinkowski.unij.api.UniLists;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

/**
 * Smart-meter telemetry: a day of half-hourly meter readings, ingested into a reactive time-series
 * downstream that signals how much it is willing to take.
 * <p>
 * A UK electricity supplier settles consumption in {@value #SETTLEMENT_PERIODS_PER_DAY} half-hourly
 * <em>settlement periods</em> - a real day of the energy market's own clock, not a number invented for an
 * example. One Kafka record is one meter's day: keyed by meter id ("MPAN" in the industry's language),
 * carrying all {@value #SETTLEMENT_PERIODS_PER_DAY} periods. So a record is not one unit of work, it is
 * forty-eight of them, and the downstream store accepts them at its own pace.
 * <p>
 * <b>That is why this example exists, and why it is a {@link Flux}.</b> There are two independent bounds
 * here and readers routinely conflate them:
 * <ul>
 *     <li>{@link ParallelConsumerOptions#getMaxConcurrency()} is the <b>outer</b> bound - how many meter
 *     readings Parallel Consumer has in flight <em>across</em> meters at once. Under
 *     {@link ParallelConsumerOptions.ProcessingOrder#KEY} ordering the distinct meter count caps it
 *     further, and a meter's own days are never applied out of order.</li>
 *     <li>{@link Flux#limitRate(int)} is the <b>inner</b> bound - how many settlement periods are in
 *     flight <em>within</em> one reading. It is a Reactive Streams demand signal, and it is a
 *     {@code Flux} operator: a single-element publisher has no demand to throttle, which is exactly why
 *     each record expands into its periods rather than being processed whole.</li>
 * </ul>
 * That is the honest scope of the claim. Parallel Consumer does not do backpressure for you; it hands
 * your function a record and subscribes to whatever publisher you return, with unbounded demand. The
 * throttling below is yours, inside one record's stream.
 * <p>
 * <b>A multi-element publisher must be reduced to exactly one element before it is returned.</b>
 * {@link bz.stub.parallelconsumer.reactor.ReactorProcessor#react} subscribes with
 * {@code subscribe(onNext, onError)}, so it reads <em>every</em> {@code onNext} as "this record is done".
 * Return the 48-element {@code Flux} as it stands and one offset is reported succeeded 48 times: Parallel
 * Consumer's assertion in {@code PartitionState.onSuccess} trips and offsets stop committing. The
 * {@code count()} at the end of the chain is what makes the record's completion a single signal - and it
 * says something useful while it is there, namely how many settlement periods were stored.
 * <p>
 * <b>{@code doOnRequest} sits UPSTREAM of {@code limitRate}, and the order is the whole point.</b>
 * Downstream of {@code limitRate} the callback reports {@code request(unbounded)} forever - it is
 * watching Parallel Consumer's subscription, not the throttle - and the example silently teaches
 * nothing. Upstream it reports {@code request(}{@value #DEMAND_WINDOW}{@code )} and then the
 * replenishments, which is the demand actually flowing. {@code MeterTelemetryAppTest} asserts on exactly
 * that, so the mistake cannot come back unnoticed.
 * <p>
 * <b>The simulated latency is {@link Mono#delayElement(Duration)}, never a sleep.</b> A
 * {@code Thread.sleep} inside a {@code Mono} occupies a {@link Schedulers#parallel()} thread, which is
 * precisely the mistake BlockHound exists to catch. {@code delayElement} is timer-driven, and continues
 * the chain on {@code Schedulers.parallel()} - so this example's narrative is <em>demand</em>, not thread
 * count; the threads named in the logs are the reactive scheduler's, not a Parallel Consumer worker pool.
 * <p>
 * This sits beside {@link ReactorApp}, which answers "how do I call this?". This one answers "does it
 * actually do what it claims?" - so it logs every demand signal and prints a {@link RunSummary} when it
 * closes.
 * <p>
 * <b>What is scaffolding and what is the library.</b> Only the regions tagged {@code meterTelemetry} and
 * {@code meterTelemetryProcess} - the options builder and the {@code react} call - are Parallel Consumer.
 * {@link #openReading(MeterReading)}, {@link #recordDemand(MeterReading, long)},
 * {@link #ingestSample(MeterReading, MeterSample)},
 * {@link #finishReading(MeterReading)}, {@link ConcurrencyObserver} and {@link RunSummary} are this
 * example's own instrumentation, and live outside the tags on purpose: they come from a module that is
 * never published to Maven Central, so a snippet that referenced them would not compile in the reader's
 * project.
 */
@Slf4j
public class MeterTelemetryApp {

    /**
     * Half-hourly settlement periods in a UK electricity settlement day. The industry's own number, so a
     * reader recognises it rather than wondering where it came from.
     */
    public static final int SETTLEMENT_PERIODS_PER_DAY = 48;

    /**
     * What writing one settlement period into the time-series store costs. Simulated with
     * {@link Mono#delayElement(Duration)} - non-blocking and timer-driven - so a reading is about
     * {@value #SETTLEMENT_PERIODS_PER_DAY} x this, and the concurrency around it is real.
     */
    public static final Duration SAMPLE_INGEST_LATENCY = Duration.ofMillis(10);

    /**
     * The INNER bound: settlement periods in flight within one reading, i.e. four hours of the meter's
     * day at a time. What {@link Flux#limitRate(int)} requests from upstream, and therefore the number
     * {@code doOnRequest} reports - as long as it stays upstream of the throttle.
     */
    public static final int DEMAND_WINDOW = 8;

    /**
     * The OUTER bound: meter readings in flight across meters. Under
     * {@link ParallelConsumerOptions.ProcessingOrder#KEY} ordering the distinct
     * meter count is usually the smaller cap, which is what the run summary reports beside this.
     */
    public static final int MAX_CONCURRENCY = 32;

    /**
     * Separates a reading's settlement date from its periods: {@code <date>#<kWh>,<kWh>,...}. Not a regex
     * metacharacter, so it can be handed to {@link String#split(String, int)} as-is.
     */
    static final String SETTLEMENT_DATE_SEPARATOR = "#";

    /**
     * Separates the individual half-hourly consumption figures.
     */
    static final String SAMPLE_SEPARATOR = ",";

    /**
     * Category for the Reactive Streams trace {@link Flux#log} emits - {@code onSubscribe},
     * {@code request(n)}, {@code onNext}. A child of this package's logger, so a reader who turns the
     * package down to {@code warn} loses the trace with it.
     */
    public static final String DEMAND_TRACE = "bz.stub.parallelconsumer.examples.reactor.demand";

    /**
     * How {@code request(Long.MAX_VALUE)} appears in a demand log line - the same word Reactor's own
     * {@link Flux#log} uses, so the two traces read as one.
     */
    static final String UNBOUNDED = "unbounded";

    @Getter
    private final String inputTopic = "meter-readings";

    private final Map<String, String> envVars = System.getenv();

    /**
     * A consumer supplied via the constructor (e.g. a mock in tests), or {@code null} to build a real one
     * from the environment. Constructor injection rather than a package-private {@code getKafkaConsumer()}
     * the test overrides: the override seam relies on package-private access and breaks the moment a test
     * moves to another package.
     */
    private final Consumer<String, String> injectedConsumer;

    /**
     * How many readings were really in flight at once. Example scaffolding - see the class javadoc.
     */
    @Getter
    private final ConcurrencyObserver observer = new ConcurrencyObserver();

    /**
     * One open observation scope per reading being streamed, keyed by {@link MeterReading#getId()}.
     * Opened in {@link #beginReading(MeterReading)} and closed in {@link #finishReading(MeterReading)},
     * because a reading's scope spans a whole asynchronous {@link Flux} rather than one method call.
     */
    private final Map<String, ConcurrencyObserver.Scope> openScopes = new ConcurrentHashMap<>();

    /**
     * Every demand signal {@code doOnRequest} saw, in arrival order. The evidence that {@code limitRate}
     * is bounding the stream: all bounded, none {@link Long#MAX_VALUE}.
     */
    @Getter
    private final List<Long> demandSignals = new CopyOnWriteArrayList<>();

    /**
     * The threads the chain continued on <em>after</em> {@link Mono#delayElement(Duration)} - Reactor's
     * {@code parallel-*} scheduler. Named in the run's logging because this example's story is demand,
     * not a worker pool: these are not Parallel Consumer threads.
     */
    @Getter
    private final Set<String> ingestContinuationThreads = ConcurrentHashMap.newKeySet();

    private final Set<String> metersSeen = ConcurrentHashMap.newKeySet();

    private final Set<Integer> partitionsSeen = ConcurrentHashMap.newKeySet();

    private final AtomicLong readingsStreamed = new AtomicLong();

    private final AtomicLong samplesIngested = new AtomicLong();

    /**
     * The summary {@link #close()} printed, kept so a test can assert on exactly what was emitted rather
     * than on a second one built for the occasion. Empty until the app has closed, and empty afterwards
     * if it never streamed a reading.
     */
    @Getter
    private Optional<RunSummary> emittedSummary = Optional.empty();

    ReactorProcessor<String, String> parallelConsumer;

    public MeterTelemetryApp() {
        this(null);
    }

    protected MeterTelemetryApp(Consumer<String, String> injectedConsumer) {
        this.injectedConsumer = injectedConsumer;
    }

    /**
     * Builds the value one meter reading carries: a settlement date and all
     * {@value #SETTLEMENT_PERIODS_PER_DAY} half-hourly consumption figures for that day.
     */
    public static String readingValue(LocalDate settlementDate) {
        if (settlementDate == null) {
            throw new IllegalArgumentException("settlementDate must be set - a reading is one meter's day");
        }
        StringBuilder value = new StringBuilder(settlementDate.toString()).append(SETTLEMENT_DATE_SEPARATOR);
        for (int period = 1; period <= SETTLEMENT_PERIODS_PER_DAY; period++) {
            if (period > 1) {
                value.append(SAMPLE_SEPARATOR);
            }
            value.append(String.format(Locale.ROOT, "%.3f", consumptionFor(period)));
        }
        return value.toString();
    }

    /**
     * A domestic load profile in kWh: a low overnight base, a morning peak around 07:30 and a larger,
     * broader evening peak around 18:30. Deterministic, so two runs of the example see the same day.
     *
     * @param settlementPeriod 1..{@value #SETTLEMENT_PERIODS_PER_DAY}, period 1 being 00:00-00:30
     */
    static double consumptionFor(int settlementPeriod) {
        double hourOfDay = (settlementPeriod - 1) / 2.0d;
        double overnightBase = 0.08d;
        double morningPeak = 0.45d * Math.exp(-Math.pow(hourOfDay - 7.5d, 2) / 2.0d);
        double eveningPeak = 0.80d * Math.exp(-Math.pow(hourOfDay - 18.5d, 2) / 4.0d);
        return overnightBase + morningPeak + eveningPeak;
    }

    /**
     * The setup half of the README snippet, tagged {@code meterTelemetry}. The {@code react} call carries
     * a tag of its own, {@code meterTelemetryProcess}, and the README includes the two as separate blocks
     * in that order: the generator (asciidoc-template-maven-plugin) reads only the FIRST region of any
     * given tag name, so a second region sharing this tag would be silently dropped rather than
     * concatenated.
     */
    @SuppressWarnings("FeatureEnvy")
    ReactorProcessor<String, String> setupParallelConsumer() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();

        // tag::meterTelemetry[]
        var options = ParallelConsumerOptions.<String, String>builder()
                // Readings are keyed by meter id, and a meter's settlement days must be applied in order:
                // yesterday's corrected reading must not land after today's. KEY ordering guarantees that
                // - at most one reading per meter in flight, in offset order - while readings for
                // DIFFERENT meters still stream in parallel.
                .ordering(KEY) // <1>
                // The OUTER bound: meter readings in flight ACROSS meters. The INNER bound - settlement
                // periods in flight WITHIN one reading - is limitRate, below. They are independent, and
                // conflating them is the usual misreading of a "backpressure" example.
                .maxConcurrency(MAX_CONCURRENCY) // <2>
                .consumer(kafkaConsumer)
                .build();

        ReactorProcessor<String, String> telemetryProcessor = new ReactorProcessor<>(options);

        telemetryProcessor.subscribe(UniLists.of(inputTopic));

        return telemetryProcessor;
        // end::meterTelemetry[]
    }

    @SuppressWarnings("UnqualifiedFieldAccess")
    public void run() {
        this.parallelConsumer = setupParallelConsumer();

        postSetup();

        // tag::meterTelemetryProcess[]
        // react() is the ONLY entry point on ReactorProcessor - never call the core poll* methods on it.
        // It subscribes to the publisher you return, once per record, with UNBOUNDED demand. So any
        // throttling within a record is yours to declare, which is what limitRate does below.
        parallelConsumer.react(context -> { // <1>
            ConsumerRecord<String, String> record = context.getSingleRecord().getConsumerRecord();
            MeterReading reading = MeterReading.parse(record.key(), record.partition(), record.value());

            // openReading / recordDemand / ingestSample / finishReading are THIS EXAMPLE's scaffolding -
            // the concurrency observation and the per-reading logging - and deliberately live outside
            // this region.
            return openReading(reading)
                    // One record is one meter's DAY, so it expands into its 48 half-hourly settlement
                    // periods. A single-element publisher would have no demand to throttle - and
                    // limitRate is a Flux operator, so it would not even compile on a Mono.
                    .thenMany(Flux.fromIterable(reading.getSamples())) // <2>
                    // UPSTREAM of limitRate, and that is the whole point: here the callback sees the
                    // demand limitRate is actually issuing. Move it below limitRate and it reports
                    // request(unbounded) forever - Parallel Consumer's own subscription - and this
                    // example would silently demonstrate nothing.
                    .doOnRequest(demand -> recordDemand(reading, demand)) // <3>
                    .log(DEMAND_TRACE, Level.INFO, SignalType.ON_SUBSCRIBE, SignalType.REQUEST, SignalType.ON_NEXT)
                    // The INNER bound: eight settlement periods - four hours of the meter's day - are
                    // requested at a time, then replenished as they are consumed.
                    .limitRate(DEMAND_WINDOW) // <4>
                    // concatMap, not flatMap: a meter's own periods are written in time order.
                    .concatMap(sample -> ingestSample(reading, sample))
                    // Before the terminal signal reaches Parallel Consumer, so this reading is closed out
                    // before the next reading for the same meter can be dispatched.
                    .doOnTerminate(() -> finishReading(reading))
                    // REDUCE TO ONE ELEMENT, or the record is completed once PER SAMPLE. ReactorProcessor
                    // subscribes with subscribe(onNext, onError) and treats every onNext as "this record
                    // is done", so a 48-element Flux reports the same offset succeeded 48 times: Parallel
                    // Consumer's own assertion in PartitionState.onSuccess trips and offsets stop
                    // committing. count() gives the subscriber exactly one signal - and it is the number
                    // of settlement periods stored, which is what the record's acknowledgement should say.
                    .count(); // <5>
        });
        // end::meterTelemetryProcess[]
    }

    /**
     * Hook for tests to finish wiring their mock consumer once the processor has subscribed.
     */
    protected void postSetup() {
        // nothing to do when running against a real broker
    }

    /**
     * Opens one reading's stream, with this example's instrumentation around it.
     * <p>
     * Outside the tagged region on purpose (see the class javadoc). <b>The work happens on
     * {@link Schedulers#boundedElastic()}, inside the returned publisher</b>, which is the only safe place
     * for anything that might park a thread: Parallel Consumer forces this engine's dispatch pool to a
     * single thread, and {@link Mono#delayElement(Duration)} continues on {@link Schedulers#parallel()},
     * which is sized to the core count. A test's concurrency barrier therefore goes here, by overriding
     * {@link #beginReading(MeterReading)}, and nowhere else.
     */
    protected Mono<Void> openReading(MeterReading reading) {
        return Mono.<Void>fromRunnable(() -> beginReading(reading))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Marks a reading as in flight. Overridable so a test can watch - and gate - what happens at the
     * start of a reading's stream without the example having to grow a test hook of its own.
     */
    protected void beginReading(MeterReading reading) {
        openScopes.put(reading.getId(), observer.enter());
        metersSeen.add(reading.getMeterId());
        partitionsSeen.add(reading.getPartition());
        log.info("[{}] meter {} settlement day {} opened - {} reading(s) now in flight, {} settlement periods to ingest",
                Thread.currentThread().getName(), reading.getMeterId(), reading.getSettlementDate(),
                observer.getInFlight(), reading.getSamples().size());
    }

    /**
     * What {@code doOnRequest} saw. Reports the demand in the same words Reactor's own {@link Flux#log}
     * uses, so a reader comparing the two traces is comparing like with like - and so the test can assert
     * that the bounded figure appears and {@value #UNBOUNDED} does not.
     */
    protected void recordDemand(MeterReading reading, long demand) {
        demandSignals.add(demand);
        log.info("[{}] meter {} - demand signal: request({}) - the INNER bound, limitRate({}) within one reading",
                Thread.currentThread().getName(), reading.getMeterId(), renderDemand(demand), DEMAND_WINDOW);
    }

    /**
     * Writes one settlement period into the time-series store.
     * <p>
     * The latency is {@link Mono#delayElement(Duration)} - <b>never</b> {@code Thread.sleep}, which inside
     * a {@code Mono} would occupy a {@link Schedulers#parallel()} thread and is exactly what BlockHound
     * exists to catch. {@code delayElement} also moves the continuation onto {@code Schedulers.parallel()},
     * which is why the thread names change part-way through a reading.
     */
    protected Mono<String> ingestSample(MeterReading reading, MeterSample sample) {
        return Mono.fromCallable(() -> writeToTimeSeriesStore(reading, sample))
                .delayElement(SAMPLE_INGEST_LATENCY)
                .doOnNext(acknowledgement -> onSampleIngested(reading, sample));
    }

    /**
     * Closes a reading's observation scope, whatever the stream's terminal signal was. Overridable
     * alongside {@link #beginReading(MeterReading)} so a test can track per-meter in-flight counts.
     * <p>
     * Idempotent: the scope's removal is the guard, so a second terminal signal cannot double-count the
     * reading or drive the in-flight count negative.
     */
    protected void finishReading(MeterReading reading) {
        ConcurrencyObserver.Scope scope = openScopes.remove(reading.getId());
        if (scope == null) {
            return;
        }
        scope.close();
        readingsStreamed.incrementAndGet();
        log.info("[{}] meter {} settlement day {} ingested - {} settlement periods, {} reading(s) still in flight",
                Thread.currentThread().getName(), reading.getMeterId(), reading.getSettlementDate(),
                reading.getSamples().size(), observer.getInFlight());
    }

    /**
     * Drains and closes the processor, then prints the run summary - which is the only place this example
     * reports numbers, and reports them as a demonstration rather than as a benchmark.
     */
    public void close() {
        if (parallelConsumer != null) {
            parallelConsumer.closeDrainFirst();
        }
        logDemandBounds();
        this.emittedSummary = buildRunSummary();
        emittedSummary.ifPresent(RunSummary::log);
    }

    /**
     * States the two bounds in one line, because the run summary can only report the outer one and a
     * reader who takes "peak in-flight" for the whole story has learnt the wrong lesson.
     */
    private void logDemandBounds() {
        log.info("Demand bounds - OUTER: maxConcurrency({}) caps meter READINGS in flight across meters "
                        + "(KEY ordering caps it further at the distinct meter count); "
                        + "INNER: limitRate({}) caps SETTLEMENT PERIODS in flight within one reading, of {} per reading. "
                        + "{} period(s) ingested in total, continuing on Reactor scheduler thread(s) {}",
                MAX_CONCURRENCY, DEMAND_WINDOW, SETTLEMENT_PERIODS_PER_DAY,
                samplesIngested.get(), ingestContinuationThreads);
    }

    /**
     * The summary's partition and meter counts are what was actually OBSERVED, not what was configured: a
     * summary that reported a configured ceiling the data never contained would be describing a run that
     * did not happen.
     */
    private Optional<RunSummary> buildRunSummary() {
        long readings = readingsStreamed.get();
        if (readings < 1) {
            log.info("Meter telemetry closed without streaming a reading - nothing to summarise");
            return Optional.empty();
        }
        return Optional.of(RunSummary.builder()
                .exampleName("Smart-meter telemetry (reactor)")
                .recordCount(readings)
                .partitionCount(partitionsSeen.size())
                .distinctKeys(metersSeen.size())
                .maxConcurrency(MAX_CONCURRENCY)
                .ordering(KEY)
                // A reading is 48 settlement periods, each delayed - so this is what ONE record costs
                .simulatedLatency(SAMPLE_INGEST_LATENCY.multipliedBy(SETTLEMENT_PERIODS_PER_DAY))
                .observer(observer)
                .build());
    }

    /**
     * Settlement periods ingested, i.e. individual half-hourly figures written to the store.
     */
    public long getSamplesIngested() {
        return samplesIngested.get();
    }

    static String renderDemand(long demand) {
        return demand == Long.MAX_VALUE ? UNBOUNDED : Long.toString(demand);
    }

    private String writeToTimeSeriesStore(MeterReading reading, MeterSample sample) {
        // Stands in for the store's write call. Pure and non-blocking - the cost is the delayElement in
        // ingestSample, so that nothing here can park the scheduler thread it happens to run on.
        return reading.getMeterId() + "/" + reading.getSettlementDate() + "/" + sample.getSettlementPeriod();
    }

    private void onSampleIngested(MeterReading reading, MeterSample sample) {
        samplesIngested.incrementAndGet();
        // The continuation after delayElement runs on Schedulers.parallel(), NOT on a Parallel Consumer
        // worker - this example's story is demand, not thread count, and the names should say so.
        ingestContinuationThreads.add(Thread.currentThread().getName());
        log.debug("[{}] meter {} period {} ({} kWh) written",
                Thread.currentThread().getName(), reading.getMeterId(),
                sample.getSettlementPeriod(), sample.getKilowattHours());
    }

    private Consumer<String, String> getKafkaConsumer() {
        if (injectedConsumer != null) {
            return injectedConsumer;
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envVars.getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, envVars.getOrDefault("GROUP_ID", "meter-telemetry"));
        // Parallel Consumer commits the offsets itself, once the reading's periods have all been ingested
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    /**
     * One meter's settlement day: the meter id the record was keyed by, the day it covers, and all
     * {@value #SETTLEMENT_PERIODS_PER_DAY} half-hourly figures for it.
     */
    @Value
    public static class MeterReading {

        /**
         * The record's key - an MPAN in the industry's language. KEY ordering is what stops one meter's
         * days being applied out of order.
         */
        String meterId;

        /**
         * The partition the reading arrived on. Carried so the run summary can report the partition count
         * "peak in-flight" has to be read against.
         */
        int partition;

        String settlementDate;

        List<MeterSample> samples;

        /**
         * Parses {@code <date>#<kWh>,<kWh>,...} into a reading.
         *
         * @throws IllegalArgumentException if the value is not a settlement date followed by exactly
         *                                  {@value #SETTLEMENT_PERIODS_PER_DAY} consumption figures
         */
        public static MeterReading parse(String meterId, int partition, String value) {
            if (meterId == null || meterId.trim().isEmpty()) {
                throw new IllegalArgumentException("a meter reading must be keyed by its meter id");
            }
            if (value == null) {
                throw new IllegalArgumentException("meter " + meterId + " sent a reading with no value");
            }
            String[] fields = value.split(SETTLEMENT_DATE_SEPARATOR, 2);
            if (fields.length != 2 || fields[0].trim().isEmpty()) {
                throw new IllegalArgumentException("a meter reading's value must be '<settlementDate>"
                        + SETTLEMENT_DATE_SEPARATOR + "<kWh>,<kWh>,...', was: '" + value + "'");
            }
            String[] figures = fields[1].split(SAMPLE_SEPARATOR);
            if (figures.length != SETTLEMENT_PERIODS_PER_DAY) {
                throw new IllegalArgumentException("a settlement day is " + SETTLEMENT_PERIODS_PER_DAY
                        + " half-hourly periods, meter " + meterId + " sent " + figures.length);
            }
            List<MeterSample> samples = new ArrayList<>(SETTLEMENT_PERIODS_PER_DAY);
            for (int index = 0; index < figures.length; index++) {
                samples.add(new MeterSample(index + 1, parseKilowattHours(meterId, figures[index])));
            }
            return new MeterReading(meterId, partition, fields[0], UniLists.copyOf(samples));
        }

        private static double parseKilowattHours(String meterId, String figure) {
            try {
                return Double.parseDouble(figure.trim());
            } catch (NumberFormatException e) {
                // Classified, never swallowed: a malformed figure is a data problem the operator must see,
                // and it reaches Parallel Consumer as an ordinary user-function failure.
                throw new IllegalArgumentException("meter " + meterId + " sent a non-numeric consumption "
                        + "figure: '" + figure + "'", e);
            }
        }

        /**
         * Identifies this reading among the readings in flight - a meter and the day it covers.
         */
        public String getId() {
            return meterId + "/" + settlementDate;
        }
    }

    /**
     * One half-hourly settlement period of a meter's day.
     */
    @Value
    public static class MeterSample {

        /**
         * 1..{@value #SETTLEMENT_PERIODS_PER_DAY}, period 1 being 00:00-00:30.
         */
        int settlementPeriod;

        double kilowattHours;
    }
}
