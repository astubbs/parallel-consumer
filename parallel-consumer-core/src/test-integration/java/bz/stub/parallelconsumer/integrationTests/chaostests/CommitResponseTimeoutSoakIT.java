package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * <b>Soak, not a gate</b>: the first reproduction attempt for astubbs#177 (confluentinc#833) and
 * astubbs#175 (confluentinc#809) - {@code Timeout waiting for commit response}. Two field reports had
 * been open for months with no reproduction attempt at all;
 * {@code docs/inflight/bug-177-commit-response-timeout-unreproduced.md} owns the question, the
 * candidate mechanisms and the discriminator, and this class is the experiment it asks for. Read that
 * note before changing anything here - in particular before "fixing" the parts of this workload that
 * look pathological, because being pathological is the point.
 *
 * <h2>The shape, and why each term is the reporter's rather than ours</h2>
 * astubbs#177's reporter posted their actual application code, which is unusually specific, so the
 * workload here is a transcription rather than a guess:
 * <ul>
 *   <li>{@link ProcessingOrder#KEY} over {@link #KEY_SPACE} = 1000 distinct keys - their second
 *   publisher keys records {@code 0..999}, and it is that consumer their metrics and their exception
 *   came from.</li>
 *   <li><b>Half the records fail, and fail on EVERY attempt.</b> Their user function throws whenever
 *   a header flag is set, and the flag is set on {@code i % 2} of records - so a marked record is
 *   <em>permanently</em> poisoned, not flaky. Under {@code KEY} ordering that head-of-line blocks its
 *   key's shard for the rest of the run, and the partition's committed offset is pinned behind it.
 *   That is what makes this an ACCUMULATION - the reporter's "runs for a while and then exits" -
 *   rather than a startup race, and it is the single most load-bearing choice in the file. Modelling
 *   it as "fails ~50% of the time, per attempt" would let every record eventually succeed, the
 *   commit watermark would advance, and the accumulation being hunted would never happen.</li>
 *   <li>{@link CommitMode#PERIODIC_CONSUMER_SYNC} - the only mode that blocks a non-owning thread on
 *   {@code ConsumerOffsetCommitter}'s response queue, so the only mode in which this exception exists.
 *   astubbs#175's reporter names it explicitly, with a 1s commit interval, which
 *   {@code ManagedPCInstance} also sets.</li>
 *   <li>{@code maxConcurrency} 14 and a ~100ms user function - both theirs.</li>
 *   <li><b>One instance, and no churn.</b> Neither report involves a rebalance, so no
 *   {@link ChaosConductor} is wired here. This is the deliberate difference from every other class in
 *   this package: they hunt disturbance classes, this one hunts an accumulation under steady state.</li>
 * </ul>
 * The one thing deliberately NOT the reporter's is the clock. They produced 1000 records every two
 * minutes; {@link #BURST_INTERVAL} compresses that so the accumulation is reached inside a soak
 * rather than inside a working day.
 *
 * <h2>What it asserts - exactly one thing</h2>
 * That no {@code Timeout waiting for commit response} occurs. Nothing about throughput, nothing about
 * lag, and no {@link ProgressProbe}: this workload pins its own commit watermark BY DESIGN, so the
 * Class 2 lag detector would observe continuously and say nothing about the question. A probe firing
 * on the workload's intended behaviour is not evidence, and wiring one here would have manufactured
 * exactly the kind of noise
 * {@code docs/solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md}
 * describes.
 *
 * <h2>The discriminator, which is now in the product rather than in this test</h2>
 * astubbs#204 split the two mechanisms that produce one trace, so a reproduction classifies itself:
 * <ul>
 *   <li>a failure whose message says <b>the broker poll thread has died</b>, carrying the poller's own
 *   exception as its cause -> the poller DIED (astubbs#100's class, already fixed);</li>
 *   <li>a bare <b>{@code Timeout waiting for commit response}</b> -> the poller is <b>wedged but
 *   alive</b>, which is the uncharacterised defect nobody owns. Since astubbs#177's fix that message
 *   also carries {@code POLL THREAD AT TIMEOUT:} - {@code PollThreadStallDiagnosis}'s verdict on
 *   whether the poll thread is BLOCKED or merely SLOW, captured while it is still parked.</li>
 * </ul>
 * On top of that this scenario writes a <b>full JVM thread dump</b> to
 * {@code target/soak-177-threaddump-<epoch>.txt} at the moment of detection - the product's diagnosis
 * covers the poll thread, and the remaining question ("what is the control thread doing, and who holds
 * what") needs every thread.
 *
 * <h2>Calibration status, 2026-09-07 - two runs that measured the experiment rather than the defect</h2>
 * <b>2026-09-07, two runs, zero timeouts - and NEITHER is a sighting-ledger entry, because in both
 * the assertion was unfalsifiable for all but the first minute.</b> Common to both: 30 min, single
 * instance, {@code KEY}/{@code PERIODIC_CONSUMER_SYNC} at a 1s interval, {@value #KEY_SPACE} keys
 * over {@value #PARTITIONS} partitions, {@code maxConcurrency} 14, 100ms user function, 1000 records
 * every {@value #BURST_INTERVAL_SECONDS}s (90,000 produced), the suite's Testcontainers Kafka on
 * Docker, maintainer's macOS arm64 workstation.
 * <ul>
 *   <li><b>failureFraction 0.5</b> (the reporter's) - seed 3747722682837130843, succeeded 451,
 *   failed 237,006, no findings.</li>
 *   <li><b>failureFraction 0.03</b> - seed 5055695573431537469, succeeded 2,372, failed 81,114, no
 *   findings.</li>
 * </ul>
 * <b>What both runs actually measured is a total intake stall, not the absence of a timeout.</b>
 * Successes froze - at 451 and at 2,372 - inside the first ~60 seconds of each run and never moved
 * again across the remaining 29 minutes, while the producer kept publishing and the failure count
 * kept climbing at a rate that then held EXACTLY constant. A constant retry rate with a frozen
 * success count means no new record is being taken as work at all: the instance is not merely slowed
 * by head-of-line blocking, it has stopped.
 * <p>
 * <b>And a stalled instance cannot reach the exception being hunted.</b> Only
 * {@code PartitionState#onSuccess} calls {@code recordCompletion} ({@code setDirty}, before dirty
 * became derived) - {@code onFailure} in the same file is an
 * explicit no-op - and the control loop gates on {@code shouldTryCommitNow} =
 * {@code isTimeToCommitNow() && wm.isDirty() && !isRebalanceInProgress.get()}. With no success
 * anywhere, nothing is dirty, no commit request is enqueued, and
 * {@code ConsumerOffsetCommitter#commitAndWait} - the sole thrower of
 * {@value #COMMIT_RESPONSE_TIMEOUT} - is never entered. A green assertion here cannot tell "no
 * timeout occurred" from "no commit was attempted". This is the {@code dirty} asymmetry
 * {@code docs/inflight/upstream-tell-809-833-the-hang-is-fixed.md} names for this very workload.
 * <p>
 * <b>Lowering the failure fraction does NOT fix it - that arm has been run.</b> Dropping 0.5 to 0.03
 * bought about 1.5 extra bursts of throughput and then stalled identically, which rules out "too many
 * poisoned keys" as the explanation and makes the poisoned fraction the wrong knob. So is duration:
 * the stall is reached in the first minute of a thirty-minute run, and a longer run only adds
 * retry traffic to a stopped instance.
 * <p>
 * <b>What stops intake is NOT the documented offset-encoding back pressure.</b> That path logs on
 * every transition ({@code Offset map data too large}, {@code not allow further messages} in
 * {@code PartitionState#updateBlockFromEncodingResult}) and neither string appears once in either
 * run's log. The untested candidate is the load gate: {@code WorkManager#isSufficientlyLoaded}
 * compares {@code workable = inShards - parkedForRetry} against
 * {@code targetAmountOfRecordsInFlight * loadingFactor}, and {@code inShards} counts records queued
 * BEHIND a blocked shard head - records that can never be worked - while only the failing head itself
 * is {@code parkedForRetry}. A shard set full of unworkable queued records therefore reads as
 * "sufficiently loaded", the broker poller stays paused, and nothing ever arrives to change it. That
 * is the silent-stall shape the gate's own comment names against confluentinc#857. <b>It was a
 * hypothesis, and the three runs below settle it: half right.</b>
 *
 * <h2>Calibration status, 2026-09-08 - the load gate IS what stops intake, it does NOT need
 * head-of-line blocking, and it does NOT need a high failure rate</h2>
 * Four arms, each differing from the first by exactly one term, six minutes rather than thirty
 * because the stall is reached in the first second (the fourth runs ten, because its whole question
 * is <em>when</em> the latch arrives). Same seed {@code 3747722682837130843}, same
 * {@code failureFraction} 0.5, same 1000 keys over {@value #PARTITIONS} partitions,
 * {@code maxConcurrency} 14, 100ms user function, 1000 records every {@value #BURST_INTERVAL_SECONDS}s,
 * the suite's Testcontainers Kafka on Docker, maintainer's macOS arm64 workstation under 5-13 load
 * average. {@code WorkManager} at DEBUG throughout ({@code -Dpc.loadgate.log.level=debug}), and the
 * config that carried it verified in the log by {@code OnConsoleStatusListener} rather than assumed.
 * <ul>
 *   <li><b>Arm 1, the experimental arm - {@code KEY}.</b> Reproduced astubbs#471's thirty-minute
 *   result in six minutes and at the same number: succeeded froze at <b>451</b>, failed 47,977. The
 *   gate read {@code true} on 37,356 of 37,360 evaluations - the four {@code false} ones are the
 *   first 800ms, before the first fetch landed - and all 20 partitions were paused at every sample.
 *   It latched at {@code inShards=500 vs target(14)*loadingFactor(2)=28}, i.e. on the FIRST fetch,
 *   0.8s in, and never unlatched. {@code inShards} then pinned at <b>549 = 1000 - 451</b> for 36,749
 *   of the samples: one burst arrived, the non-poisoned part of it succeeded, and nothing was ever
 *   fetched again.</li>
 *   <li><b>Arm 2, the control on ORDERING - {@code UNORDERED}, everything else identical.</b> Under
 *   {@code UNORDERED} no shard head can block anything behind it, so every held record is genuinely
 *   selectable. <b>It stalled the same way</b>: gate {@code true} on 39,689 of 39,693 evaluations, 20
 *   partitions paused throughout, and successes crawling 232 -> 314 across six minutes on the
 *   records already in the buffer rather than on anything new.</li>
 *   <li><b>Arm 3, the control on the GATE ITSELF - {@code -Dsoak.messageBufferSize=20000}</b>, which
 *   moves the threshold from 42 to 20,006 and moves nothing else. <b>The outcome flips</b>: the gate
 *   read {@code false} on all 35,652 evaluations, <b>zero</b> partitions were paused at any sample,
 *   {@code inShards} climbed monotonically 549 -> 17,103 tracking the producer, and successes reached
 *   897 rather than freezing at 451.</li>
 *   <li><b>Arm 4, the control on the POISON RATE - {@code -Dsoak.failureFraction=0.01}</b>, ten
 *   minutes, ~10 poisoned records per 1000-record burst against a healthy stream of ~50/s. This is
 *   the arm that says the stall is not an artefact of the reporter's 50% rate. <b>The gate OSCILLATED
 *   and then stopped</b> - 264 {@code false} readings interleaved with {@code true}, against exactly
 *   four in each of arms 1 and 2, all of those at startup. Successes rose 992 -> 1,971 -> 2,946 ->
 *   3,902 while it oscillated, then froze at <b>3,902 for the remaining nine minutes</b>. The last
 *   {@code false} reads {@code inShards=71 - parkedForRetry=29 = 42 vs target(14)*loadingFactor(3)=42}
 *   - the boundary exactly - and the final unbroken {@code true} run is <b>8m57s</b>.</li>
 * </ul>
 * <b>Verdict, part one - what stops intake.</b> Arm 3 is the positive control: changing only the
 * gate's threshold changes only whether intake stops, so <b>the gate is what stops intake</b>. Arm 2
 * kills the stated mechanism: the stall is identical with no ordering constraint at all. Arm 1's own
 * arithmetic says the same thing more directly - 549 records over 1000 distinct keys, from ONE burst,
 * is at most one record per key, so <b>nothing was queued behind any blocked head</b>.
 * <p>
 * <b>Verdict, part two - the latch point, and why it is reached by any instance that runs long
 * enough.</b> The gate fires on {@code inShards - parkedForRetry > target * loadingFactor}, and
 * {@code parkedForRetry} is not a property of the population: by Little's law it is
 * {@code retry throughput * retryDelay}. So with {@code P} permanently-failing records held,
 *
 * <pre>{@code
 *   unparked  =  P - (retry throughput * retryDelay)
 *   latch when  unparked > targetAmountOfRecordsInFlight * loadingFactor
 * }</pre>
 *
 * {@code P} only grows under retry-forever while the subtracted term is BOUNDED - retry throughput
 * cannot exceed {@code maxConcurrency / userFunctionDuration}, so the parked term cannot exceed
 * {@code maxConcurrency * retryDelay / userFunctionDuration}, which at these defaults is
 * {@code 14 * 1000/100 = 140}. <b>The latch is therefore an eventual certainty for any instance with
 * retry-forever and any poison at all</b>, at a computable ceiling of {@code 140 + 42 = 182} held
 * poison records here. The two addends are different units on purpose: 140 bounds the parked share,
 * and <b>the 42 is the gate's own threshold term</b>, {@code target(14) * loadingFactor(3)} read off
 * the arms' gate lines, not a second measured population. {@code loadingFactor} is
 * {@link bz.stub.parallelconsumer.internal.DynamicLoadFactor#getCurrentFactor()}, which starts at 2
 * and steps up one at a time, so arm 1 latched against a threshold of 28 before it had stepped at
 * all. {@code docs/inflight/bug-119-load-gate-counts-blocked-work-as-available.md} owns why a factor
 * that had stepped further would latch later rather than never - the step-up is conditioned on the
 * work request being fulfilled, which the latch is precisely what prevents.
 * <p>
 * <b>Measured, not asserted, and the measurement beats the bound.</b> {@code parkedForRetry} has
 * median 135 and hard max <b>140</b> in arms 1, 2 and 3 while {@code inShards} ranges over 549 to
 * 17,103 - a 31x population change with an unchanged parked count, which is only possible if parked
 * is set by throughput rather than population. Arm 1's predicted {@code unparked} of
 * {@code 549 - 140 = 409} is exactly its observed minimum. But <b>the bound is not the arrival</b>:
 * arm 4 latched at {@code inShards} <b>98</b>, about 64 seconds in, and 182 was never approached.
 * That is because its retry throughput settled at 30.6/s, so its parked term was ~30 rather than 140,
 * and {@code 98 - 30} already cleared the threshold. <b>A SLOWER retry service latches the gate
 * SOONER</b>, because fewer records are in back-off and more therefore read as workable - which is
 * the opposite of the intuition, and it is why 182 is a ceiling rather than an estimate.
 * <p>
 * <b>Saturation is NOT a precondition, and this is the correction worth carrying.</b> At arm 4's
 * latch the pool was doing 30.6 failures/s = about 3 of its 14 workers, <b>22% utilisation</b> -
 * against arm 1's 95%. The instance stopped fetching from the broker while it was 78% idle. Whatever
 * limits the retry cadence to ~3.2s per record against a static 1s
 * {@link bz.stub.parallelconsumer.ParallelConsumerOptions#defaultMessageRetryDelay} (confirmed static
 * - no provider is set and there is no progressive backoff) is NOT measured here, and it is the first
 * arm below.
 * <p>
 * <b>Where head-of-line blocking DOES bite - a role it has, and a role it does not.</b> It is not
 * what latches the gate (arm 2). It is what stops the residue draining afterwards: arm 4's
 * {@code inShards} fell 103 -> 98 and then sat at exactly 98 for 6,201 consecutive evaluations,
 * nothing retiring for nine minutes. Its 98 residents are ~40 poison plus ~58 healthy records queued
 * behind poisoned heads on their own keys - at 1% over four bursts a key holds several records, where
 * arm 1's single burst gave each key exactly one. So eleven idle workers sat beside 58 deliverable
 * records they were not allowed to reach.
 * <p>
 * <b>Why no threshold and no counting rule fixes this.</b> Arm 3 lifts the intake bound and throughput
 * still dies: successes doubled and plateaued by minute three while {@code inShards} climbed linearly.
 * Removing the buffer bound converts a hard stall into an unbounded-memory slow starve. <b>The fix has
 * to bound the FAILURES rather than the buffer</b> - astubbs#149's dead letter queue. Full write-up,
 * the operator-visible shape and the interim mitigation:
 * {@code docs/inflight/bug-119-load-gate-counts-blocked-work-as-available.md}.
 *
 * <h2>Confirmation, 2026-09-09 - the latch now reports itself, and this arm is what proved it</h2>
 * The interim mitigation named above shipped in astubbs/parallel-consumer#497: a WARN on
 * {@code WorkManager}'s logger once the intake gate has read loaded across
 * {@code LATCHED_PASSES_BEFORE_WARNING} consecutive control-loop passes with no record retiring, and
 * a line when that clears. One confirming run of arm 1's shape - same seed
 * {@code 3747722682837130843}, {@code KEY}, {@code failureFraction} 0.5, on the same workstation,
 * shortened to {@code -Dsoak.duration=PT4M} because the latch arrives in the first second and this run
 * is a confirmation rather than a re-derivation, with {@code -Dpc.loadgate.log.level=info} so the
 * report is visible without the per-tick DEBUG equation.
 * <ul>
 *   <li><b>The arm reproduced.</b> {@code succeeded=451}, arm 1's number and astubbs#471's
 *   thirty-minute number, in four minutes.</li>
 *   <li><b>Exactly ONE WARN in the whole run</b>, about ten seconds after the run banner - the
 *   hundred passes at the measured latched cadence, as designed - and no clear line, because the
 *   latch never cleared. "Once, then quiet" holds over roughly forty times the reporting window.</li>
 *   <li><b>Its operands agree with the arms that derived them:</b>
 *   {@code inShards=549 parkedForRetry=138 workable=411 vs target(14)*loadingFactor(3)=42,
 *   pausedPartitions=20}. 549 is arm 1's pinned population, 138 sits inside the parked band arms 1-3
 *   measured, and every partition is paused.</li>
 * </ul>
 * <p>
 * <b>Still eliminated, re-measured on all four arms:</b> offset-encoding back pressure. Neither
 * {@code Offset map data too large} nor {@code not allow further messages} appears once in any of the
 * four logs.
 * <p>
 * <b>Arms not run, re-ordered by what these four runs established.</b> Each changes ONE term:
 * <ol>
 *   <li><b>Why is the retry cadence ~3.2s when the configured delay is 1s?</b> Arm 4's pool sat at 22%
 *   with ready records waiting, so something between "delay passed" and "dispatched" is rate-limiting,
 *   and it is what decides how early the latch arrives. Instrument the dispatch path rather than the
 *   gate. This is now the most valuable arm, because the latch point is a function of this number.</li>
 *   <li><b>Per-ATTEMPT failure instead of per-record</b>, so records eventually succeed, the shards
 *   drain, and the instance keeps committing for the whole run. On this evidence it is the only shape
 *   that keeps the commit path alive indefinitely, which promotes it from "a different mechanism" to
 *   "the first arm that can actually falsify the assertion".</li>
 *   <li><b>{@code gtassone}'s configuration from confluentinc#809</b> - 128 partitions, concurrency
 *   64, a user function from 100ms to minutes, {@code PERIODIC_CONSUMER_SYNC}. It is the closest
 *   recorded configuration to astubbs#175, the live report, and this scenario does not have it - the
 *   workload here transcribes the now-closed astubbs#177 instead, whose defect
 *   {@code upstream-tell-809-833-the-hang-is-fixed.md} says is already fixed.
 *   {@code docs/inflight/upstream-175-sporadic-commit-timeouts.md} no longer nominates it as a WEDGE
 *   candidate - astubbs#29 merged 2026-09-02 and closed the AB-BA cycle for that report - so this arm
 *   buys the configuration, not the cycle.</li>
 *   <li><s>{@code -Dsoak.failureFraction=0}</s> - <b>withdrawn, superseded by arm 4.</b> It was
 *   proposed as the control that removes the poison entirely, but arm 4 answers the question it was
 *   aimed at with a live workload rather than an empty one: the poisoned share is not the axis, and
 *   1% reaches the same terminal state as 50% - later, and with successes flowing until it does.</li>
 * </ol>
 * <b>The stall may be the more interesting lead than the timeout.</b> confluentinc#833's reporter
 * showed {@code pc_processed_records_total} FLAT across the window in which their timeout fired -
 * which is this state, not a busy one. Anyone picking this up should consider whether the reports'
 * timeout is a consequence of the stall rather than a peer of it.
 *
 * <h2>Running it</h2>
 * Tagged {@code @Tag("soak")}, which sits in {@code pom.xml}'s {@code excluded.groups} default, so it
 * is in NO suite - not the default build, not the gating integration lane, and NOT the chaos shards
 * (those select classes by name through {@code CHAOS_SCENARIOS}). It is opt-in only:
 * <pre>{@code
 * ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
 *     -Dincluded.groups=soak -Dexcluded.groups= -Dit.test=CommitResponseTimeoutSoakIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * }</pre>
 * {@code -Dfailsafe.failIfNoSpecifiedTests=false} is REQUIRED, not tidiness: {@code -am} builds the
 * parent module first, the named class is not in it, and failsafe fails the reactor there before core
 * is reached. {@code bin/chaos-test.sh}'s header owns the same trap for the chaos lane. Its cost is
 * that a run selecting NOTHING now exits 0, so read the {@code === SOAK ...} banner and the
 * {@code Soak summary:} line out of the log before believing a green - a soak that ran no test is not
 * a sighting-ledger entry.
 * <p>
 * <b>Not {@code bin/soak-test.sh}</b>, which is an unrelated tool that repeats a short test under CPU
 * load to measure a flake RATE. This lane is one long run of one scenario.
 * {@code -Dsoak.duration=PT45M} sets the run length, {@code -Dsoak.failureFraction=<0..1>} the
 * poisoned share, and {@code -Dchaos.seed=<long>} replays which records were poisoned - the seed is
 * logged at the top of every run and lifted into the ambient autopsy by {@link ChaosSeed.Holder}.
 */
@Tag("soak")
// Six hours, so a -Dsoak.duration far above the default is not silently killed by JUnit. The wait
// below is bounded by the requested duration, not by this - see docs/testing.md on why a killed run
// is uninterpretable rather than merely short.
@Timeout(value = 6, unit = TimeUnit.HOURS)
@Testcontainers
@Slf4j
class CommitResponseTimeoutSoakIT extends ChaosScenarioBase {

    /** astubbs#177's reporter keys records {@code 0..999}; this is that key space. */
    private static final int KEY_SPACE = 1_000;

    /**
     * Not the reporter's (they never said), so this is ours and chosen for one reason: enough
     * partitions that the single instance holds many independent commit watermarks, since a workload
     * that pins every one of them is a stronger version of the accumulation than one that pins a few.
     */
    private static final int PARTITIONS = 20;

    /** Their {@code maxConcurrency(14)}, verbatim. */
    private static final int MAX_CONCURRENCY = 14;

    /** Their {@code Thread.sleep(100)} inside the user function, verbatim. */
    private static final int POLL_DELAY_MS = 100;

    static final int BURST_INTERVAL_SECONDS = 20;

    /**
     * The reporter's {@code @Scheduled(fixedRate = 2, MINUTES)} burst of 1000, compressed 6x. The
     * compression is the ONE term here that is ours rather than theirs, and it is a compression of the
     * clock only: the burst size and key space are unchanged, so what arrives is the same shape at a
     * higher rate. It matters less than it looks - after the first two bursts essentially every key is
     * head-of-line blocked, so the produce rate stops setting the pace and the retry traffic does.
     */
    private static final Duration BURST_INTERVAL = Duration.ofSeconds(BURST_INTERVAL_SECONDS);

    private static final Duration DEFAULT_DURATION = Duration.ofMinutes(30);

    /** How often the watcher looks for a failed PC. Small, because the evidence it captures is a
     * thread dump and a parked thread is only interesting while it is still parked. */
    private static final Duration WATCH_INTERVAL = Duration.ofSeconds(5);

    private static final Duration DEFAULT_PROGRESS_LOG_INTERVAL = Duration.ofSeconds(60);

    /** The half of the timeout message that means the poller is WEDGED BUT ALIVE - the defect nobody owns. */
    static final String COMMIT_RESPONSE_TIMEOUT = "Timeout waiting for commit response";

    /** The half that means the poller DIED - astubbs#100's class, and self-identifying since astubbs#204. */
    static final String POLLER_DIED = "The broker poll thread has died";

    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong produced = new AtomicLong();

    private volatile boolean producing = true;

    @Test
    void noCommitResponseTimeoutUnderSustainedUserFunctionFailure() throws Exception {
        ChaosSeed seed = resolveSeed();
        Duration duration = resolveDuration();
        double failureFraction = resolveFailureFraction();
        ProcessingOrder ordering = resolveOrdering();
        int messageBufferSize = resolveMessageBufferSize();
        log.info("=== SOAK astubbs#177/astubbs#175 commit-response timeout: seed={} duration={} "
                        + "failureFraction={} ordering={} messageBufferSize={} keys={} partitions={} "
                        + "(replay: {} -Dsoak.duration={}) ===",
                seed.getValue(), duration, failureFraction, ordering, messageBufferSize, KEY_SPACE, PARTITIONS,
                seed.replayCommand().replace("-Dincluded.groups=chaos", "-Dincluded.groups=soak"), duration);

        String topic = getClass().getSimpleName() + "-" + RandomUtils.nextInt();
        ensureTopic(topic, PARTITIONS);

        ManagedPCInstance.Config config = ManagedPCInstance.Config.builder()
                .commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)
                .order(ordering)
                .inputTopic(topic)
                .pollDelayMs(POLL_DELAY_MS)
                .maxConcurrency(MAX_CONCURRENCY)
                .messageBufferSize(messageBufferSize)
                .build();

        ManagedPCInstance instance = new ManagedPCInstance(config, getKcu(), (incarnationId, context) -> {
            // Poisoned records throw on EVERY delivery - see the class javadoc on why per-record
            // beats per-attempt here. Decided from the record's identity and the seed, so a replay
            // poisons exactly the same records.
            if (isPoisoned(context.value(), seed.getValue(), failureFraction)) {
                failed.incrementAndGet();
                throw new RuntimeException("THROW_EXCEPTION_FLAG_HAPPENED for " + context.value());
            }
            succeeded.incrementAndGet();
        });

        ExecutorService pcExecutor = Executors.newWorkStealingPool();
        Thread producer = new Thread(() -> produceBursts(topic), "soak-177-producer");
        List<String> findings = new ArrayList<>();
        try {
            producer.start();
            instance.start(pcExecutor);
            findings = watchUntil(instance, Instant.now().plus(duration));
        } finally {
            producing = false;
            producer.join(30_000);
            instance.stop();
            pcExecutor.shutdownNow();
            log.info("Soak summary: seed={} produced={} succeeded={} failed={} findings={}",
                    seed.getValue(), produced.get(), succeeded.get(), failed.get(), findings);
        }

        assertWithMessage("astubbs#177/astubbs#175: no commit-response timeout, and no other terminal "
                        + "failure that would end the soak before its time (seed %s, replay with "
                        + "-Dchaos.seed=%s -Dsoak.duration=%s -Dsoak.failureFraction=%s). A finding here "
                        + "is classified in its own text - a bare '%s' is the WEDGED-BUT-ALIVE defect, a "
                        + "'%s' is astubbs#100's class",
                seed.getValue(), seed.getValue(), duration, failureFraction,
                COMMIT_RESPONSE_TIMEOUT, POLLER_DIED)
                .that(findings).isEmpty();
    }

    /**
     * Watch one instance until the soak's deadline, returning the findings - empty is the pass.
     * <p>
     * It returns rather than throwing on the first finding so the {@code finally} above still runs its
     * teardown and prints the summary, and so a run that ends early is reported as ONE thing with its
     * evidence attached rather than as whatever the teardown happened to throw next.
     */
    private List<String> watchUntil(ManagedPCInstance instance, Instant deadline) throws InterruptedException {
        List<String> findings = new ArrayList<>();
        Duration progressInterval = resolveProgressInterval();
        Instant nextProgressLog = Instant.now().plus(progressInterval);
        while (Instant.now().isBefore(deadline)) {
            ParallelEoSStreamProcessor<String, String> pc = instance.getParallelConsumer();
            if (pc != null) {
                Exception cause = pc.getFailureCause();
                if (cause != null) {
                    findings.add(classify(cause));
                    return findings;
                }
                if (pc.isClosedOrFailed()) {
                    // Closed with no cause recorded is not the 177 symptom, but it does mean the soak
                    // stopped soaking - reporting it as a finding stops a truncated run reading green.
                    findings.add("PC reported closed-or-failed with NO failure cause - the soak ended "
                            + "early for a reason this scenario cannot name, so it did not run its "
                            + "duration and proves nothing about astubbs#177");
                    return findings;
                }
            }
            if (Instant.now().isAfter(nextProgressLog)) {
                log.info("Soak progress: remaining={} produced={} succeeded={} failed={} {}",
                        Duration.between(Instant.now(), deadline), produced.get(), succeeded.get(),
                        failed.get(), describeIntake(pc));
                nextProgressLog = Instant.now().plus(progressInterval);
            }
            Thread.sleep(WATCH_INTERVAL.toMillis());
        }
        return findings;
    }

    /**
     * The intake gate's state, on the progress line, so a run that freezes says WHY in the same place it
     * says it froze.
     * <p>
     * The two runs of 2026-09-07 recorded a total intake stall and could not name what stopped intake,
     * because the only figures on the progress line were the counters that had stopped moving. These are
     * the gate's own operands - the same ones {@code WorkManager#isSufficientlyLoaded} prints at DEBUG,
     * read from the same {@code getWorkableRecords()} accessor - plus Kafka's paused-partition count,
     * which is what a latched gate actually DOES. A frozen success count beside
     * {@code loaded=true pausedPartitions=<all of them>} is the gate holding the poller down; a frozen
     * success count beside {@code loaded=false pausedPartitions=0} is something else entirely, and the
     * point of the line is that those two are no longer indistinguishable after the fact.
     * <p>
     * At INFO deliberately: it is one line per progress tick, it is the harness narrating rather than the
     * product, and the DEBUG stream it duplicates is behind {@code -Dpc.loadgate.log.level=debug} because
     * it fires per control-loop tick. This one is always in the log of every soak that ever runs.
     */
    private static String describeIntake(ParallelEoSStreamProcessor<String, String> pc) {
        if (pc == null) {
            return "intake=UNAVAILABLE (no PC yet)";
        }
        try {
            var wm = pc.getWm();
            var records = wm.getSm().getWorkableRecords();
            return String.format("intake(loaded=%s workable=%d = inShards=%d - parkedForRetry=%d; target=%d; "
                            + "pausedPartitions=%d)",
                    wm.isSufficientlyLoaded(), records.getWorkable(), records.getInShards(),
                    records.getParkedForRetry(), wm.getOptions().getTargetAmountOfRecordsInFlight(),
                    pc.getPausedPartitionSize());
        } catch (RuntimeException e) {
            // Never let the narration take down the soak it is narrating - the same rule captureThreadDump
            // below follows.
            return "intake=UNAVAILABLE (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ")";
        }
    }

    /**
     * Turn a terminal failure into the finding text, and capture the evidence that stops being
     * available a moment later. The classification is read out of the product's own message - see the
     * class javadoc: astubbs#204 made the two mechanisms say which they are, so this method reports a
     * verdict rather than reaching one.
     */
    private String classify(Exception cause) {
        String rendered = renderCauseChain(cause);
        String dump = captureThreadDump();
        String classification;
        if (rendered.contains(POLLER_DIED)) {
            classification = "POLLER DIED (astubbs#100's class - the cause chain names what killed it)";
        } else if (rendered.contains(COMMIT_RESPONSE_TIMEOUT)) {
            classification = "POLLER WEDGED BUT ALIVE (the uncharacterised defect - read the "
                    + "'POLL THREAD AT TIMEOUT' verdict in the message for blocked vs merely slow)";
        } else {
            classification = "NOT the astubbs#177 symptom - the soak ended early for another reason, "
                    + "so this run proves nothing about the reports";
        }
        String finding = classification + "; thread dump: " + dump + "; failure: " + rendered;
        log.error("=== SOAK FINDING === {}", finding);
        return finding;
    }

    private static String renderCauseChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (sb.length() > 0) {
                sb.append(" <- caused by: ");
            }
            sb.append(current.getClass().getName()).append(": ").append(current.getMessage());
            if (current.getCause() == current) {
                break;
            }
        }
        return sb.toString();
    }

    /**
     * A full JVM thread dump, written beside the build output. {@code PollThreadStallDiagnosis}
     * already reports the POLL thread inside the exception message; what it cannot report is the
     * control thread, the worker pool and who holds what, which is the half needed to tell a
     * lock-ordering defect from a broker that simply stopped answering.
     *
     * @return the path written, or a description of why nothing was
     */
    private static String captureThreadDump() {
        try {
            Path path = Paths.get("target", "soak-177-threaddump-" + Instant.now().toEpochMilli() + ".txt");
            Files.createDirectories(path.getParent());
            StringBuilder sb = new StringBuilder();
            for (ThreadInfo info : ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
                sb.append(info);
            }
            Files.write(path, sb.toString().getBytes(StandardCharsets.UTF_8));
            return path.toAbsolutePath().toString();
        } catch (IOException | RuntimeException e) {
            // Never let the evidence capture replace the finding it was capturing.
            return "UNAVAILABLE (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ")";
        }
    }

    /**
     * Whether this record is one of the reporter's flagged ones. Deterministic in the record identity
     * and the seed, so {@code -Dchaos.seed} replays the same poisoning; the reporter's own selector was
     * {@code i % 2}, which is this at {@code failureFraction} 0.5 with the alternation smoothed out so
     * the poisoned set does not line up with any partitioning of the key space.
     */
    static boolean isPoisoned(String identity, long seed, double failureFraction) {
        if (failureFraction <= 0) {
            return false;
        }
        int index = Integer.parseInt(identity.substring(identity.indexOf('-') + 1));
        // A cheap avalanche mix, so neighbouring indexes do not land in the same half.
        long mixed = (index * 0x9E3779B97F4A7C15L) ^ seed;
        mixed ^= mixed >>> 33;
        mixed *= 0xFF51AFD7ED558CCDL;
        mixed ^= mixed >>> 33;
        return Math.floorMod(mixed, 1_000_000L) < (long) (failureFraction * 1_000_000L);
    }

    /**
     * The reporter's publisher: a burst of {@link #KEY_SPACE} records over keys {@code 0..999}, once
     * per {@link #BURST_INTERVAL}, for as long as the soak runs. Record VALUES carry the unique
     * identity (the keys repeat, so they cannot), which is the same split
     * {@link ChaosScenarioBase#identityFor} makes for {@code ChaosKeyOrderIT}.
     */
    private void produceBursts(String topic) {
        // produceRange collects the identities it sent, for a coverage check this scenario does not
        // make - it asserts one thing, and completeness is not it (half these records never complete
        // by design). Kept only because it is produceRange's contract; the set is never read.
        Set<String> unreadIdentities = new ConcurrentSkipListSet<>();
        int burst = 0;
        while (producing) {
            int from = burst * KEY_SPACE;
            produceRange(topic, from, from + KEY_SPACE, unreadIdentities);
            produced.addAndGet(KEY_SPACE);
            burst++;
            try {
                Thread.sleep(BURST_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** Repeated keys over {@link #KEY_SPACE} - the shard, and so the unit KEY ordering serialises. */
    @Override
    protected String keyFor(int i) {
        return "k-" + (i % KEY_SPACE);
    }

    /** The unique record identity, in the value - see {@link #produceBursts}. */
    @Override
    protected String identityFor(int i) {
        return "v-" + i;
    }

    /** {@code -Dsoak.duration=PT45M}; ISO-8601, because a bare number would not say of what. */
    private static Duration resolveDuration() {
        String property = System.getProperty("soak.duration");
        return property == null ? DEFAULT_DURATION : Duration.parse(property);
    }

    /** {@code -Dsoak.failureFraction=0} is the control arm - see the class javadoc's arm list. */
    private static double resolveFailureFraction() {
        String property = System.getProperty("soak.failureFraction");
        return property == null ? 0.5d : Double.parseDouble(property);
    }

    /**
     * {@code -Dsoak.ordering=UNORDERED} is the control arm for the head-of-line half of the intake-stall
     * hypothesis, and {@code KEY} - the reporter's - is the default.
     * <p>
     * It is a knob rather than a second test class because the two arms must differ by exactly ONE term:
     * same seed, same poisoned set, same key space, same burst clock. Under {@code UNORDERED} no shard
     * head can block anything queued behind it, so every record the shards hold is genuinely selectable
     * and {@code inShards} is an honest count of workable records. If the stall survives that, what
     * stops intake is the buffer filling with permanently-failing records, not head-of-line blocking -
     * which is a different defect with a different fix.
     */
    private static ProcessingOrder resolveOrdering() {
        String property = System.getProperty("soak.ordering");
        return property == null ? ProcessingOrder.KEY : ProcessingOrder.valueOf(property);
    }

    /**
     * {@code -Dsoak.messageBufferSize=20000} raises the record-intake gate's threshold and NOTHING else - the
     * decisive control arm on the gate itself, and 0 (leave the dynamic load factor alone) by default.
     * <p>
     * The gate is {@code inShards - parkedForRetry > targetAmountOfRecordsInFlight * loadingFactor}, which at
     * {@code maxConcurrency} 14 and the initial factor of 2 is a threshold of <b>28</b> - about a eighteenth of
     * the poisoned records a single 1000-record burst leaves permanently resident. If the gate is what stops
     * intake, a threshold set above what the run can accumulate keeps the instance taking work for the whole
     * run; if the instance stalls anyway, the gate is not the mechanism.
     */
    private static int resolveMessageBufferSize() {
        String property = System.getProperty("soak.messageBufferSize");
        return property == null ? 0 : Integer.parseInt(property);
    }

    /**
     * {@code -Dsoak.progressInterval=PT5S}; ISO-8601, like {@code soak.duration}, and 60s by default.
     * <p>
     * A knob because the interval that suits a thirty-minute run is the wrong one for reading the moment
     * intake freezes - which the 2026-09-07 runs put inside the first sixty seconds, i.e. inside a single
     * sample. A short diagnostic run wants several samples across the freeze.
     */
    private static Duration resolveProgressInterval() {
        String property = System.getProperty("soak.progressInterval");
        return property == null ? DEFAULT_PROGRESS_LOG_INTERVAL : Duration.parse(property);
    }
}
