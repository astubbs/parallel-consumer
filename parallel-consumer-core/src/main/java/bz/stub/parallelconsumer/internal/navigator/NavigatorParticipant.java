package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.RateLimiter;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.state.ShardKey;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.DoubleSupplier;

/**
 * One PC instance's membership of the shared {@link ResourceAllocator} (U3's engine seam): the instance's stable
 * member id, its tagged resource names, and the allocator handle, bound together once at module construction so
 * the selection path never re-derives any of them per record (R3's zero-cost resolution).
 * <p>
 * <b>Two shapes, decided once per instance.</b> An untagged instance gets the {@link #inert()} participant:
 * {@link #isActive()} is false and every caller short-circuits before touching the allocator - no lookups, no
 * clock reads, no allocation on that path (R3). A tagged instance gets {@link #activeMember}, and its methods
 * split exactly as {@link ResourceAllocator}'s do:
 * <ul>
 *   <li><b>Pure reads</b> - {@link #hasSpendableCreditForAllTags}, {@link #availableAt},
 *       {@link #earliestBlockedResourceNextCreditAt}. Safe from any thread at any frequency; the claim-path
 *       eligibility term (KTD1) and the {@code timeToBlockFor} wakeup bound (KTD5) consume these.</li>
 *   <li><b>Mutating</b> - {@link #join}, {@link #leave}, {@link #readQuantum} (lifecycle, R16/KTD4) and
 *       {@link #spendOneCreditPerTag} (the post-claim debit, KTD1). Owned by the engine's lifecycle and claim
 *       seams; never called from a pure query.</li>
 * </ul>
 * <b>Immutability is the concurrency contract</b>: every field is final and set before publication, so the
 * participant itself needs no lock ({@code @GuardedBy} has nothing to name) - all synchronisation lives inside
 * the allocator implementation (KTD11). Every {@code now} passed in must come from the one canonical clock the
 * allocator and its members share (KTD4): in production both the module clock and the allocator's
 * construction-time clock are UTC; the virtual-clock test lane shares one {@code MutableClock} across both.
 * <p>
 * <b>A third category, U4's attribution bookkeeping</b> - {@link #onDeferralEpisodeStarted},
 * {@link #onDeferralEpisodeEnded}, {@link #blockingResourceDeferrals}, {@link #initMetrics}. Called from the
 * shard-walk refusal branches (the defer-transition observation site) and the claim-success path. Its counters
 * ({@link #deferredRecordCount}, {@link #lastReasonValue}, {@link #deferralEpisodeCounters}) are final
 * references to internally-thread-safe types ({@link LongAdder}, {@link AtomicInteger},
 * {@link ConcurrentHashMap}) - the same "final reference to a self-guarding type needs no {@code @GuardedBy}"
 * shape {@code StubResourceAllocator.Counters} uses, so immutability of the FIELD still holds even though the
 * VALUE it refers to mutates.
 * <p>
 * <b>Fail-safe on a throwing allocator.</b> The allocator is user-supplied (a public options seam), so every
 * interaction here is guarded: a {@link RuntimeException} from it degrades this instance rather than killing it
 * (the control task's own boundary would otherwise close the whole consumer). Eligibility reads treat the
 * resource as BLOCKED with no known next credit - a deferral, never a free pass - view reads return their
 * empty/zero shapes, and mutating calls are skipped. Each failure is counted monotonically
 * ({@link #allocatorFailureCount()}, the {@code pc.navigator.allocator.failures} gauge) and logged rate-limited
 * under {@link #LOG_PREFIX} via {@link #recordAllocatorFailure}.
 *
 * @author Antony Stubbs
 */
@Slf4j
public final class NavigatorParticipant {

    private static final NavigatorParticipant INERT =
            new NavigatorParticipant(null, Collections.emptyList(), null);

    /** Null exactly when this participant is {@link #inert()}. */
    private final ResourceAllocator allocator;

    /** Immutable; empty exactly when this participant is {@link #inert()}. */
    private final List<String> resourceTags;

    /** Null exactly when this participant is {@link #inert()}; otherwise stable for the instance's lifetime. */
    private final String memberId;

    /**
     * The greppable log prefix every navigator attribution line carries (U4, mirrors the admission package's own
     * {@code "Adaptive concurrency"} convention) - public so {@code ProcessingShard}'s attribution site, the
     * actual log-emission location, uses the SAME literal rather than a drifting duplicate.
     */
    public static final String LOG_PREFIX = "Navigator";

    private static final String RESOURCE_TAG_KEY = "resource";

    /**
     * Records currently resource-deferred for this participant (U4's deferred-count gauge) - incremented on the
     * transition INTO a deferral episode, decremented on the transition OUT (dispatch, or the record leaving the
     * shard while still deferred). Deliberately never clamped: a pairing bug should show as a wrong number, not
     * be hidden (the counter-clamp learning). Zero for {@link #inert()}, and never touched on that path (R3).
     */
    private final LongAdder deferredRecordCount = new LongAdder();

    /** Which {@link NavigatorDecisionReason} bound the MOST RECENT deferral episode - the gauge value (KTD6). */
    private final AtomicInteger lastReasonValue = new AtomicInteger(NavigatorDecisionReason.NO_DEFERRAL_VALUE);

    /**
     * One Micrometer {@link Counter} per {@link NavigatorDecisionReason}, tagged by reason name - populated by
     * {@link #initMetrics} and incremented once per deferral EPISODE (never per re-evaluation pass) by
     * {@link #onDeferralEpisodeStarted}. Empty (and every increment a no-op) until {@link #initMetrics} runs, and
     * permanently empty for {@link #inert()}.
     */
    private final Map<NavigatorDecisionReason, Counter> deferralEpisodeCounters = new ConcurrentHashMap<>();

    /**
     * U5's per-ordering-shard breakdown of {@link #deferredRecordCount} (KTD9, R18): how many records are
     * currently resource-deferred, keyed by the engine's own {@link ShardKey}. Maintained at the same
     * exactly-once episode transitions as the total - never incremented per evaluation pass - and read as a
     * weakly-consistent snapshot by {@link #resourceIneligibleCountByShardSnapshot()}, so the user-function
     * thread never scans the controller-owned shard map itself. An entry that reaches zero is REMOVED (a
     * long-lived KEY-ordered instance would otherwise accrete an entry per key forever); an unpaired decrement
     * therefore leaves a visible negative entry rather than being clamped away (the counter-clamp learning).
     * Final reference to a self-guarding type, like its siblings above - no {@code @GuardedBy} to name (KTD11).
     */
    private final ConcurrentHashMap<ShardKey, Long> resourceDeferredCountByShard = new ConcurrentHashMap<>();

    /** How often {@link #recordAllocatorFailure} may warn - failures sit on the per-claim hot path, so an
     * unlimited warning would log at claim frequency. Same cadence as {@code ProcessingShard}'s own navigator
     * constraint report. */
    private static final int ALLOCATOR_FAILURE_LOG_INTERVAL_SECONDS = 5;

    /**
     * Total allocator calls that threw (the fail-safe posture above) - monotonic, never reset, the
     * {@link PCMetricsDef#NAVIGATOR_ALLOCATOR_FAILURES} gauge target. Zero for a healthy allocator and always
     * zero for {@link #inert()}, which holds no allocator to fail. Final reference to a self-guarding type,
     * like its counter siblings (KTD11).
     */
    private final LongAdder allocatorFailureCount = new LongAdder();

    /**
     * Rate-limits the allocator-failure warning. {@link RateLimiter} itself is NOT thread-safe (a plain
     * timestamp, no lock to name in a {@code @GuardedBy}) - tolerated deliberately, because a race between the
     * claim path and the metrics scrape thread can only duplicate or drop a WARN line, never corrupt state; the
     * {@link #allocatorFailureCount} it accompanies is the exact record.
     */
    private final RateLimiter allocatorFailureLogLimiter = new RateLimiter(ALLOCATOR_FAILURE_LOG_INTERVAL_SECONDS);

    private NavigatorParticipant(ResourceAllocator allocator, List<String> resourceTags, String memberId) {
        this.allocator = allocator;
        this.resourceTags = resourceTags;
        this.memberId = memberId;
    }

    /** The untagged instance's participant (R3): inactive, and every method a guaranteed no-op. */
    public static NavigatorParticipant inert() {
        return INERT;
    }

    /**
     * A tagged instance's participant. The caller (the module) has already validated the tags against the
     * allocator's registry ({@code ParallelConsumerOptions#validate()}, R4/R19), so this only pins the shape.
     *
     * @throws IllegalArgumentException when the tag list is empty - an "active" participant with nothing to
     *                                  gate would silently behave as inert, which is the configuration lie
     *                                  R19 exists to prevent
     */
    public static NavigatorParticipant activeMember(ResourceAllocator allocator, List<String> resourceTags,
                                                    String memberId) {
        if (resourceTags == null || resourceTags.isEmpty()) {
            throw new IllegalArgumentException("An active navigator participant needs at least one resource tag - "
                    + "use inert() for an untagged instance");
        }
        return new NavigatorParticipant(allocator,
                Collections.unmodifiableList(new ArrayList<>(resourceTags)), memberId);
    }

    /** Whether this instance participates in the navigator at all - the R3 gate every caller checks first. */
    public boolean isActive() {
        return allocator != null;
    }

    /** The stable member id this instance is known to the allocator by. Null when {@link #inert()}. */
    public String memberId() {
        return memberId;
    }

    /** The resource names this instance's function requires (R2). Immutable; empty when {@link #inert()}. */
    public List<String> resourceTags() {
        return resourceTags;
    }

    // ------------------------------------------------------------------
    // Pure reads (KTD1 eligibility, KTD5 wakeup) - never mutate anything
    // ------------------------------------------------------------------

    /**
     * The claim's resource-eligibility term (KTD1): true when EVERY tagged resource holds a live lease with at
     * least one spendable credit for this member at {@code now}. Pure - a lease can exist with zero credits, and
     * that counts as blocked. Always true when {@link #inert()}.
     */
    public boolean hasSpendableCreditForAllTags(Instant now) {
        if (!isActive()) {
            return true;
        }
        for (String tag : resourceTags) {
            if (isBlocked(tag, now)) {
                return false;
            }
        }
        return true;
    }

    /**
     * When a record deferred NOW becomes dispatchable (R7): the LATEST of the blocking resources' next-credit
     * times - a record needing several resources cannot run until the last of them has credit, so the max, not
     * the min. Empty when nothing is blocking (or when every blocking resource's policy mints nothing, in which
     * case there is no time to name). A projection, not a promise (KD10's best-effort framing).
     */
    public Optional<Instant> availableAt(Instant now) {
        List<Instant> credits = blockedNextCredits(now);
        return credits.isEmpty() ? Optional.empty() : Optional.of(Collections.max(credits));
    }

    /**
     * The wakeup bound's input (KTD5): the EARLIEST next-credit time over the resources currently blocking -
     * the first instant at which any deferred work could become dispatchable, so the control loop's block time
     * is capped by it rather than by the poll default. Min where {@link #availableAt} is max, deliberately: a
     * wake that finds the work still multi-resource-blocked just re-blocks (soft, R8's best-effort posture).
     * Empty when nothing is blocking.
     */
    public Optional<Instant> earliestBlockedResourceNextCreditAt(Instant now) {
        List<Instant> credits = blockedNextCredits(now);
        return credits.isEmpty() ? Optional.empty() : Optional.of(Collections.min(credits));
    }

    /**
     * The blocking resources' next-credit times, unreduced - {@code availableAt} takes the max, the wakeup the
     * min. Derived from {@link #blockingResourceDeferrals} so the blocked-tag walk, its allocator calls and the
     * fail-safe guard exist exactly once; a deferral with no time to name is simply skipped, exactly as the
     * allocator's own {@link Optional#empty()} is.
     */
    private List<Instant> blockedNextCredits(Instant now) {
        List<Instant> credits = new ArrayList<>();
        for (ResourceDeferral deferral : blockingResourceDeferrals(now)) {
            deferral.getNextCreditAt().ifPresent(credits::add);
        }
        return credits;
    }

    /**
     * U4's attribution read: EVERY currently-blocking tagged resource paired with its own next-credit time -
     * never a chosen one (R9's all-binding-predicates clause). {@link #availableAt} and
     * {@link #earliestBlockedResourceNextCreditAt} reduce this same set to its max/min; this is the unreduced
     * form the defer-moment log line and {@link NavigatorDecision} need. Empty when nothing is blocking, or when
     * {@link #inert()} (R3).
     */
    public List<ResourceDeferral> blockingResourceDeferrals(Instant now) {
        List<String> blocked = blockedTags(now);
        if (blocked.isEmpty()) {
            return Collections.emptyList();
        }
        List<ResourceDeferral> deferrals = new ArrayList<>(blocked.size());
        for (String tag : blocked) {
            Optional<Instant> nextCreditAt;
            try {
                nextCreditAt = allocator.nextCreditAt(memberId, tag, now);
            } catch (RuntimeException e) {
                recordAllocatorFailure(e);
                nextCreditAt = Optional.empty(); // fail safe: still blocking, with no KNOWN next credit
            }
            deferrals.add(new ResourceDeferral(tag, nextCreditAt));
        }
        return Collections.unmodifiableList(deferrals);
    }

    /**
     * U4's attribution decision: {@link #blockingResourceDeferrals} plus whether admission slots are ALSO
     * binding, assembled into a {@link NavigatorDecision} - or empty when nothing is currently blocking (the
     * refusal branch that called {@code onQueueingForExecution()} was refused for a different reason, e.g. the
     * record is still in flight). Pure - never mutates the counters below; the caller decides whether this is a
     * NEW episode (see {@link #onDeferralEpisodeStarted}).
     */
    public Optional<NavigatorDecision> currentDecision(Instant now, boolean admissionSlotsAlsoBinding) {
        List<ResourceDeferral> blocking = blockingResourceDeferrals(now);
        if (blocking.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(NavigatorDecision.of(blocking, admissionSlotsAlsoBinding));
    }

    // ------------------------------------------------------------------
    // U4's attribution bookkeeping - called once per deferral EPISODE transition, never per pass
    // ------------------------------------------------------------------

    /**
     * Called exactly once per deferral episode, at the transition INTO resource-deferred (the caller - the
     * shard-walk refusal branch - deduplicates via the record's own CAS-guarded marker; this method trusts that
     * it is only called on a genuine transition). Bumps the deferred-count gauge and the record's shard's entry
     * in the per-shard breakdown (U5), records the reason as the latest-reason gauge value, and increments that
     * reason's episode counter. A no-op counter map entry (before {@link #initMetrics} runs, or for
     * {@link #inert()}, which never reaches here) is silently skipped rather than thrown - metrics registration
     * is best-effort observability, never load-bearing for the decision.
     *
     * @param shardKey the ordering shard holding the deferred record - the key the paired
     *                 {@link #onDeferralEpisodeEnded(ShardKey)} must later present
     */
    public void onDeferralEpisodeStarted(NavigatorDecision decision, ShardKey shardKey) {
        deferredRecordCount.increment();
        adjustShardCount(shardKey, 1);
        lastReasonValue.set(decision.getReason().getValue());
        Counter counter = deferralEpisodeCounters.get(decision.getReason());
        if (counter != null) {
            counter.increment();
        }
    }

    /**
     * Called exactly once per deferral episode, at the transition OUT of resource-deferred - a successful claim
     * (the credit spend site) or the record leaving the shard while still deferred (revocation, a stale sweep).
     * Never touches the allocator's conservation counters (KTD10: revocation is a credit no-op) - this is a
     * SEPARATE, purely observational count.
     *
     * @param shardKey the same ordering shard the episode's {@link #onDeferralEpisodeStarted} named - engine
     *                 callers all derive it the way {@code ShardManager#computeShardKey} does
     *                 ({@code ShardKey.of(record, ordering)}), so the pairing cannot drift
     */
    public void onDeferralEpisodeEnded(ShardKey shardKey) {
        deferredRecordCount.decrement();
        adjustShardCount(shardKey, -1);
    }

    /**
     * One shard's deferred-count entry moved by one episode transition: zero entries are removed rather than
     * kept (no accretion under KEY ordering), negatives are kept rather than clamped (a pairing bug must show
     * as a wrong number - the counter-clamp learning, same stance as {@link #deferredRecordCount}).
     */
    private void adjustShardCount(ShardKey shardKey, long delta) {
        resourceDeferredCountByShard.compute(shardKey, (key, current) -> {
            long next = (current == null ? 0L : current) + delta;
            return next == 0 ? null : next;
        });
    }

    /** The deferred-count gauge's live value (U4) - the {@link PCMetrics} extractor target. */
    public long currentlyDeferredCount() {
        return deferredRecordCount.sum();
    }

    /**
     * U5's per-shard read (KTD9): an immutable, weakly-consistent snapshot of the currently-resource-deferred
     * count per ordering shard. A copy, deliberately - the caller (the {@link NavigatorView} on the
     * user-function thread) must never hold a live view of a map the controller is mutating, and shards with
     * nothing deferred are absent rather than zero. Pure: reads the navigator-owned breakdown, never the
     * controller-owned shard map.
     */
    public Map<ShardKey, Long> resourceIneligibleCountByShardSnapshot() {
        if (resourceDeferredCountByShard.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new HashMap<>(resourceDeferredCountByShard));
    }

    /**
     * Pure read for the view (U5, R18's global half): {@code resourceName}'s declared policy rate in credits
     * per second. Callers gate on {@link #isActive()} - the inert participant answers {@code 0.0} rather than
     * touching an allocator it does not have.
     */
    public double globalRatePerSecond(String resourceName) {
        if (!isActive()) {
            return 0.0;
        }
        try {
            return allocator.globalRatePerSecond(resourceName);
        } catch (RuntimeException e) {
            recordAllocatorFailure(e);
            return 0.0; // fail safe: the view's zero shape, same answer as an unknown resource
        }
    }

    /**
     * Pure read for the view (U5, R18's instance-local half): the rate currently available to THIS member
     * against {@code resourceName} under current membership, in credits per second. {@code 0.0} when not
     * currently a member, or (callers gate on {@link #isActive()}) when inert.
     */
    public double localRatePerSecond(String resourceName, Instant now) {
        if (!isActive()) {
            return 0.0;
        }
        try {
            return allocator.localRatePerSecond(memberId, resourceName, now);
        } catch (RuntimeException e) {
            recordAllocatorFailure(e);
            return 0.0; // fail safe: the view's zero shape, same answer as a non-member
        }
    }

    /** The latest-reason gauge's live value (U4, KTD6) - {@link NavigatorDecisionReason#NO_DEFERRAL_VALUE} before any episode. */
    public int lastReasonValue() {
        return lastReasonValue.get();
    }

    /**
     * Registers the {@code pc.navigator.*} meters (U4): the deferred-count, latest-reason and
     * allocator-failure gauges, one episode counter per {@link NavigatorDecisionReason}, and
     * per-tagged-resource spent/overdraft/next-credit
     * gauges read live from the allocator's {@link ConservationLedger} - mirrors
     * {@code AdmissionController#initMetrics}'s mode-gated pattern. A NO-OP for {@link #inert()} (R3: an
     * untagged instance registers nothing) or when {@code pcMetrics} is null. Called once, by
     * {@code PCModule#navigatorParticipant()} immediately after construction.
     * <p>
     * {@code clock} is captured by the per-resource gauge lambdas below and read at EVERY scrape (KTD4's one
     * canonical clock, not {@code Instant.now()}) - production passes the module's own UTC clock; the
     * virtual-clock test lane passes the shared {@code MutableClock}, so a gauge read in a test advances exactly
     * as the engine's own eligibility checks do.
     */
    public void initMetrics(PCMetrics pcMetrics, Clock clock) {
        if (!isActive() || pcMetrics == null) {
            return;
        }
        pcMetrics.gaugeFromMetricDef(PCMetricsDef.NAVIGATOR_DEFERRED_RECORDS, this,
                NavigatorParticipant::currentlyDeferredCount);
        pcMetrics.gaugeFromMetricDef(PCMetricsDef.NAVIGATOR_DEFERRAL_REASON, this,
                NavigatorParticipant::lastReasonValue);
        pcMetrics.gaugeFromMetricDef(PCMetricsDef.NAVIGATOR_ALLOCATOR_FAILURES, this,
                NavigatorParticipant::allocatorFailureCount);
        for (NavigatorDecisionReason reason : NavigatorDecisionReason.values()) {
            Counter counter = pcMetrics.getCounterFromMetricDef(PCMetricsDef.NAVIGATOR_DEFERRAL_EPISODES,
                    Tag.of("reason", reason.name()));
            deferralEpisodeCounters.put(reason, counter);
        }
        for (String resourceName : resourceTags) {
            Tag resourceTag = Tag.of(RESOURCE_TAG_KEY, resourceName);
            // guarded: these read the user-supplied allocator from the scrape thread, so a throwing allocator
            // must degrade the gauge to its zero/absent shape, never fail the scrape (the fail-safe posture)
            pcMetrics.gaugeFromMetricDef(PCMetricsDef.NAVIGATOR_CREDITS_SPENT, this,
                    p -> p.guardedGaugeRead(
                            () -> p.allocator.conservationLedger(resourceName, clock.instant()).getSpent(), 0),
                    resourceTag);
            pcMetrics.gaugeFromMetricDef(PCMetricsDef.NAVIGATOR_CREDITS_OVERDRAFT, this,
                    p -> p.guardedGaugeRead(
                            () -> p.allocator.conservationLedger(resourceName, clock.instant()).getOverdraft(), 0),
                    resourceTag);
            pcMetrics.gaugeFromMetricDef(PCMetricsDef.NAVIGATOR_NEXT_CREDIT_AT, this,
                    p -> p.guardedGaugeRead(() -> p.allocator.nextCreditAt(resourceName, clock.instant())
                            .map(Instant::getEpochSecond).orElse(-1L), -1),
                    resourceTag);
        }
    }

    // ------------------------------------------------------------------
    // Mutating - the engine's lifecycle and post-claim seams only
    // ------------------------------------------------------------------

    /**
     * The post-claim debit (KTD1): one credit from EVERY tagged resource, called immediately after the claim
     * CAS wins and never on a lost race. Always succeeds - a credit gone between the eligibility read and this
     * call lands as overdraft in the allocator (KD10); no rollback, no refund.
     */
    public void spendOneCreditPerTag(Instant now) {
        if (!isActive()) {
            return;
        }
        for (String tag : resourceTags) {
            try {
                allocator.spend(memberId, tag, now);
            } catch (RuntimeException e) {
                // per tag, so one failing resource never skips a healthy resource's debit (its ledger stays honest)
                recordAllocatorFailure(e);
            }
        }
    }

    /** Membership join (R16) - the engine calls this once, at the running transition. No-op when inert. */
    public void join(Instant now) {
        if (isActive()) {
            try {
                allocator.join(memberId, now);
            } catch (RuntimeException e) {
                recordAllocatorFailure(e);
            }
        }
    }

    /**
     * Membership leave (R16) - the engine calls this at its CLOSING transition (after any drain has
     * completed, never before: leave expires live credits immediately, which would starve a draining backlog)
     * so the share is dropped at the next quantum without waiting for the lease TTL (AE2). No-op when inert.
     */
    public void leave(Instant now) {
        if (isActive()) {
            try {
                allocator.leave(memberId, now);
            } catch (RuntimeException e) {
                recordAllocatorFailure(e);
            }
        }
    }

    /**
     * THE per-pass quantum pull (KTD4): renews the membership lease and materialises this quantum's share.
     * The engine calls this once per control-loop pass, beside the admission tick. No-op when inert.
     */
    public void readQuantum(Instant now) {
        if (isActive()) {
            try {
                allocator.readQuantum(memberId, now);
            } catch (RuntimeException e) {
                recordAllocatorFailure(e);
            }
        }
    }

    /**
     * Blocked = no live lease, or a live lease with zero credits left (KTD1's eligibility definition) - or an
     * allocator that THREW: an unreadable resource fails safe as blocked (a deferral, never a free pass, and
     * never a crash on the per-claim hot path).
     */
    private boolean isBlocked(String tag, Instant now) {
        Optional<CapacityLease> lease;
        try {
            lease = allocator.currentLease(memberId, tag, now);
        } catch (RuntimeException e) {
            recordAllocatorFailure(e);
            return true;
        }
        return !lease.isPresent() || lease.get().getAvailableCredits() <= 0;
    }

    /**
     * The one shared failure seam behind the class javadoc's fail-safe posture: count it (monotonic, for the
     * {@link PCMetricsDef#NAVIGATOR_ALLOCATOR_FAILURES} gauge) and warn rate-limited. The CALLER supplies the
     * degraded answer - blocked for eligibility, empty/zero for views, skip for mutations - because only the
     * call site knows its safe shape. Deliberately try/catch at each site rather than a supplier-wrapping
     * helper: a capturing lambda would allocate per call on the hot claim path; this method costs nothing until
     * an exception is already in flight.
     */
    private void recordAllocatorFailure(RuntimeException failure) {
        allocatorFailureCount.increment();
        allocatorFailureLogLimiter.performIfNotLimited(() -> log.warn(
                LOG_PREFIX + " ({}): resource allocator threw - degrading soft, not crashing: eligibility reads "
                        + "report blocked, view reads report empty, mutating calls are skipped "
                        + "({} allocator failures so far)",
                memberId, allocatorFailureCount.sum(), failure));
    }

    /** The allocator-failure count's live value - the {@link PCMetricsDef#NAVIGATOR_ALLOCATOR_FAILURES} gauge target. */
    public long allocatorFailureCount() {
        return allocatorFailureCount.sum();
    }

    /**
     * The scrape-thread half of the fail-safe posture: the per-resource gauges in {@link #initMetrics} read the
     * allocator live, so a throwing allocator would otherwise fail every scrape. Off the hot path, so the
     * supplier allocation the claim path avoids is fine here.
     */
    private double guardedGaugeRead(DoubleSupplier read, double fallback) {
        try {
            return read.getAsDouble();
        } catch (RuntimeException e) {
            recordAllocatorFailure(e);
            return fallback;
        }
    }

    private List<String> blockedTags(Instant now) {
        if (!isActive()) {
            return Collections.emptyList();
        }
        List<String> blocked = new ArrayList<>(resourceTags.size());
        for (String tag : resourceTags) {
            if (isBlocked(tag, now)) {
                blocked.add(tag);
            }
        }
        return blocked;
    }

    @Override
    public String toString() {
        return isActive()
                ? "NavigatorParticipant(memberId=" + memberId + ", resourceTags=" + resourceTags + ")"
                : "NavigatorParticipant(inert)";
    }
}
