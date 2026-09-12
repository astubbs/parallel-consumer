package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.OptionalInt;
import java.util.Set;

/**
 * A route's data, separate from the typed {@link Route} facade the user chains calls on.
 * <p>
 * They are separate because declaring {@link Produced} types <em>re-types</em> a route - that is what makes producing
 * from a route which declared none a compile error (R3) - and a type parameter cannot change on an object that
 * already exists. So {@code produced(...)} hands back a second {@code Route} view over this same state, and the
 * definition only ever holds the state.
 * <p>
 * A setting left null here was not declared on the route, and resolves to the definition's per-route default, taken
 * as an independent copy so that a route which overrides part of a policy does not edit the default every other
 * route shares (R6).
 */
class RouteState implements RouteView {

    /**
     * The definition this route was declared on, held so that {@link #resolveDefaults()} can read the per-route
     * defaults from it. A route cannot be moved between definitions, which is what makes a resolved value stable
     * once it has been computed.
     */
    private final ParallelConsumerDefinition owner;

    /**
     * The topics this route binds, unmodifiable and in declaration order - a route declared over a set shares one
     * function, one type pair and one admission target across all of them (R5). Order is kept because it is what a
     * refusal message and a handle's {@code toString} print, and those are read side by side.
     */
    private final Set<String> topics;

    /**
     * How this route's keys are read. Set at construction from the topic helper that started the route and replaced
     * by {@link #consumed(Format, Format)} when the route declares its own; never null, because the format-named
     * helper that started the route always supplies a pair.
     */
    private Format<?> consumedKey;

    /**
     * How this route's values are read.
     *
     * @see #consumedKey
     */
    private Format<?> consumedValue;

    /**
     * How this route's produced keys are written, and null until {@code produced(...)} is called. That null is the
     * whole answer to {@link #producesRecords()}, and on the typed side it is what makes producing from a route
     * which declared nothing a compile error (R3).
     */
    private Format<?> producedKey;

    /**
     * How this route's produced values are written, null on the same terms as {@link #producedKey}.
     */
    private Format<?> producedValue;

    /**
     * The route's one function. Null until {@code process(...)} ends the statement, which is what
     * {@link #hasFunction()} reports: a second function on the same topic is refused rather than replacing the
     * first, and a route that never got one is refused at validation.
     */
    private ProcessFunction<?, ?, ?, ?> function;

    /**
     * This route's own retry limit, as a tri-state that cannot be set inconsistently: <b>null</b> means the route
     * declared none and takes the definition's default, <b>empty</b> means it declared retries unbounded, and a
     * present value is the limit it declared. It was two fields - an {@code Integer} limit and an
     * {@code ownUnboundedRetries} flag - whose agreement was maintained by hand in the two setters below.
     */
    private OptionalInt ownRetryLimit;

    /**
     * This route's own retry delay, or null when it declared none and takes the definition's default (R6).
     */
    private Duration ownRetryDelay;

    /**
     * This route's own admission target, boxed so that null means undeclared - routes do not compete for one shared
     * limit, so an undeclared target still resolves to a target of its own (R23).
     */
    private Integer ownConcurrency;

    /**
     * This route's own park policy, or null for the definition's default. Never handed out as-is: whichever policy
     * wins, {@link #resolveDefaults()} resolves to a {@link AfterRetries#copy()} of it, so a route cannot edit a
     * policy another route also reads (R6).
     */
    private AfterRetries ownAfterRetries;

    /**
     * This route's own park observer, or null for the definition's default. It is the one setting here that is a
     * callback rather than data, which is why it stays off {@link RouteView} (R18).
     */
    private ParkObserver<?, ?> ownParkObserver;

    /**
     * Whether {@link #resolveDefaults()} has run. <b>Volatile, and it is the publication edge for every
     * {@code resolved*} field below</b>: resolveDefaults writes them all and then writes this flag last, and every
     * reader tests this flag before reading them, so the volatile write/read pair is what makes those plain fields
     * visible to the worker threads that read a route's policy once per failed record. Without it the only edge
     * was the engine's thread-start, inherited from the definition being validated on the thread that then starts
     * the engine - true today, stated nowhere, and lost the moment anything resolves later than that.
     */
    private volatile boolean resolved;

    /**
     * The retry limit a worker actually applies: this route's own, or the definition's. Plain, and safe to read
     * without a lock only because {@link #resolved} publishes it - see that field.
     */
    private OptionalInt resolvedRetryLimit;

    /**
     * The retry delay a worker actually applies, published by {@link #resolved} like every field around it.
     */
    private Duration resolvedRetryDelay;

    /**
     * The admission target the engine actually applies. A primitive rather than a box, because by the time anything
     * reads it a value has been resolved and there is no undeclared case left to represent.
     */
    private int resolvedConcurrency;

    /**
     * The park policy a worker actually applies, always a copy nobody else holds, and never null - a route with no
     * policy anywhere resolves to a plain {@link AfterRetries#park()} rather than to nothing (R27).
     */
    private AfterRetries resolvedAfterRetries;

    /**
     * The observer the dispatch wrapper actually calls, or null when neither the route nor the definition declared
     * one. Not copied: it is the user's own object.
     */
    private ParkObserver<?, ?> resolvedParkObserver;

    /**
     * Package-private: a route is only ever created by the definition's own route helper, which has already refused
     * a topic that another route claims. The topics are defensively copied here rather than at the call site, so
     * that a caller holding the collection it passed cannot change what this route binds.
     */
    RouteState(ParallelConsumerDefinition owner, Set<String> topics, Format<?> consumedKey, Format<?> consumedValue) {
        this.owner = owner;
        this.topics = Collections.unmodifiableSet(new LinkedHashSet<>(topics));
        this.consumedKey = consumedKey;
        this.consumedValue = consumedValue;
    }

    /**
     * Fills every resolved field from the route's own declarations, falling back to the definition's per-route
     * defaults. Idempotent, and run by validation before any view is handed out.
     */
    void resolveDefaults() {
        if (resolved) {
            return;
        }
        InstanceDefaults defaults = owner.defaults();
        resolvedRetryLimit = ownRetryLimit != null ? ownRetryLimit : defaults.retryLimit();
        resolvedRetryDelay = ownRetryDelay != null ? ownRetryDelay : defaults.retryDelay();
        resolvedConcurrency = ownConcurrency != null ? ownConcurrency : defaults.concurrency();
        AfterRetries afterRetries = ownAfterRetries != null ? ownAfterRetries : defaults.afterRetries();
        resolvedAfterRetries = afterRetries == null ? AfterRetries.park() : afterRetries.copy();
        // Not copied: an observer is the user's own object, and there is nothing about it a route could override
        // part of. It is wired like every other setting - the route's own, or the instance default (R6, R16).
        resolvedParkObserver = ownParkObserver != null ? ownParkObserver : defaults.parkObserver();
        // Last, and volatile: everything above is published by this write. See the field.
        resolved = true;
    }

    /**
     * Called by every setter below, so that a setting declared after a resolution has already happened still takes
     * effect. It clears the flag only: the stale {@code resolved*} values stay until the next
     * {@link #resolveDefaults()} overwrites them, because no reader may look at them while the flag is false.
     * <p>
     * <b>Package-private, not private, because the definition's own defaults are the other half of a resolution.</b>
     * A route resolves against {@link InstanceDefaults}, so moving an instance default has to invalidate every route
     * that inherited from it - see {@code ParallelConsumerDefinition#changingDefaults()}. Without that, a
     * definition whose routes had already resolved went on running the old value: {@code validate()} is documented
     * as failing early while the definition stays mutable, so {@code pc.validate(); pc.withDefaultRetryLimit(0);
     * pc.start()} silently kept the limit of ten, and merely reading a route through {@code DefinitionView} first
     * did the same.
     */
    void invalidateResolution() {
        resolved = false;
    }

    /**
     * Satisfies {@link RouteView#topics()} with the unmodifiable set built at construction, so the view cannot be
     * used to change which topics a route binds.
     */
    @Override
    public Set<String> topics() {
        return topics;
    }

    /**
     * Satisfies {@link RouteView#consumedKey()}. Not a resolved setting: types are the route's own or the ones its
     * helper started it with, and there is no definition-wide default to fall back to.
     */
    @Override
    public Format<?> consumedKey() {
        return consumedKey;
    }

    /**
     * Satisfies {@link RouteView#consumedValue()}.
     *
     * @see #consumedKey()
     */
    @Override
    public Format<?> consumedValue() {
        return consumedValue;
    }

    /**
     * Satisfies {@link RouteView#producedKey()}, returning null on a route that declared no produced types - the
     * view's contract, and the state the sandbox and the dispatch wrapper both branch on.
     */
    @Override
    public Format<?> producedKey() {
        return producedKey;
    }

    /**
     * Satisfies {@link RouteView#producedValue()}, null on the same terms as {@link #producedKey()}.
     */
    @Override
    public Format<?> producedValue() {
        return producedValue;
    }

    /**
     * Satisfies {@link RouteView#producesRecords()} from the key alone: the two produced formats are only ever set
     * together, so testing one is testing both.
     */
    @Override
    public boolean producesRecords() {
        return producedKey != null;
    }

    /**
     * Satisfies {@link RouteView#retryLimit()}, resolving first so that a reader never has to know whether this
     * route declared a limit or is taking the definition's.
     */
    @Override
    public OptionalInt retryLimit() {
        resolveDefaults();
        return resolvedRetryLimit;
    }

    /**
     * Satisfies {@link RouteView#retryDelay()}, resolving first.
     *
     * @see #retryLimit()
     */
    @Override
    public Duration retryDelay() {
        resolveDefaults();
        return resolvedRetryDelay;
    }

    /**
     * Satisfies {@link RouteView#concurrency()}, resolving first.
     *
     * @see #retryLimit()
     */
    @Override
    public int concurrency() {
        resolveDefaults();
        return resolvedConcurrency;
    }

    /**
     * Satisfies {@link RouteView#afterRetries()}, resolving first - and hands back <b>a copy</b>, because this is
     * the read-only view of a route and the resolved policy is not read-only.
     * <p>
     * It used to hand back the very object the workers read. A caller holding the definition after startup could
     * then call {@code thenRetryAfter} or {@code forCycles} through a view that promises to change nothing: past
     * every validation rule, and onto two plain fields that a worker reads once per failed record with no edge to
     * carry the write - so a worker could see a half-configured cycle, or silently start running a schedule nobody
     * declared. A copy makes the view honest and leaves the workers reading what resolution published.
     * <p>
     * The wrapper and the validation rules take {@link #resolvedAfterRetries()} instead, which is the same object
     * they always read.
     */
    @Override
    public AfterRetries afterRetries() {
        return resolvedAfterRetries().copy();
    }

    /**
     * The resolved policy itself, for the dispatch wrapper and the validation rules inside this package - the
     * readers that must see what the workers see rather than a copy of it, and that are inside the publication
     * edge {@link #resolved} establishes.
     */
    AfterRetries resolvedAfterRetries() {
        resolveDefaults();
        return resolvedAfterRetries;
    }

    /**
     * The route's function, for the dispatch wrapper that calls it. Deliberately not on {@link RouteView}, which is
     * the wire-shaped read of a route and carries data only (R18); the wildcards are erased because the typing was
     * checked at the {@link Route} facade, where the user wrote it.
     */
    ProcessFunction<?, ?, ?, ?> function() {
        return function;
    }

    /**
     * The observer told once when a record on this route parks, or null when neither the route nor the definition
     * declared one (R16).
     * <p>
     * Deliberately <b>not</b> on {@link RouteView}: that view is the wire-shaped read of a route, and an observer is
     * a callback rather than data (R18). The dispatch wrapper reads it from here.
     */
    ParkObserver<?, ?> parkObserver() {
        resolveDefaults();
        return resolvedParkObserver;
    }

    /**
     * Whether the statement has already ended. It is what makes a second {@code process(...)} on the same topic a
     * refusal rather than a silent replacement: one function per route is the rule, and a route that quietly took
     * the last function declared would drop work nobody could see going missing (R2).
     */
    boolean hasFunction() {
        return function != null;
    }

    /**
     * The topics, rendered for a validation message that must name the offending route.
     */
    String describeTopics() {
        return describeTopics(topics);
    }

    /**
     * The same rendering for a caller that holds a route's topics without holding the route - {@link RouteHandle},
     * which is handed only the set. One owner, because the two spellings of it must name a route identically:
     * a refusal and a handle's {@code toString} are read side by side.
     */
    static String describeTopics(Set<String> topics) {
        return topics.size() == 1 ? topics.iterator().next() : topics.toString();
    }

    /**
     * Takes the pair together, because a key format without its value format is not a state any route may be left
     * in. No resolution to invalidate: types have no definition-wide default.
     */
    void consumed(Format<?> key, Format<?> value) {
        this.consumedKey = key;
        this.consumedValue = value;
    }

    /**
     * Takes the pair together for the same reason as {@link #consumed(Format, Format)}, and the typed side re-types
     * the route as it calls this - which is what turns producing from a route that declared nothing into a compile
     * error rather than a runtime one (R3).
     */
    void produced(Format<?> key, Format<?> value) {
        this.producedKey = key;
        this.producedValue = value;
    }

    /**
     * Records the function unconditionally: the refusal of a second one belongs to the facade, which owns the
     * message naming the topic. Guard with {@link #hasFunction()} before calling.
     */
    void function(ProcessFunction<?, ?, ?, ?> function) {
        this.function = function;
    }

    /**
     * Declares a bounded limit for this route. Validated by the facade before it arrives here, so a limit reaching
     * this point is already known to be non-negative (R10).
     */
    void ownRetryLimit(int limit) {
        this.ownRetryLimit = OptionalInt.of(limit);
        invalidateResolution();
    }

    /**
     * Declares retries unbounded for this route, which is a declaration and not an absence - the empty
     * {@link OptionalInt} is what tells {@link #resolveDefaults()} to stop rather than fall back to the
     * definition's limit (R10).
     */
    void ownUnboundedRetries() {
        this.ownRetryLimit = OptionalInt.empty();
        invalidateResolution();
    }

    /**
     * Declares this route's own retry delay. Validated by the facade, which owns the message naming the topic.
     */
    void ownRetryDelay(Duration delay) {
        this.ownRetryDelay = delay;
        invalidateResolution();
    }

    /**
     * Declares this route's own admission target (R23).
     *
     * @see #ownRetryDelay(Duration)
     */
    void ownConcurrency(int concurrency) {
        this.ownConcurrency = concurrency;
        invalidateResolution();
    }

    /**
     * Declares this route's own park policy. Stored as handed over rather than copied here: the copy is taken at
     * resolution, which is the point where the definition's default could otherwise be shared (R6).
     */
    void ownAfterRetries(AfterRetries policy) {
        this.ownAfterRetries = policy;
        invalidateResolution();
    }

    /**
     * Whether this route declared an after-retries policy of its own, as against inheriting the instance default.
     * Validation needs the distinction that {@link #afterRetries()} deliberately erases: a policy that can never
     * fire is a mistake when this route wrote it, and merely an unused default when it did not (R6, R27).
     */
    boolean declaresOwnAfterRetries() {
        return ownAfterRetries != null;
    }

    /**
     * Whether this route declared a retry limit of its own, in either form - a bound or unbounded. Validation
     * reads it only to say <em>where</em> a route's unbounded retries were declared, so that a refusal points at
     * the call the author actually wrote rather than at the one they inherited.
     */
    boolean declaresOwnRetryLimit() {
        return ownRetryLimit != null;
    }

    /**
     * Declares this route's own park observer (R16). Never copied - it is the user's object, and there is no part
     * of it a route could override.
     */
    void ownParkObserver(ParkObserver<?, ?> observer) {
        this.ownParkObserver = observer;
        invalidateResolution();
    }

    /**
     * Names the route the way every refusal about it does, and no more: the settings are deliberately left out, so
     * that a message quoting a route stays about the route rather than becoming a dump of its policy.
     */
    @Override
    public String toString() {
        return "Route(" + describeTopics() + ")";
    }
}
