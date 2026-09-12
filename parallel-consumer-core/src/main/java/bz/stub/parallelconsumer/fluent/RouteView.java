package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.Set;

/**
 * One route as a reader sees it: its topics, its types and its resolved policy, with no way to change any of it.
 * <p>
 * This is what the runtime seam is handed (KTD9). The sandbox reads the types to know what to hydrate and how to
 * encode it; the dispatch wrapper reads the policy to know when a record has run out of attempts.
 */
@InterfaceStability.Unstable
public interface RouteView {

    /**
     * The topics this route binds. More than one when the route was declared over a set (R5), in which case they
     * share the one function, the one type pair and the one admission target.
     */
    Set<String> topics();

    /**
     * How this route reads key bytes. A fake reads it to encode a hydrated key the way the route expects to decode
     * it, which is what lets one definition run against either world (KTD9).
     */
    Format<?> consumedKey();

    /**
     * How this route reads value bytes - the other half of what a fake needs to hydrate records this route accepts.
     */
    Format<?> consumedValue();

    /**
     * How this route writes produced key bytes, and - by being null or not - the declaration of whether it produces
     * at all. A runtime asks it to build the serialisers the produce path needs before any record arrives.
     *
     * @return null when the route declares no produced types, which is also what makes producing from it a compile
     * error (R3)
     */
    Format<?> producedKey();

    /**
     * How this route writes produced value bytes, null on the same terms as {@link #producedKey()} - the two are
     * declared together, so neither can be present without the other.
     *
     * @see #producedKey()
     */
    Format<?> producedValue();

    /**
     * Whether this route is one of the reasons the instance needs a producer. False when it declared no produced
     * types, in which case its function can only report terminal outcomes (R4).
     */
    boolean producesRecords();

    /**
     * How many attempts a failed record gets on this route before its after-retries reaction fires. Already
     * resolved against the instance default, so a reader never sees an "undeclared" third state - the route's own
     * limit and the default it inherited are indistinguishable here, deliberately.
     *
     * @return the attempt limit after the first, or empty when the route asked for unbounded retries (R10)
     */
    OptionalInt retryLimit();

    /**
     * How long a failed record waits before its next attempt on this route - the route's own delay, or the instance
     * default it inherited (R6, R10).
     */
    Duration retryDelay();

    /**
     * What happens once a record on this route runs out of attempts (R27).
     */
    AfterRetries afterRetries();

}
