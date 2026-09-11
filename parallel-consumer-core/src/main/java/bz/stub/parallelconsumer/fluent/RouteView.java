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
 * This is what the runtime seam is handed (KTD9). The sandbox reads the types to know what to generate and how to
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
     * How this route reads key bytes. A fake reads it to encode a generated key the way the route expects to decode
     * it, which is what lets one definition run against either world (KTD9).
     */
    Format<?> consumedKey();

    /**
     * How this route reads value bytes - the other half of what a fake needs to generate records this route accepts.
     */
    Format<?> consumedValue();

    /**
     * @return null when the route declares no produced types, which is also what makes producing from it a compile
     * error (R3)
     */
    Format<?> producedKey();

    /**
     * @see #producedKey()
     */
    Format<?> producedValue();

    /**
     * Whether this route is one of the reasons the instance needs a producer. False when it declared no produced
     * types, in which case its function can only report terminal outcomes (R4).
     */
    boolean producesRecords();

    /**
     * @return the attempt limit after the first, or empty when the route asked for unbounded retries (R10)
     */
    OptionalInt retryLimit();

    /**
     * How long a failed record waits before its next attempt on this route - the route's own delay, or the instance
     * default it inherited (R6, R10).
     */
    Duration retryDelay();

    /**
     * This route's admission target: routes do not compete for one shared limit (R23, KD6).
     */
    int concurrency();

    /**
     * What happens once a record on this route runs out of attempts (R27).
     */
    AfterRetries afterRetries();

}
