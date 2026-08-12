package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.examples.support.SimulatedService;
import lombok.extern.slf4j.Slf4j;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns the depot named on a parcel scan into a coordinate, by way of a slow, occasionally unavailable
 * geocoding API.
 * <p>
 * Example scaffolding, not a library type - and deliberately outside {@link ParcelTrackingApp}'s tagged
 * (copied) region.
 * <p>
 * <b>The call blocks, and that is the point.</b> The latency is a {@code sleep} inside
 * {@link SimulatedService}, which faithfully models what a JDBC driver or a blocking HTTP client does to
 * the calling thread. Parallel Consumer's core value proposition is precisely that it gives you a pool of
 * threads to park those blocked calls on, so that one slow lookup does not stall the whole partition. A
 * non-blocking simulation here would demonstrate the opposite of what the reader came to see.
 * <p>
 * <b>Failures are deterministic, and never swallowed.</b> A fixed fraction of lookups fail (every Nth
 * call, see {@link SimulatedService}), and the failure leaves this class as a
 * {@link GeocodeUnavailableException} which propagates out of the user function. That is what makes
 * Parallel Consumer's retry behaviour visible: the scan is re-processed rather than dropped, and its
 * offset is not committed until it succeeds.
 */
@Slf4j
public class GeocodeService {

    /**
     * What one geocode lookup costs. Simulated with a sleep; the concurrency around it is real.
     */
    public static final Duration DEFAULT_LATENCY = Duration.ofMillis(120);

    /**
     * One lookup in five fails. High for a real API, chosen so a short example run actually shows a
     * retry rather than hoping one turns up.
     */
    public static final double DEFAULT_FAILURE_FRACTION = 0.2d;

    /**
     * Depot to "latitude,longitude". A tiny fixed gazetteer, so a lookup's answer is reproducible across
     * runs and a run summary's numbers do not move for reasons the reader cannot see. Insertion-ordered
     * so {@link #knownDepots()} is stable.
     */
    private static final Map<String, String> GAZETTEER = buildGazetteer();

    private final SimulatedService geocodeApi;

    /**
     * The service the example runs with: {@link #DEFAULT_LATENCY} per lookup, failing
     * {@link #DEFAULT_FAILURE_FRACTION} of calls.
     */
    public GeocodeService() {
        this(DEFAULT_LATENCY, DEFAULT_FAILURE_FRACTION);
    }

    public GeocodeService(Duration latency, double failureFraction) {
        this.geocodeApi = new SimulatedService("geocode API", latency, failureFraction);
    }

    private static Map<String, String> buildGazetteer() {
        Map<String, String> gazetteer = new LinkedHashMap<>();
        gazetteer.put("Hemel Hempstead Hub", "51.7526,-0.4692");
        gazetteer.put("Warrington Omega Depot", "53.4200,-2.6300");
        gazetteer.put("Bristol Avonmouth Depot", "51.5030,-2.6990");
        gazetteer.put("Glasgow Eurocentral Hub", "55.8290,-3.9560");
        gazetteer.put("Doncaster iPort Depot", "53.4820,-1.1400");
        gazetteer.put("Thurrock Gateway Depot", "51.4830,0.3480");
        gazetteer.put("Swansea Llansamlet Depot", "51.6580,-3.8880");
        gazetteer.put("Newcastle Team Valley Depot", "54.9280,-1.6100");
        return Collections.unmodifiableMap(gazetteer);
    }

    /**
     * The depots this service can resolve, in a fixed order - what a parcel scan's depot name has to be
     * one of.
     */
    public static List<String> knownDepots() {
        return UniLists.copyOf(new ArrayList<>(GAZETTEER.keySet()));
    }

    /**
     * Resolves a depot name to "latitude,longitude", blocking for the service's latency first.
     *
     * @param depot the depot named on the scan, one of {@link #knownDepots()}
     * @return the depot's coordinate as "latitude,longitude"
     * @throws IllegalArgumentException      if {@code depot} is blank, or names no depot in the gazetteer
     * @throws GeocodeUnavailableException   on the calls the failure fraction says should fail
     */
    public String lookup(String depot) {
        if (depot == null || depot.trim().isEmpty()) {
            throw new IllegalArgumentException("depot must be set - it is what the scan is geocoded from");
        }
        try {
            return geocodeApi.call(() -> resolve(depot));
        } catch (SimulatedService.SimulatedFailureException e) {
            // Rethrown, never swallowed. Swallowing it here would commit the scan's offset as though the
            // lookup had succeeded, and Parallel Consumer's retry - the thing this example exists to show
            // - would never happen.
            throw new GeocodeUnavailableException("geocode lookup for '" + depot + "' failed", e);
        }
    }

    private static String resolve(String depot) {
        String coordinate = GAZETTEER.get(depot);
        if (coordinate == null) {
            // A real geocoder only tells you this after the round trip, so this check lives inside the
            // call, behind the latency - not in front of it.
            throw new IllegalArgumentException("no gazetteer entry for depot '" + depot
                    + "' - known depots: " + GAZETTEER.keySet());
        }
        return coordinate;
    }

    /**
     * How long every lookup blocks for. The run summary reports it as the simulated latency.
     */
    public Duration getLatency() {
        return geocodeApi.getLatency();
    }

    /**
     * Lookups attempted, including the ones that failed and the retries of them.
     */
    public long getCallCount() {
        return geocodeApi.getCallCount().get();
    }

    /**
     * Lookups that failed, i.e. the number of times Parallel Consumer was given something to retry.
     */
    public long getFailureCount() {
        return geocodeApi.getFailureCount().get();
    }

    /**
     * The geocoding API being unavailable for one scan. An ordinary unchecked exception, so Parallel
     * Consumer's retry path sees exactly what a real outage would look like.
     */
    public static class GeocodeUnavailableException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public GeocodeUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
