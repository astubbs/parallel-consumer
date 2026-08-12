package bz.stub.parallelconsumer.examples.metrics;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.examples.support.SimulatedService;
import lombok.extern.slf4j.Slf4j;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.List;

/**
 * Allocates stock for an order against a warehouse management system - the slow, occasionally
 * unsuccessful call that {@link OrderFulfilmentApp} is built around.
 * <p>
 * Example scaffolding, not a library type - and deliberately outside {@link OrderFulfilmentApp}'s tagged
 * (copied) region.
 * <p>
 * <b>The call blocks, and that is the point.</b> The latency is a {@code sleep} inside
 * {@link SimulatedService}, which is a faithful model of what a warehouse management system's SOAP or
 * JDBC client does to the calling thread. Parallel Consumer's value proposition is precisely that it
 * gives you a pool of threads to park those blocked calls on, so one slow allocation does not stall the
 * whole partition. Simulating it non-blockingly would demonstrate the opposite of what the reader came
 * to see.
 * <p>
 * <b>Failures are deterministic, and never swallowed.</b> A fixed fraction of allocations fail (every
 * Nth call, see {@link SimulatedService}), and the failure leaves this class as a
 * {@link StockUnavailableException} which propagates out of the user function. That is what makes both
 * Parallel Consumer's retry behaviour and the example's {@code outcome}-tagged failure counter visible:
 * the order is re-processed rather than dropped, and its offset is not committed until it succeeds.
 * <p>
 * <b>{@link #allocate(String, String, String)} is overridable on purpose.</b> That is the seam a test
 * uses to hold N allocations at a {@link java.util.concurrent.CyclicBarrier} simultaneously, which is
 * how the concurrency this example claims is proved rather than timed.
 */
@Slf4j
public class FulfilmentService {

    /**
     * What one warehouse allocation costs. Simulated with a sleep; the concurrency around it is real.
     */
    public static final Duration DEFAULT_ALLOCATION_LATENCY = Duration.ofMillis(80);

    /**
     * One allocation in ten finds the stock already committed to another order. High for a real
     * warehouse, chosen so a short example run actually shows a retry rather than hoping one turns up.
     */
    public static final double DEFAULT_OUT_OF_STOCK_FRACTION = 0.1d;

    /**
     * The distribution centres orders can be allocated from. A small, <em>fixed</em> set, because it is
     * used as a metric tag value - see {@link OrderFulfilmentApp#allocationTimer(String)}.
     */
    private static final List<String> WAREHOUSES =
            UniLists.of("Peterborough NDC", "Lutterworth RDC", "Warrington RDC");

    private final SimulatedService warehouseManagementSystem;

    /**
     * The service the example runs with: {@link #DEFAULT_ALLOCATION_LATENCY} per allocation, failing
     * {@link #DEFAULT_OUT_OF_STOCK_FRACTION} of calls.
     */
    public FulfilmentService() {
        this(DEFAULT_ALLOCATION_LATENCY, DEFAULT_OUT_OF_STOCK_FRACTION);
    }

    public FulfilmentService(Duration allocationLatency, double outOfStockFraction) {
        this.warehouseManagementSystem =
                new SimulatedService("warehouse management system", allocationLatency, outOfStockFraction);
    }

    /**
     * The distribution centres this service allocates from, in a fixed order.
     */
    public static List<String> knownWarehouses() {
        return WAREHOUSES;
    }

    /**
     * Which distribution centre serves a customer. Deterministic, and - crucially for the meters - drawn
     * from a <em>bounded</em> set, so tagging a meter with the result cannot make the time series count
     * grow with traffic.
     *
     * @param customerId the order's key
     * @return one of {@link #knownWarehouses()}
     */
    public static String warehouseFor(String customerId) {
        if (customerId == null || customerId.trim().isEmpty()) {
            throw new IllegalArgumentException("customerId must be set - it is what routes an order to a warehouse");
        }
        return WAREHOUSES.get(Math.floorMod(customerId.hashCode(), WAREHOUSES.size()));
    }

    /**
     * Allocates stock for one fulfilment stage of one order, blocking for the warehouse management
     * system's latency first.
     *
     * @param customerId the order's key
     * @param stage      the fulfilment stage being allocated, one of
     *                   {@link OrderFulfilmentApp#FULFILMENT_STAGES}
     * @param warehouse  the distribution centre, from {@link #warehouseFor(String)}
     * @return the warehouse management system's allocation reference
     * @throws IllegalArgumentException  if any argument is blank, or {@code warehouse} is not a known one
     * @throws StockUnavailableException on the calls the out-of-stock fraction says should fail
     */
    public String allocate(String customerId, String stage, String warehouse) {
        requireSet("customerId", customerId);
        requireSet("stage", stage);
        requireSet("warehouse", warehouse);
        try {
            return warehouseManagementSystem.call(() -> reserve(customerId, stage, warehouse));
        } catch (SimulatedService.SimulatedFailureException e) {
            // Rethrown, never swallowed. Swallowing it here would commit the order's offset as though the
            // stock had been allocated, and Parallel Consumer's retry - one of the two things this example
            // exists to make visible on a dashboard - would never happen.
            throw new StockUnavailableException("stock for " + customerId + " at stage " + stage
                    + " could not be allocated from " + warehouse, e);
        }
    }

    private static String reserve(String customerId, String stage, String warehouse) {
        if (!WAREHOUSES.contains(warehouse)) {
            // A real warehouse management system only rejects an unknown site after the round trip, so
            // this check lives inside the call, behind the latency - not in front of it.
            throw new IllegalArgumentException("no such distribution centre '" + warehouse
                    + "' - known warehouses: " + WAREHOUSES);
        }
        return "ALLOC-" + warehouse.charAt(0) + stage.charAt(0) + "-" + customerId;
    }

    private static void requireSet(String name, String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must be set");
        }
    }

    /**
     * How long every allocation blocks for. The run summary reports it as the simulated latency.
     */
    public Duration getAllocationLatency() {
        return warehouseManagementSystem.getLatency();
    }

    /**
     * Allocations attempted, including the ones that failed and the retries of them.
     */
    public long getCallCount() {
        return warehouseManagementSystem.getCallCount().get();
    }

    /**
     * Allocations that failed, i.e. the number of times Parallel Consumer was given something to retry.
     * The example's {@code outcome="out_of_stock"} counter should agree with this figure.
     */
    public long getFailureCount() {
        return warehouseManagementSystem.getFailureCount().get();
    }

    /**
     * The warehouse having no stock to allocate for one order. An ordinary unchecked exception, so
     * Parallel Consumer's retry path sees exactly what a real allocation failure would look like.
     */
    public static class StockUnavailableException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public StockUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
