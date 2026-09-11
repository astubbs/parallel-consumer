package bz.stub.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The quickstart's JSON value type: a parcel-logistics order, small enough to read in a console.
 * <p>
 * A plain mutable bean with a no-argument constructor, because that is what Jackson reads into with no annotations
 * at all. Nothing about the fluent API requires this shape - a route's value type is whatever its deserialiser
 * produces - but a quickstart that needs a {@code @JsonCreator} before it runs is teaching Jackson rather than
 * Parallel Consumer.
 * <p>
 * <b>The field names are chosen, not arbitrary.</b> The sandbox's generator fills a value by field <em>name</em>,
 * so {@code status} arrives as one of the parcel statuses it knows, {@code parcelCount} as a plausible small
 * quantity and {@code destinationCity} as a city - which is what lets the quickstart's filtered outcome fire on
 * generated data rather than sit as a branch nothing ever takes.
 *
 * @see FluentQuickstartApp
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    private String orderId;

    private String customerId;

    private String destinationCity;

    private int parcelCount;

    /**
     * Where the order is in its life: {@code CREATED}, {@code COLLECTED}, {@code IN_TRANSIT},
     * {@code OUT_FOR_DELIVERY}, {@code DELIVERED} or {@code RETURNED}.
     */
    private String status;
}
