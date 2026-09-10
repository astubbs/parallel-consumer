package bz.stub.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * What a processed {@link Order} becomes when a route produces: the second type in the README's produced-types
 * example, so that a route with different consumed and produced value types is real code rather than a sketch.
 *
 * @see FluentApiSnippets
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Dispatch {

    private String orderId;

    private String destinationCity;

    private int parcelCount;

    public static Dispatch of(Order order) {
        return new Dispatch(order.getOrderId(), order.getDestinationCity(), order.getParcelCount());
    }
}
