package bz.stub.parallelconsumer.sandbox.demo;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Instant;

/**
 * One parcel in the logistics network.
 * <p>
 * <b>Immutable, with no no-argument constructor</b> - the second hydration path the hydration has to cover, and
 * the one a reflection filler cannot take: there are no setters to call and no instance to call them on until the
 * constructor has already run. Instancio builds it through the constructor instead.
 * <p>
 * A JSON route over this type would generate and serialise fine and then fail to read back, because Jackson needs
 * a creator it can name and this class carries no annotations. That is a property of the type, not of the
 * sandbox: use {@link Order} for a JSON route and this one where the point is the constructor.
 * <p>
 * <b>The getters below carry no documentation of their own</b>, for the reason {@link Order} gives: each returns
 * the field above it, and what is worth knowing is written there.
 */
@InterfaceStability.Unstable
public final class Parcel {

    /**
     * The PC-and-ten-digits shape the tracking rule produces, which is also what {@link Dispatch} carries its
     * parcels as.
     */
    private final String trackingNumber;

    /**
     * A person's full name, from the name rule.
     */
    private final String recipientName;

    /**
     * A street address, from the address rule.
     */
    private final String deliveryAddress;

    /**
     * A real city name.
     */
    private final String city;

    /**
     * A postcode, from the address rules - UK-shaped, because the hydration's faker is built with a UK locale.
     */
    private final String postcode;

    /**
     * A parcel-sized weight in kilograms, from the weight rule - which exists precisely so that a double field
     * whose name says "weight" is not filled as money.
     */
    private final double weightKg;

    /**
     * One of the demo domain's parcel statuses, as {@link Order}'s is.
     */
    private final String status;

    /**
     * <b>The {@link Instant} the hydration's time rules are exercised against</b> - the reason this type carries a
     * java.time value where {@link Order} carries epoch millis. Within the month before the hydration's fixed
     * "now", so a seed reproduces it.
     */
    private final Instant collectedAt;

    /**
     * The only way to build one, and the whole point of the type: Instancio has to decide every value before it
     * can construct the object, where a bean is constructed first and filled afterwards.
     */
    public Parcel(String trackingNumber,
                  String recipientName,
                  String deliveryAddress,
                  String city,
                  String postcode,
                  double weightKg,
                  String status,
                  Instant collectedAt) {
        this.trackingNumber = trackingNumber;
        this.recipientName = recipientName;
        this.deliveryAddress = deliveryAddress;
        this.city = city;
        this.postcode = postcode;
        this.weightKg = weightKg;
        this.status = status;
        this.collectedAt = collectedAt;
    }

    public String getTrackingNumber() {
        return trackingNumber;
    }

    public String getRecipientName() {
        return recipientName;
    }

    public String getDeliveryAddress() {
        return deliveryAddress;
    }

    public String getCity() {
        return city;
    }

    public String getPostcode() {
        return postcode;
    }

    public double getWeightKg() {
        return weightKg;
    }

    public String getStatus() {
        return status;
    }

    /**
     * The one {@code java.time} field in the demo domain, and deliberately on the type that is never sent through
     * a JSON route - so the hydration's time rules are exercised without also exercising the bare
     * {@code ObjectMapper} limitation {@link Order} records.
     */
    public Instant getCollectedAt() {
        return collectedAt;
    }

    /**
     * Names the parcel and where it is going, which is what a failing assertion about a generated parcel needs.
     */
    @Override
    public String toString() {
        return "Parcel(" + trackingNumber + ", " + recipientName + ", " + deliveryAddress + ", " + city + " "
                + postcode + ", " + weightKg + "kg, " + status + ", " + collectedAt + ")";
    }
}
