package bz.stub.parallelconsumer.sandbox.demo;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Instant;

/**
 * One parcel in the logistics network.
 * <p>
 * <b>Immutable, with no no-argument constructor</b> - the second hydration path the generator has to cover, and
 * the one a reflection filler cannot take: there are no setters to call and no instance to call them on until the
 * constructor has already run. Instancio builds it through the constructor instead.
 * <p>
 * A JSON route over this type would generate and serialise fine and then fail to read back, because Jackson needs
 * a creator it can name and this class carries no annotations. That is a property of the type, not of the
 * sandbox: use {@link Order} for a JSON route and this one where the point is the constructor.
 */
@InterfaceStability.Unstable
public final class Parcel {

    private final String trackingNumber;

    private final String recipientName;

    private final String deliveryAddress;

    private final String city;

    private final String postcode;

    private final double weightKg;

    private final String status;

    private final Instant collectedAt;

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
     * a JSON route - so the generator's time rules are exercised without also exercising the bare
     * {@code ObjectMapper} limitation {@link Order} records.
     */
    public Instant getCollectedAt() {
        return collectedAt;
    }

    @Override
    public String toString() {
        return "Parcel(" + trackingNumber + ", " + recipientName + ", " + deliveryAddress + ", " + city + " "
                + postcode + ", " + weightKg + "kg, " + status + ", " + collectedAt + ")";
    }
}
