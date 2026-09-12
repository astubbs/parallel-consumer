package bz.stub.parallelconsumer.sandbox.demo;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.math.BigDecimal;

/**
 * An order placed by a customer - the sandbox's default demo type, and the one a quickstart route consumes.
 * <p>
 * A plain bean with setters, on purpose: it is the shape most Kafka payload classes have, it is what a JSON
 * deserialiser can read back with no annotations and no module, and it is one of the two hydration paths the
 * hydration has to cover. {@link Parcel} is the other.
 * <p>
 * Field names are chosen so the hydration's field-name rules recognise them: {@code email} is an email address,
 * {@code totalAmount} is money, {@code placedAtEpochMillis} is a recent time. Rename one to {@code field3} and it
 * becomes a random string, which is the point being demonstrated.
 * <p>
 * <b>The time is epoch millis rather than an {@link java.time.Instant}</b>, and that is a workaround, not a
 * preference: the fluent API's {@code json(...)} helper builds a bare Jackson {@code ObjectMapper}, which cannot
 * write a {@code java.time} value without {@code jackson-datatype-jsr310} registered, and fails the record with
 * {@code SerializationException: Could not write JSON}. A demo type is the wrong place to be teaching that, so it
 * sidesteps it; {@link Parcel} carries the {@code Instant} the hydration's time rules are exercised against.
 * <p>
 * <b>The accessors below carry no documentation of their own, deliberately.</b> Each one reads or writes the
 * field above it and does nothing else; what is worth knowing about {@code city} is on the field, and repeating it
 * as "@return the city" on the getter would be noise between a reader and the four classes this package exists to
 * show off.
 */
@InterfaceStability.Unstable
public class Order {

    /**
     * Filled with a UUID by the identifier rule, which claims any field whose name mentions an id or a reference.
     */
    private String orderId;

    /**
     * A person's full name, from the name rule - two words, which is what the quickstart's console output shows.
     */
    private String customerName;

    /**
     * An actual email address, from the email rule. The field this type exists to demonstrate: rename it and the
     * value becomes a random string, which is the whole of the point about field-name-aware generation.
     */
    private String email;

    /**
     * A real city name, from the address rules.
     */
    private String city;

    /**
     * One of the demo domain's parcel statuses, drawn from a closed set - so a route can filter on one of them and
     * actually see records both match and not match.
     */
    private String status;

    /**
     * Money: two decimal places, in a plausible range, rather than the nine-digit number a general-purpose filler
     * would produce.
     */
    private BigDecimal totalAmount;

    /**
     * A plausible number of items, from the quantity rule - a dozen at most, not a nine-digit int.
     */
    private int itemCount;

    /**
     * When the order was placed, as epoch millis for the Jackson reason in this class's own documentation, and
     * within the month before the hydration's fixed "now" so that a seeded run reproduces it.
     */
    private long placedAtEpochMillis;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public int getItemCount() {
        return itemCount;
    }

    public void setItemCount(int itemCount) {
        this.itemCount = itemCount;
    }

    public long getPlacedAtEpochMillis() {
        return placedAtEpochMillis;
    }

    public void setPlacedAtEpochMillis(long placedAtEpochMillis) {
        this.placedAtEpochMillis = placedAtEpochMillis;
    }

    /**
     * Rendered into the quickstart's console output and into a test's failure message, so it names the fields a
     * reader would check rather than the object's identity.
     */
    @Override
    public String toString() {
        return "Order(" + orderId + ", " + customerName + ", " + city + ", " + status + ", " + totalAmount
                + ", items=" + itemCount + ", placedAt=" + java.time.Instant.ofEpochMilli(placedAtEpochMillis) + ")";
    }
}
