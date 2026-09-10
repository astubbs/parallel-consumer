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
 * generator has to cover. {@link Parcel} is the other.
 * <p>
 * Field names are chosen so the generator's field-name rules recognise them: {@code email} is an email address,
 * {@code totalAmount} is money, {@code placedAtEpochMillis} is a recent time. Rename one to {@code field3} and it
 * becomes a random string, which is the point being demonstrated.
 * <p>
 * <b>The time is epoch millis rather than an {@link java.time.Instant}</b>, and that is a workaround, not a
 * preference: the fluent API's {@code json(...)} helper builds a bare Jackson {@code ObjectMapper}, which cannot
 * write a {@code java.time} value without {@code jackson-datatype-jsr310} registered, and fails the record with
 * {@code SerializationException: Could not write JSON}. A demo type is the wrong place to be teaching that, so it
 * sidesteps it; {@link Parcel} carries the {@code Instant} the generator's time rules are exercised against.
 */
@InterfaceStability.Unstable
public class Order {

    private String orderId;

    private String customerName;

    private String email;

    private String city;

    private String status;

    private BigDecimal totalAmount;

    private int itemCount;

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

    @Override
    public String toString() {
        return "Order(" + orderId + ", " + customerName + ", " + city + ", " + status + ", " + totalAmount
                + ", items=" + itemCount + ", placedAt=" + java.time.Instant.ofEpochMilli(placedAtEpochMillis) + ")";
    }
}
