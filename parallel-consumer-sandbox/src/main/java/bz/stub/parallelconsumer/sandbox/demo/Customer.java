package bz.stub.parallelconsumer.sandbox.demo;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Who an {@link Order} belongs to and where a {@link Parcel} is going. Immutable, like {@link Parcel}.
 * <p>
 * <b>Nothing in this module references it, and that is not an oversight.</b> These four types are the demo domain
 * a user routes on - a published module's payload vocabulary, not internal scaffolding - and this package's
 * documentation counts two beans and two immutables, of which this is one. It is the type to declare on a route
 * when what you want hydrated is a person rather than an order.
 * <p>
 * <b>The getters below carry no documentation of their own</b>, for the reason {@link Order} gives.
 */
@InterfaceStability.Unstable
public final class Customer {

    /**
     * A UUID from the identifier rule.
     */
    private final String customerId;

    /**
     * A person's full name, from the name rule.
     */
    private final String fullName;

    /**
     * An actual email address, from the email rule.
     */
    private final String email;

    /**
     * A phone number, from the phone rule - which is ahead of the identifier rule in the table, so a field called
     * {@code phone} is not filled with a UUID.
     */
    private final String phone;

    /**
     * A real city name, from the address rules.
     */
    private final String city;

    /**
     * A country name, from the address rules.
     */
    private final String country;

    /**
     * The only way to build one - see {@link Parcel}, which is the other constructor-filled type here.
     */
    public Customer(String customerId, String fullName, String email, String phone, String city, String country) {
        this.customerId = customerId;
        this.fullName = fullName;
        this.email = email;
        this.phone = phone;
        this.city = city;
        this.country = country;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getFullName() {
        return fullName;
    }

    public String getEmail() {
        return email;
    }

    public String getPhone() {
        return phone;
    }

    public String getCity() {
        return city;
    }

    public String getCountry() {
        return country;
    }

    /**
     * Names the customer and where they are, leaving the phone number out - it is the one field a log line rarely
     * wants and a screenshot never should.
     */
    @Override
    public String toString() {
        return "Customer(" + customerId + ", " + fullName + ", " + email + ", " + city + ", " + country + ")";
    }
}
