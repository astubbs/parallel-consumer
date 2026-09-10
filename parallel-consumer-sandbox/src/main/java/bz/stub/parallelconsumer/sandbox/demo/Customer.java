package bz.stub.parallelconsumer.sandbox.demo;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Who an {@link Order} belongs to and where a {@link Parcel} is going. Immutable, like {@link Parcel}.
 */
@InterfaceStability.Unstable
public final class Customer {

    private final String customerId;

    private final String fullName;

    private final String email;

    private final String phone;

    private final String city;

    private final String country;

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

    @Override
    public String toString() {
        return "Customer(" + customerId + ", " + fullName + ", " + email + ", " + city + ", " + country + ")";
    }
}
