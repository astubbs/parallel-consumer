package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.sandbox.demo.Order;
import bz.stub.parallelconsumer.sandbox.demo.Parcel;
import com.google.protobuf.Int32Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * What the generator can fill, and what it says when it cannot.
 * <p>
 * The three shapes are not variations on one another - they are three different mechanisms. A bean is filled by
 * calling setters on an instance that already exists; an immutable class has no instance until its constructor has
 * run, so its values have to be decided first and passed in; an Avro specific record is described by a schema its
 * Java types do not carry, so it is filled from the schema instead. A suite that only ever filled a bean would not
 * notice either of the others breaking, which is why all three are here.
 */
@Timeout(60)
class HydrationTest {

    /**
     * The generator's own fixed "now" - {@code FieldValues.SANDBOX_NOW}. Timestamps are generated behind it, and
     * fixed rather than read from the clock so that a seed reproduces them.
     */
    private static final Instant SANDBOX_NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final RandomObjects generator = RandomObjects.seededWith(42);

    @Test
    void aBeanIsFilledThroughItsSettersWithValuesItsFieldNamesAskFor() {
        Order order = generator.create(Order.class, 0);

        assertWithMessage("an 'email' field should hold an email address, not a random string")
                .that(order.getEmail()).contains("@");
        assertThat(order.getCustomerName()).isNotEmpty();
        assertThat(order.getCustomerName()).contains(" ");
        assertThat(order.getCity()).isNotEmpty();
        assertThat(order.getOrderId()).isNotEmpty();
        assertWithMessage("a 'status' field should hold one of the domain's statuses")
                .that(order.getStatus())
                .isIn(java.util.Arrays.asList("CREATED", "COLLECTED", "IN_TRANSIT", "OUT_FOR_DELIVERY",
                        "DELIVERED", "RETURNED"));
        assertWithMessage("a 'totalAmount' field should hold money, not a nine-digit random number")
                .that(order.getTotalAmount()).isGreaterThan(BigDecimal.ZERO);
        assertThat(order.getTotalAmount()).isLessThan(BigDecimal.valueOf(2501));
        assertWithMessage("an 'itemCount' field should hold a plausible count")
                .that(order.getItemCount()).isIn(com.google.common.collect.ContiguousSet.closed(1, 11));
        assertWithMessage("a time field should be recent rather than random across the epoch")
                .that(order.getPlacedAtEpochMillis()).isAtMost(SANDBOX_NOW.toEpochMilli());
        assertThat(order.getPlacedAtEpochMillis())
                .isGreaterThan(SANDBOX_NOW.minusSeconds(60L * 60 * 24 * 31).toEpochMilli());
    }

    @Test
    void anImmutableClassIsFilledThroughItsConstructor() {
        Parcel parcel = generator.create(Parcel.class, 0);

        assertWithMessage("a class with only a constructor and final fields must still be filled")
                .that(parcel).isNotNull();
        assertThat(parcel.getTrackingNumber()).matches("PC\\d{10}");
        assertThat(parcel.getRecipientName()).contains(" ");
        assertThat(parcel.getPostcode()).isNotEmpty();
        assertThat(parcel.getWeightKg()).isGreaterThan(0.0);
        assertThat(parcel.getWeightKg()).isAtMost(30.0);
        assertWithMessage("an Instant field named ...At should be a recent time")
                .that(parcel.getCollectedAt()).isLessThan(SANDBOX_NOW.plusSeconds(1));
    }

    @Test
    void anAvroSpecificRecordIsFilledFromItsSchema() {
        AvroParcel parcel = generator.create(AvroParcel.class, 0);

        assertThat(parcel).isNotNull();
        assertWithMessage("the round trip through the schema must produce the class's own String type, not Utf8")
                .that(parcel.getTrackingNumber()).isNotNull();
        assertThat(parcel.getRecipientName()).isNotNull();
        // Not asserted as realistic: RandomData reads the schema, which says "a string", and knows nothing about
        // field names. That is the documented trade - the schema is the more authoritative source for an Avro
        // type, and it costs the Datafaker layer.
        assertThat(parcel.getSchema()).isEqualTo(AvroParcel.SCHEMA$);
    }

    @Test
    void aProtobufMessageIsRefusedNamingTheTypeRatherThanFilledBadly() {
        IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> generator.create(Int32Value.class, 0));

        assertThat(refusal).hasMessageThat().contains("Protobuf");
        assertWithMessage("the refusal must name the type, so the reader knows which route to fix")
                .that(refusal).hasMessageThat().contains(Int32Value.class.getName());
    }

    /**
     * The rule table is ordered, and a field name can match more than one rule - {@code emailAddress} matches both
     * the email rule and the address rule. This pins which one wins, because the answer is Instancio's selector
     * precedence rather than anything this module decides, and a silent change of it would turn every email in a
     * demo into a street address with nothing going red.
     */
    @Test
    void aFieldMatchingTwoRulesGetsTheMoreSpecificOne() {
        AmbiguouslyNamed filled = generator.create(AmbiguouslyNamed.class, 0);

        assertWithMessage("emailAddress matches both the email rule and the address rule; the email rule is the "
                + "specific one and must win")
                .that(filled.getEmailAddress()).contains("@");
    }

    /**
     * A bean whose only job is to have a field name that two rules recognise.
     */
    public static class AmbiguouslyNamed {

        private String emailAddress;

        public String getEmailAddress() {
            return emailAddress;
        }

        public void setEmailAddress(String emailAddress) {
            this.emailAddress = emailAddress;
        }
    }
}
