package io.confluent.csid.utils;

import org.assertj.core.api.Assertions;
import org.assertj.core.presentation.StandardRepresentation;
import org.junit.jupiter.api.Test;

import java.text.NumberFormat;

public class StringTestUtils {

    public static final StandardRepresentation STANDARD_REPRESENTATION = new StandardRepresentation();

    public static String pretty(Object properties) {
        return STANDARD_REPRESENTATION.toStringOf(properties);
    }

    public static String format(Number value) {
        return NumberFormat.getNumberInstance().format(value);
    }

    @Test
    void checkFormat() {
        Assertions.assertThat(format(5000)).isEqualTo("5,000");
    }
}
