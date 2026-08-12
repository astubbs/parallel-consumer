package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

@UtilityClass
public class StringUtils {

    /**
     * @see MessageFormatter#arrayFormat(String, Object[])
     * @see FormattingTuple#getMessage()
     */
    public static String msg(String s, Object... args) {
        return MessageFormatter.arrayFormat(s, args).getMessage();
    }

    public static boolean isBlank(final String property) {
        if (property == null) return true;
        else return property.trim().isEmpty(); // isBlank @since 11
    }
}
