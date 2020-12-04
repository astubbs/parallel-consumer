package io.confluent.csid.utils;

import io.confluent.parallelconsumer.InternalRuntimeError;
import lombok.experimental.UtilityClass;

@UtilityClass
public class JavaUtils {
    public static int safeCast(final long aLong) {
        int castLong = (int) aLong;
        if (castLong != aLong) throw new InternalRuntimeError("Casting error");
        return castLong;
    }
}
