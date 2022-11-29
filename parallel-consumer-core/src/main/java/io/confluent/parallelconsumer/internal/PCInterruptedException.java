package io.confluent.parallelconsumer.internal;

import lombok.experimental.StandardException;

/**
 * So we can include context when interrupted
 *
 * @author Antony Stubbs
 */
@StandardException
public class PCInterruptedException extends InterruptedException {
}
