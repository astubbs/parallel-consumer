package io.confluent.parallelconsumer.state;

import java.util.Queue;

/**
 * @author Antony Stubbs
 */
public interface Queueable<T> {

    Queue<T> queue();
}
