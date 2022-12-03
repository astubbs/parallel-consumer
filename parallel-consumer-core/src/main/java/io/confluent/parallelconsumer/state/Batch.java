package io.confluent.parallelconsumer.state;

import lombok.Value;

import java.util.List;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@Value
public class Batch<K, V> {

    List<WorkContainer<K, V>> values;
}
