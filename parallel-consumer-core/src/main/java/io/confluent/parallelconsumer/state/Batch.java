package io.confluent.parallelconsumer.state;

import lombok.AllArgsConstructor;
import lombok.Value;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@Value
@AllArgsConstructor
public class Batch<K, V> {

    List<WorkContainer<K, V>> values;

    public Batch(WorkContainer<K, V> element) {
        this.values = UniLists.of(element);
    }

}
