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
public class Batch<T> {

    List<T> values;

    public Batch(T element) {
        this.values = UniLists.of(element);
    }

}
