package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.state.Batch;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@Slf4j
public class Batcher<T> {

    int maxBatchSize = 1;

    // todo rename
    public List<Batch<T>> makeBatchesAsBatch(List<T> workToProcess) {
        return makeBatches(workToProcess).stream().map(Batch::new).collect(Collectors.toList());
    }

    public List<List<T>> makeBatches(List<T> workToProcess) {
        var batches = partition(workToProcess, maxBatchSize);

        // debugging
        if (log.isDebugEnabled()) {
            var sizes = batches.stream().map(List::size).sorted().collect(Collectors.toList());
            log.debug("Number batches: {}, smallest {}, sizes {}", batches.size(), sizes.stream().findFirst().get(), sizes);
            List<Integer> integerStream = sizes.stream().filter(x -> x < maxBatchSize).collect(Collectors.toList());
            if (integerStream.size() > 1) {
                log.warn("More than one batch isn't target size: {}. Input number of batches: {}", integerStream, batches.size());
            }
        }

        return batches;
    }

    private static <T> List<List<T>> partition(Collection<T> sourceCollection, int maxBatchSize) {
        List<List<T>> listOfBatches = new ArrayList<>();
        List<T> batchInConstruction = new ArrayList<>();

        //
        for (T item : sourceCollection) {
            batchInConstruction.add(item);

            //
            if (batchInConstruction.size() == maxBatchSize) {
                listOfBatches.add(batchInConstruction);
                batchInConstruction = new ArrayList<>(); // todo object allocation warning
            }
        }

        // add partial tail
        if (!batchInConstruction.isEmpty()) {
            listOfBatches.add(batchInConstruction);
        }

        log.debug("sourceCollection.size() {}, batches: {}, batch sizes {}",
                sourceCollection.size(),
                listOfBatches.size(),
                listOfBatches.stream().map(List::size).collect(Collectors.toList()));
        return listOfBatches;
    }

}
