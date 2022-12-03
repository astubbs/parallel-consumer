package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Comparator;

import static io.confluent.csid.utils.KafkaUtils.toTopicPartition;

/**
 * Simple value class for processing {@link ShardKey}s to make the various key systems type safe and extendable.
 *
 * @author Antony Stubbs
 */
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
public abstract class ShardKey<T> implements Comparable<T> {

    static Comparator<TopicPartition> tpComparator = Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition);

    public static <T extends ShardKey<T>> ShardKey<T> of(WorkContainer<?, ?> wc, ProcessingOrder ordering) {
        return of(wc.getCr(), ordering);
    }

    public static <T extends ShardKey<T>> ShardKey<T> of(ConsumerRecord<?, ?> rec, ProcessingOrder ordering) {
        return switch (ordering) {
            case KEY -> ofKey(rec);
            case PARTITION, UNORDERED -> ofTopicPartition(rec);
        };
    }

    public static <T extends ShardKey<T>> ShardKey<T> ofKey(ConsumerRecord<?, ?> rec) {
        // todo casts
        return (ShardKey) new KeyOrderedKey(rec);
    }

    public static <T extends ShardKey<T>> ShardKey<T> ofTopicPartition(ConsumerRecord<?, ?> rec) {
        // todo casts
        return (ShardKey) new TopicPartitionKey(toTopicPartition(rec));
    }

    @Value
    @RequiredArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class KeyOrderedKey extends ShardKey<KeyOrderedKey> {

        /**
         * Note: We use just the topic name here, and not the partition, so that if we were to receive records from the
         * same key from the partitions we're assigned, they will be put into the same queue.
         */
        TopicPartition topicName;

        /**
         * The key of the record being referenced. Nullable if record is produced with a null key.
         */
        Object key;

        static Comparator<Object> keyComparator = Comparator.nullsFirst(Comparator.comparing(Object::toString));


        public KeyOrderedKey(final ConsumerRecord<?, ?> rec) {
            this(new TopicPartition(rec.topic(), rec.partition()), rec.key());
        }

        public static KeyOrderedKey of(ConsumerRecord<?, ?> rec) {
            return new KeyOrderedKey(rec);
        }

        @Override
        public int compareTo(@NonNull KeyOrderedKey other) {
            return Comparator.comparing(KeyOrderedKey::getTopicName, tpComparator)
                    .thenComparing(KeyOrderedKey::getKey, keyComparator)
                    .compare(this, other);
        }
    }

    @Value
    @EqualsAndHashCode(callSuper = true)
    public static class TopicPartitionKey extends ShardKey<TopicPartitionKey> {

        TopicPartition topicPartition;

        public static TopicPartitionKey of(final ConsumerRecord<?, ?> rec) {
            return new TopicPartitionKey(new TopicPartition(rec.topic(), rec.partition()));
        }

        @Override
        public int compareTo(@NonNull TopicPartitionKey other) {
            return Comparator.comparing(TopicPartitionKey::getTopicPartition, tpComparator)
                    .compare(this, other);
        }
    }

}
