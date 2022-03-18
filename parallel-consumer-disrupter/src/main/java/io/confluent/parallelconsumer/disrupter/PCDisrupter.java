package io.confluent.parallelconsumer.disrupter;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @see AbstractParallelEoSStreamProcessor
 */
public class PCDisrupter<K, V> { //extends AbstractParallelEoSStreamProcessor<K, V> {

    public class LongEvent {
        private long value;

        public void set(long value) {
            this.value = value;
        }
    }

    Map<Thread, RingBuffer<LongEvent>> rb;

    @SneakyThrows
    PCDisrupter() {
//        super();
        AtomicLong atomicLong = new AtomicLong();
//        atomicLong.set();
//        VarHandle varHandle = new VarHandle();
//        varHandle.setVolatile();
//        Disruptor

        int bufferSize = 1024;

        Disruptor<LongEvent> disruptor =
                new Disruptor<>(LongEvent::new, bufferSize, DaemonThreadFactory.INSTANCE);

        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            System.out.println("Event: " + event))
            long sequence1 = sequence;
            boolean endOfBatch1 = endOfBatch;
        });
        disruptor.start();


        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();
        ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; true; l++) {
            bb.putLong(0, l);
            ringBuffer.publishEvent((event, sequence, buffer) -> event.set(buffer.getLong(0)), bb);
            Thread.sleep(1000);
        }
    }

    void getWork(Thread t) {
        RingBuffer<LongEvent> longEventRingBuffer = rb.get(t);
        long cursor = longEventRingBuffer.getCursor();
        LongEvent longEvent = longEventRingBuffer.get(4L);
    }
}
