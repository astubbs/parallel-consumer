package io.confluent.parallelconsumer.examples.connect;

import lombok.experimental.Delegate;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.mockito.Mockito;

public class PCSinkTaskContext implements SinkTaskContext {
    @Delegate
    private final SinkTaskContext delegate = Mockito.mock(SinkTaskContext.class);
}
