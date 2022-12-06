package io.confluent.parallelconsumer.state;

import io.micrometer.core.instrument.Counter;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Simple formatter for rates
 */
@Value
@RequiredArgsConstructor
public class PCRate {

    Duration upTime;

    double count;

    TimeUnit timeUnit = TimeUnit.SECONDS;

    public PCRate(Duration upTime, Counter failureCounter) {
        this(upTime, failureCounter.count());
    }

    public String toString() {
        var convertedUnit = timeUnit.convert(upTime);
        var rate = count / convertedUnit;
        return String.format("%.2f", rate);
    }

}
