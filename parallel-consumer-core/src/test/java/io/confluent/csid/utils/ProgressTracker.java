package io.confluent.csid.utils;

import io.confluent.parallelconsumer.InternalRuntimeError;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;

@RequiredArgsConstructor
public class ProgressTracker {

    public final AtomicInteger processedCount;
    AtomicInteger lastSeen = new AtomicInteger(0);
    AtomicInteger rounds = new AtomicInteger(0);
    final int roundsAllowed = 3;
    final int coldRoundsAllowed = 20;

    public boolean checkForProgress() {
        boolean progress = processedCount.get() > lastSeen.get();
        boolean warmedUp = processedCount.get() > 0;
        boolean enoughAttempts = rounds.get() > roundsAllowed;
        if (warmedUp && !progress && enoughAttempts) {
            return true;
        } else if (progress) {
            rounds.set(0);
        } else if (!warmedUp && rounds.get() > coldRoundsAllowed) {
            return true;
        }
        lastSeen.set(processedCount.get());
        rounds.incrementAndGet();
        return false;
    }

    public Exception getError() {
        return new InternalRuntimeError(msg("No progress beyond {} records after {} rounds", processedCount, rounds));
    }
}
