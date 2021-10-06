package io.confluent.parallelconsumer;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import io.github.bucket4j.ConsumptionProbe;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;

@Slf4j
class RateLimitTest {

  @Test
  void test() {
    // define the limit 100 times per 1 minute
    Bandwidth limit = Bandwidth.simple(100, Duration.ofMinutes(1));
    // construct the bucket
    Bucket bucket = Bucket4j.builder().addLimit(limit).build();

    double exchangeRate;

    // do polling in infinite loop
    while (true) {
      ConsumptionProbe consumptionProbe = bucket.tryConsumeAndReturnRemaining(1);
      long remainingTokens = consumptionProbe.getRemainingTokens();

      // Consume a token from the token bucket.
      bucket.asVerbose().getAvailableTokens();
      // If a token is not available this method will block until the refill adds one to the bucket.
//      bucket.asScheduler().consume(1);
      log.info("Calling");
    }
  }
}
