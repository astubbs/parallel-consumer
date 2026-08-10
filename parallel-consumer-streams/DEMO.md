# Demo: catching up on a queue of card payments

A card issuer screens every authorisation for fraud by calling a scoring service. The screener has been
down, 900 authorisations have piled up, and it has just come back. You will see the same queue cleared
twice - once by ordinary Kafka Streams, and once with this module letting one partition work on several
cards at the same time - and the difference is how long the backlog takes to disappear.

## Run it

Takes about **six minutes**, and needs Docker running.

```bash
bin/streams-benchmark.sh --scenario payments
```

## What you should see

Kafka Streams logs a lot. The result is a framed block near the end - search the output for
`Card-payment authorisation screening`. It looks like this:

```
==============================================================================
  Card-payment authorisation screening - catching up after an outage
==============================================================================
CONFIGURATION
  authorisations queued  900
  distinct cards         123 (Zipf s=1.0, so a few cards are hot)     <-- (3)
  fraud scoring call     60ms p50, 400ms p99
  partitions / threads   1 / 1
  worker pool            4

  MEASUREMENT                             STOCK             PC  PC vs STOCK
  ========================================================================
  authorisations / second                10.8/s         40.6/s        3.76x  <-- (1)
  time to clear the queue                   84s            28s        3.00x  <-- (2)
```

**(1) is the headline.** The `STOCK` column is ordinary Kafka Streams, about eleven authorisations a
second. The `PC` column is the same code with this module enabled, about forty. The scoring call takes
exactly as long in both runs - the only difference is how many are being scored at the same time.

**(2) is the same fact in the unit you would actually act on.** A backlog that took 84 seconds to clear
now takes 28.

**(3) is why the number is not higher.** One card's authorisations must still be screened strictly in
order, and the cards are skewed, so a few busy cards carry much of the queue and still go one at a time.
123 distinct cards is the ceiling on how much could have run at once.

If your run shows about **1.0x**, check Docker has enough CPU - both columns will be starved.

## Try something else

- `--skew 2.0` - a few cards much hotter. The advantage shrinks. This is the honest limit.
- `--skew 0` - cards spread evenly. It grows, towards the worker pool size.
- `--blocking-fraction 0` - local computation instead of the scoring call. Different answer.
- `--records 3000` - a deeper backlog: more time saved, same rate.
- `--help` - every parameter, with its default.

## Then read

- [The full results](../docs/plans/2026-08-11-001-realistic-benchmark-result.md) - including where this
  makes no difference at all, and the predictions it refuted.
- [The module README](README.md) - what this is, how to switch it off, and its limitations.
