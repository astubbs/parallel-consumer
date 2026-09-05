# Demo: catching up on a queue of card payments

A card issuer screens every authorisation for fraud by calling a scoring service. The screener has been
down, authorisations have piled up, and it has just come back. You will see the same queue cleared
three times - twice by ordinary Kafka Streams, and once with this module letting one partition work on
several cards at the same time - and the difference is how long the backlog takes to disappear.

## Run it

Takes about **fifteen minutes**, and needs Docker running.

```bash
bin/streams-benchmark.sh --scenario payments
```

## What you should see

Kafka Streams logs a lot. The result is a framed block near the end - search the output for
`Card-payment authorisation screening`. It has this shape:

```
==============================================================================
  Card-payment authorisation screening - catching up after an outage
==============================================================================
CONFIGURATION
  authorisations queued  ...
  distinct cards         ... (Zipf s=1.0, so a few cards are hot)      <-- (3)
  fraud scoring call     ...ms p50, ...ms p99
  partitions / threads   1 / 1
  worker pool            4

  MEASUREMENT                             STOCK             PC  PC vs STOCK
  ========================================================================
  NOISE FLOOR (two stock arms)             .../s          .../s        ....x  <-- (0)
  authorisations / second                  .../s          .../s        ....x  <-- (1)
  time to clear the queue                    ...s           ...s       ....x  <-- (2)
```

**No numbers are printed here on purpose.** A figure written into documentation describes the machine
it was measured on, and quietly stops describing yours. The run prints its own, and those are the ones
to quote.

**(0) is what you read everything else against.** Those two arms are *both* ordinary Kafka Streams,
given the identical records. Whatever they differ by is your machine, not this module - so a headline
close to that number is not a result. On an idle machine it sits near 1.0x; on a busy one it does not,
and then the run has told you to be sceptical of itself.

**(1) is the headline.** The `STOCK` column is ordinary Kafka Streams. The `PC` column is the same
code with this module enabled. The scoring call takes exactly as long in both runs - the only
difference is how many are being scored at the same time.

**(2) is the same fact in the unit you would actually act on**: how long until the backlog is gone.

**(3) is why the number is not higher.** One card's authorisations must still be screened strictly in
order, and the cards are skewed, so a few busy cards carry much of the queue and still go one at a
time. The distinct-card count is the ceiling on how much could have run at once.

If your headline lands inside the noise floor, either Docker has too little CPU - both columns will be
starved - or you have run one of the variations below that this module genuinely does not help with.
Both are real answers, and the run says which by printing the floor beside the headline.

## Try something else

- `--skew 2.0` - a few cards much hotter. The advantage shrinks. This is the honest limit.
- `--skew 0` - cards spread evenly. It grows, towards the worker pool size.
- `--blocking-fraction 0` - local computation instead of the scoring call. Different answer.
- `--records 3000` - a deeper backlog: more time saved, same rate.
- `--pool 1` - **the negative control.** One worker means PC dispatch has no concurrency to exploit, so
  the headline should collapse into the noise floor and the run should FAIL its own assertion. If it
  passes, the assertion is not checking anything.
- `--help` - every parameter, with its default.

## Then read

- [The module README](README.md) - what this is, how to switch it off, its limitations, and the
  seam-on evidence lane that classifies what Kafka's own suite says about this path.
