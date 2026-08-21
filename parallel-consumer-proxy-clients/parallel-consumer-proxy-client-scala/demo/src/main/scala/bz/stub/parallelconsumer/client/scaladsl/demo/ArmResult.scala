// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl.demo

import scala.concurrent.duration.FiniteDuration

/**
 * What one arm achieved: how long it took, over how many records, and across how many distinct keys.
 *
 * ==`processed` and `keys` are the deterministic pair, and that is why they are here==
 *
 * Throughput alone cannot show the work happened - a short arm reports a flattering rate, and a rate
 * is all a reader would have to judge it by. `processed` must equal the target, so an arm that fell
 * short is visibly a failed arm rather than a fast one; `keys` is the distinct keys the arm actually
 * saw, which shows the backlog was spread rather than one key repeated.
 *
 * '''Unlike `elapsed` and the rate, both are the same in every language''' - the same records, the
 * same key space, so the same two figures. That is what makes the two tables comparable across
 * eleven demos when no timing ever could be, and it is what `bin/ci-demo-conformance.sh` leans on.
 *
 * '''There is no latency field, and its absence is the contract rather than an omission.''' The
 * backlog is pre-produced, so the workload is closed-loop and a per-record timing would be flattered
 * by however far an arm had fallen behind. Throughput is the only honest number this shape can
 * produce, so it is the only one there is a place to put.
 */
private[demo] final case class ArmResult(
    arm: String,
    elapsed: FiniteDuration,
    processed: Int,
    keys: Int) {

  def ratePerSecond: Double = {
    val seconds = elapsed.toNanos / 1000000000d
    if (seconds > 0) processed / seconds else 0d
  }
}
