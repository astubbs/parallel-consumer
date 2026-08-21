// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl.demo

import scala.concurrent.duration.FiniteDuration

/**
 * What one arm achieved: how long it took, and over how many records.
 *
 * '''There is no latency field, and its absence is the contract rather than an omission.''' The
 * backlog is pre-produced, so the workload is closed-loop and a per-record timing would be flattered
 * by however far an arm had fallen behind. Throughput is the only honest number this shape can
 * produce, so it is the only one there is a place to put.
 */
private[demo] final case class ArmResult(arm: String, elapsed: FiniteDuration, processed: Int) {

  def ratePerSecond: Double = {
    val seconds = elapsed.toNanos / 1000000000d
    if (seconds > 0) processed / seconds else 0d
  }
}
