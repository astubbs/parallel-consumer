// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

/**
 * The per-record verdict the user's function reports: success (optionally carrying records for the
 * engine to produce) or failure (carrying a reason). Success lets the record's offset advance;
 * failure returns it to the engine's own retry scheduling, and the reason rides the redelivery as
 * [[PreviousFailure.reason]].
 *
 * A sealed trait rather than a flag-plus-fields class, because that is what the reference surface's
 * "closed two-armed value" actually is - and a `match` over it is checked for exhaustiveness, so a
 * third arm could not be added without every consumer being made to handle it. There deliberately is
 * no third arm: a function that cannot decide has not finished processing, and a function with no
 * verdict to give says so by never completing its future
 * ([[ParallelConsumerClient.noVerdict]]), not by returning a value.
 */
sealed trait Outcome

object Outcome {

  /** The function completed. `produce` is optional output for the engine to produce. */
  final case class Success(produce: Seq[OutboundRecord] = Nil) extends Outcome {

    /** Omits the records themselves: they carry payload (guide §10.5). */
    override def toString: String = s"Success(produce=${produce.size})"
  }

  /**
   * The function failed; the record returns to retry scheduling. `reason` rides the redelivery
   * verbatim, so it may be read by whoever handles the record next - and by nobody else.
   */
  final case class Failure(reason: Option[String] = None) extends Outcome {

    /** Omits the reason: it is worker-supplied text and may embed anything. */
    override def toString: String = "Failure"
  }

  /** The common case, spelled without the empty argument list. */
  val succeeded: Outcome = Success()

  /** The common failure, from the text that will ride the redelivery. */
  def failed(reason: String): Outcome = Failure(Option(reason))
}
