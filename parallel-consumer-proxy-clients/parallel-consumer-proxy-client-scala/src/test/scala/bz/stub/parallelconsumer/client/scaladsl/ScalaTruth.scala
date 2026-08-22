// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

import com.google.common.truth.{
  BooleanSubject,
  IntegerSubject,
  IterableSubject,
  LongSubject,
  MapSubject,
  StringSubject,
  Subject,
  Truth
}

/**
 * Google Truth, reachable from Scala.
 *
 * The repository asserts with Truth everywhere and this module does too - what is here is not a
 * second assertion library, it is the boxing Scala will not do on its own. `Truth.assertThat` is
 * overloaded on `Boolean`, `Integer` and `Long`, and Scala's overload resolution takes a
 * `scala.Boolean` to the `Object` overload rather than boxing it, so `assertThat(x).isFalse()` fails
 * to compile with "isFalse is not a member of Subject" - a compile error rather than a wrong
 * assertion, but one every Scala test in this module would otherwise have to work around at each
 * call site.
 *
 * Forwarding rather than converting: each method hands the same value to the same Truth entry point,
 * so failure messages are Truth's own.
 */
private[scaladsl] object ScalaTruth {

  def assertThat(actual: Boolean): BooleanSubject = Truth.assertThat(Boolean.box(actual))

  def assertThat(actual: Int): IntegerSubject = Truth.assertThat(Int.box(actual))

  def assertThat(actual: Long): LongSubject = Truth.assertThat(Long.box(actual))

  def assertThat(actual: String): StringSubject = Truth.assertThat(actual)

  def assertThat(actual: java.lang.Iterable[_]): IterableSubject = Truth.assertThat(actual)

  def assertThat(actual: java.util.Map[_, _]): MapSubject = Truth.assertThat(actual)

  def assertThat(actual: AnyRef): Subject = Truth.assertThat(actual)
}
