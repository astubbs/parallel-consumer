// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

/** Prints the one line every language in this tree prints - the Scala end of the polyglot build
  * scaffolding (astubbs#242). The wording is fixed by `bin/foreign-client-step.sh`; a change there
  * has to change all eleven.
  */
object HelloFixture {

  val Line: String = "parallel-consumer-proxy-client hello fixture: scala"

  def main(args: Array[String]): Unit = print(Line)
}
