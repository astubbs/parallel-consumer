// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

/**
 * Prints the one line every language in this tree prints - the Kotlin end of the polyglot build
 * scaffolding (astubbs#242). The wording is fixed by `bin/foreign-client-step.sh`; a change there
 * has to change all eleven.
 */
public object HelloFixture {

    public const val LINE: String = "parallel-consumer-proxy-client hello fixture: kotlin"

    @JvmStatic
    public fun main(args: Array<String>) {
        print(LINE)
    }
}
