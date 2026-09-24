// Copyright (C) 2026 Antony Stubbs and contributors

// Command hello prints the one line bin/foreign-client-step.sh checks for. It is the Go end of the
// polyglot build scaffolding (astubbs#242) - toolchain found, source compiled, binary linked,
// program ran, bytes matched - and it will be replaced by the real client when the Go wave lands.
package main

import "fmt"

func main() {
	fmt.Print("parallel-consumer-proxy-client hello fixture: go")
}
