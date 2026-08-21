// Copyright (C) 2026 Antony Stubbs and contributors

package main

import (
	"fmt"
	"os"
	"strings"
)

// logf is the demo's whole output channel: plain lines on stdout, no logging framework.
//
// The tables are the product here, and the shared contract fixes their columns and their order, so
// a framework's prefixes and levels would only get between the reader and them. Everything the
// sidecar itself emits goes to stderr, which keeps the two separable by a scripted caller.
func logf(format string, args ...any) {
	fmt.Fprintf(os.Stdout, format+"\n", args...)
}

func errorf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

// report prints one replay's table.
//
// SAME COLUMNS, SAME ORDER, SAME WIDTHS AS EVERY OTHER LANGUAGE'S DEMO, so a reader who has run one
// has run them all - that is the whole point of the shared contract. THROUGHPUT ONLY: the backlog
// is pre-produced, so the workload is closed-loop and per-record timings would be flattered by
// however far an arm fell behind.
//
// baseline is the AK core arm the ratios are against, or nil when there is none to compare with.
// acrossReplays marks the big replay, whose ratios are against the SMALL replay's AK core - which
// is not like-for-like, and says so under the table rather than in a footnote nobody reads.
func report(title string, results []armResult, baseline *armResult, acrossReplays bool) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n\n%s\n", title)

	comparison := "vs AK core"
	if acrossReplays {
		comparison = "vs AK core*"
	}
	fmt.Fprintf(&b, "  %-14s %10s %14s %14s\n", "arm", "elapsed", "msg/s", comparison)

	for _, r := range results {
		ratio := "-"
		if baseline != nil && baseline.ratePerSecond() != 0 {
			ratio = fmt.Sprintf("%.1fx", r.ratePerSecond()/baseline.ratePerSecond())
		}
		fmt.Fprintf(&b, "  %-14s %9.1fs %14s %14s\n",
			r.arm, r.elapsed.Seconds(), thousands(int(r.ratePerSecond())), ratio)
	}
	if acrossReplays {
		b.WriteString("\n  * against the SMALL replay's AK core arm. Across replays, so not like-for-like.\n")
	}
	logf("%s", b.String())
}

// thousands renders a count with comma separators, which Go's fmt has no verb for.
func thousands(n int) string {
	sign := ""
	if n < 0 {
		sign, n = "-", -n
	}
	digits := fmt.Sprintf("%d", n)
	var out strings.Builder
	for i, d := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			out.WriteByte(',')
		}
		out.WriteRune(d)
	}
	return sign + out.String()
}
