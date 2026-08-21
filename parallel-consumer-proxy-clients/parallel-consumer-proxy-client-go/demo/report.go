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

// armColumnWidth is the arm column, wide enough for the longest label an arm can carry now that
// each names the client it ran - `go-grpc (this client)` is twenty-one characters against the
// seven `go-grpc` used to need. Column WIDTH is deliberately not contract, unlike column identity
// and order: a language with a longer client name would otherwise be in permanent violation of an
// alignment rule it cannot keep.
const armColumnWidth = 22

// report prints one replay's table.
//
// SAME COLUMNS, IN THE SAME ORDER, AS EVERY OTHER LANGUAGE'S DEMO, so a reader who has run one has
// run them all - that is the whole point of the shared contract. Width is the one thing that is
// not shared, and armColumnWidth says why. THROUGHPUT ONLY as far as
// speed goes: the backlog is pre-produced, so the workload is closed-loop and per-record timings
// would be flattered by however far an arm fell behind.
//
// WHAT IT DID COMES BEFORE HOW FAST IT DID IT. `records` and `keys` are first because a rate on its
// own cannot show the work happened - a short arm reads as a fast one - and because they are the
// only two figures here that are DETERMINISTIC: every language over the same backlog reports the
// same pair, while elapsed and msg/s can never be compared across languages or machines. The three
// speed columns stay adjacent at the right, since `vs AK core` is derived from msg/s.
//
// baseline is the AK core arm the ratios are against, or nil when there is none to compare with.
// acrossReplays marks the big replay, whose ratios are against the SMALL replay's AK core - which
// is not like-for-like, and says so under the table rather than in a footnote nobody reads.
func report(title string, results []armResult, baseline *armResult, acrossReplays bool) {
	logf("%s", renderReport(title, results, baseline, acrossReplays))
}

// renderReport is the table as text, split out from report so a test can assert the columns the
// contract fixes without capturing this process's stdout.
func renderReport(title string, results []armResult, baseline *armResult, acrossReplays bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n\n%s\n", title)

	comparison := "vs AK core"
	if acrossReplays {
		comparison = "vs AK core*"
	}
	fmt.Fprintf(&b, "  %-*s %9s %8s %10s %12s %12s\n",
		armColumnWidth, "arm", "records", "keys", "elapsed", "msg/s", comparison)

	for _, r := range results {
		ratio := "-"
		if baseline != nil && baseline.ratePerSecond() != 0 {
			ratio = fmt.Sprintf("%.1fx", r.ratePerSecond()/baseline.ratePerSecond())
		}
		fmt.Fprintf(&b, "  %-*s %9s %8s %9.1fs %12s %12s\n",
			armColumnWidth, r.arm, thousands(r.processed), thousands(r.keys),
			r.elapsed.Seconds(), thousands(int(r.ratePerSecond())), ratio)
	}
	if acrossReplays {
		b.WriteString("\n  * against the SMALL replay's AK core arm. Across replays, so not like-for-like.\n")
	}
	return b.String()
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
