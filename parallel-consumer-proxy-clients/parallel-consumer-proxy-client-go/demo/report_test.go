// Copyright (C) 2026 Antony Stubbs and contributors

package main

import (
	"strings"
	"testing"
	"time"
)

// The output rules the shared contract - parallel-consumer-proxy/demo/README.md - states for every
// language's demo. They are checked here because the contract's own words are about STANDARD
// OUTPUT, and standard output is the one thing a unit test can hold a demo to without a broker.
//
// bin/ci-demo-conformance.sh checks the same shape ACROSS languages by requiring their skeletons to
// match. That is a drift check and needs eleven images; these are the same rules asserted for this
// one demo in a second, so a change that breaks them is caught before a container is ever built.

// TestBannerOpensBySayingWhatItIs is the rule that came from someone watching a demo and being
// unimpressed: the first thing printed said `<lang>-grpc: the proxy granted 100 executor threads`,
// which does not contain the words Parallel Consumer.
func TestBannerOpensBySayingWhatItIs(t *testing.T) {
	lines := strings.Split(strings.Trim(banner, "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("the banner must be a rule, two lines and a rule; got %d lines:\n%s", len(lines), banner)
	}
	if lines[0] != lines[3] {
		t.Errorf("the banner's two rules must match: %q vs %q", lines[0], lines[3])
	}
	if got := len(lines[0]); got != 64 {
		t.Errorf("the contract's rule is 64 characters wide, got %d", got)
	}
	if strings.Trim(lines[0], "=") != "" {
		t.Errorf("the rule must be made of '=' alone, got %q", lines[0])
	}
	if !strings.Contains(lines[1], "PARALLEL CONSUMER") {
		t.Errorf("the banner must name the product, got %q", lines[1])
	}
	// The one thing that differs per language. A banner that forgot it would still pass every
	// check above and leave a reader unable to tell which of eleven demos they had run.
	if !strings.Contains(lines[1], "Go demo") {
		t.Errorf("the banner must name this language's demo, got %q", lines[1])
	}
	if want := "  The same records, twice: one at a time, then all at once."; lines[2] != want {
		t.Errorf("the banner's second line is fixed by the contract:\n got %q\nwant %q", lines[2], want)
	}
}

// TestEveryArmNamesTheClientItRan guards the contract's "AK core is a category, not a client". A
// reader cannot judge a comparison without knowing what produced it, and the answer differs in
// every language.
func TestEveryArmNamesTheClientItRan(t *testing.T) {
	if !strings.HasPrefix(armAKCore, "AK core") {
		t.Errorf("the serial arm keeps the role name in full: %q", armAKCore)
	}
	if !strings.Contains(armAKCore, "franz-go") {
		t.Errorf("the serial arm must name the Kafka client it actually ran: %q", armAKCore)
	}
	if !strings.Contains(armGoSidecar, "go-grpc") {
		t.Errorf("the sidecar arm keeps its language-and-transport name: %q", armGoSidecar)
	}
	if !strings.Contains(armGoSidecar, "(") {
		t.Errorf("the sidecar arm must say what drives it: %q", armGoSidecar)
	}
	for _, arm := range []string{armAKCore, armGoSidecar} {
		if len(arm) > armColumnWidth {
			t.Errorf("arm %q is %d characters and the arm column is %d - the table would be ragged",
				arm, len(arm), armColumnWidth)
		}
	}
}

// TestTableReportsWhatEachArmDid is the pair of columns the contract added so that the table
// DEMONSTRATES the run rather than asserting it: throughput alone cannot show the work happened.
func TestTableReportsWhatEachArmDid(t *testing.T) {
	results := []armResult{
		{arm: armAKCore, elapsed: 2 * time.Second, processed: 2000, keys: 1000},
		{arm: armGoSidecar, elapsed: time.Second, processed: 2000, keys: 1000},
	}
	table := renderReport("Small replay", results, &results[0], false)

	header := lineContaining(t, table, "arm")
	// Column IDENTITY and ORDER are contract; padding is not, so the check is on the sequence.
	wantOrder := []string{"arm", "records", "keys", "elapsed", "msg/s", "vs AK core"}
	if got := strings.Fields(strings.ReplaceAll(header, "vs AK core", "vs-AK-core")); len(got) != 6 {
		t.Fatalf("the header must carry six columns, got %d in %q", len(got), header)
	}
	at := 0
	for _, column := range wantOrder {
		next := strings.Index(header[at:], column)
		if next < 0 {
			t.Fatalf("column %q is missing or out of order in %q", column, header)
		}
		at += next + len(column)
	}

	row := lineContaining(t, table, armGoSidecar)
	for _, want := range []string{"2,000", "1,000"} {
		if !strings.Contains(row, want) {
			t.Errorf("the arm's row must report %s, got %q", want, row)
		}
	}
	// Records and keys are the DETERMINISTIC pair; a rate is not, so a run that processed nothing
	// must not be able to hide behind a plausible-looking elapsed.
	empty := renderReport("Small replay", []armResult{{arm: armAKCore, elapsed: time.Second}}, nil, false)
	if !strings.Contains(lineContaining(t, empty, armAKCore), " 0 ") {
		t.Errorf("an arm that processed nothing must say 0, got:\n%s", empty)
	}
}

// TestNoLatencyAnywhere is an absolute the contract states and bin/ci-demo-conformance.sh also
// enforces: the backlog is pre-produced, so per-record timings are flattered by however far an arm
// fell behind, and reporting them would be dishonest rather than merely extra.
func TestNoLatencyAnywhere(t *testing.T) {
	table := renderReport("Big replay",
		[]armResult{{arm: armGoSidecar, elapsed: time.Second, processed: 10, keys: 10}}, nil, true)
	for _, forbidden := range []string{"latency", "p99", "p95", "percentile"} {
		if strings.Contains(strings.ToLower(table), forbidden) {
			t.Errorf("the table reported %q, which the contract forbids:\n%s", forbidden, table)
		}
	}
}

// TestKeySetCountsDistinctKeys covers the counter both arms share, including the concurrent use the
// sidecar arm makes of it - `go test -race` is where that half earns its keep.
func TestKeySetCountsDistinctKeys(t *testing.T) {
	keys := newKeySet()
	done := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 250; i++ {
				keys.add([]byte("key-" + string(rune('a'+i%7))))
			}
		}()
	}
	for worker := 0; worker < 4; worker++ {
		<-done
	}
	if got := keys.size(); got != 7 {
		t.Errorf("1000 records over 7 distinct keys must count 7, got %d", got)
	}
}

func lineContaining(t *testing.T, text, want string) string {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, want) {
			return line
		}
	}
	t.Fatalf("no line containing %q in:\n%s", want, text)
	return ""
}
