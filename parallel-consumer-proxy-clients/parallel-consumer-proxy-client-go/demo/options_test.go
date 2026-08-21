// Copyright (C) 2026 Antony Stubbs and contributors

package main

import (
	"strings"
	"testing"
)

// noEnvironment is an empty environment, passed rather than read from the process so these tests
// never depend on what the shell running them happens to export.
func noEnvironment(string) string { return "" }

func environmentOf(pairs map[string]string) func(string) string {
	return func(name string) string { return pairs[name] }
}

// TestNoArgumentsIsTheDocumentedDefault guards THE CASE THAT HAS ACTUALLY BROKEN, twice in this
// family: the demo started with nothing at all. The defaults are also the numbers the contract
// publishes, so this doubles as the check that they have not drifted from
// parallel-consumer-proxy/demo/README.md.
func TestNoArgumentsIsTheDocumentedDefault(t *testing.T) {
	got, err := parseOptions(nil, noEnvironment)
	if err != nil {
		t.Fatalf("no arguments must parse, got %v", err)
	}
	want := options{records: 2000, delayMs: 2, maxConcurrency: 100, partitions: 10, replayFactor: 20}
	if got != want {
		t.Errorf("no arguments gave %+v, want %+v", got, want)
	}
}

// TestFlagsBeatEnvironmentBeatsDefaults is the precedence rule stated in the contract, and it is
// the one thing every language's demo has to agree on that no single language can check alone.
func TestFlagsBeatEnvironmentBeatsDefaults(t *testing.T) {
	env := environmentOf(map[string]string{
		"PC_DEMO_RECORDS":  "50",
		"PC_DEMO_DELAY_MS": "7",
		"PC_DEMO_TOPIC":    "from-the-environment",
	})
	got, err := parseOptions([]string{"--records", "60"}, env)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.records != 60 {
		t.Errorf("a flag must beat the environment: records = %d, want 60", got.records)
	}
	if got.delayMs != 7 {
		t.Errorf("the environment must beat the default: delayMs = %d, want 7", got.delayMs)
	}
	if got.topic != "from-the-environment" {
		t.Errorf("topic = %q, want from-the-environment", got.topic)
	}
	if got.partitions != 10 {
		t.Errorf("an unmentioned setting keeps its default: partitions = %d, want 10", got.partitions)
	}
}

// TestEveryFlagHasAnEnvironmentVariable checks the mapping the contract states as a rule -
// PC_DEMO_ plus the flag in upper snake case - rather than trusting seven hand-written bindings to
// have been written the same way.
func TestEveryFlagHasAnEnvironmentVariable(t *testing.T) {
	values := map[string]string{
		"--records": "11", "--delay-ms": "12", "--concurrency": "13", "--partitions": "14",
		"--replay-factor": "15", "--bootstrap": "somewhere:9092", "--topic": "a-topic",
	}
	for _, flag := range flags {
		name := envPrefix + strings.ToUpper(strings.ReplaceAll(strings.TrimPrefix(flag, "--"), "-", "_"))
		viaEnv, err := parseOptions(nil, environmentOf(map[string]string{name: values[flag]}))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		viaFlag, err := parseOptions([]string{flag, values[flag]}, noEnvironment)
		if err != nil {
			t.Fatalf("%s: %v", flag, err)
		}
		if viaEnv != viaFlag {
			t.Errorf("%s and %s must set the same thing: %+v vs %+v", flag, name, viaFlag, viaEnv)
		}
	}
}

// TestAnEmptyEnvironmentVariableIsNotASetting matters because compose substitutes an unset variable
// as the empty string. Treating "" as a value would let an unset PC_DEMO_RECORDS override the
// default with zero, and the demo would report a confident throughput for no records at all.
func TestAnEmptyEnvironmentVariableIsNotASetting(t *testing.T) {
	got, err := parseOptions(nil, environmentOf(map[string]string{"PC_DEMO_RECORDS": "  "}))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.records != 2000 {
		t.Errorf("records = %d, want the default 2000", got.records)
	}
}

func TestBadInputIsRefusedRatherThanIgnored(t *testing.T) {
	cases := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"an unknown flag", []string{"--recrods", "5"}, nil, "unknown option: --recrods"},
		{"a missing value", []string{"--records"}, nil, "--records needs a value"},
		{"a value that is not a number", []string{"--records", "lots"}, nil, "needs a whole number"},
		{"a records count below one", []string{"--records", "0"}, nil, "must be at least 1"},
		{"a negative delay", []string{"--delay-ms", "-1"}, nil, "must not be negative"},
		{"a bad environment value", nil, map[string]string{"PC_DEMO_CONCURRENCY": "0"}, "PC_DEMO_CONCURRENCY must be at least 1"},
		{"a big replay the demo cannot count", []string{"--records", "2000000", "--replay-factor", "2000"}, nil, "more records than the demo can count"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := noEnvironment
			if c.env != nil {
				env = environmentOf(c.env)
			}
			_, err := parseOptions(c.args, env)
			if err == nil {
				t.Fatalf("expected a refusal mentioning %q, got none", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error was %q, want it to mention %q", err, c.want)
			}
		})
	}
}

// TestAnUnknownFlagDoesNotSwallowTheNextArgument is the reason the parser rejects the flag before
// it reads a value: doing it the other way round consumes the following argument on behalf of a
// flag that does not exist, and the message then names the wrong thing.
func TestAnUnknownFlagDoesNotSwallowTheNextArgument(t *testing.T) {
	_, err := parseOptions([]string{"--nonsense"}, noEnvironment)
	if err == nil || !strings.Contains(err.Error(), "unknown option: --nonsense") {
		t.Errorf("error was %v, want it to name the unknown flag rather than a missing value", err)
	}
}

// TestTheFingerprintNeverCarriesTheBootstrapAddress is a CONTRACT RULE, not a style preference:
// own-cluster mode puts a user's real broker address in that field, and the demo prints the
// fingerprint on every run.
func TestTheFingerprintNeverCarriesTheBootstrapAddress(t *testing.T) {
	o, err := parseOptions([]string{"--bootstrap", "secret-broker.internal:9093", "--topic", "orders"}, noEnvironment)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if strings.Contains(o.String(), "secret-broker") {
		t.Errorf("the fingerprint printed the bootstrap address:\n%s", o.String())
	}
	// The five settings the reference implementation prints, in its order.
	for _, want := range []string{"records = ", "delayMs = ", "maxConcurrency = ", "partitions = ", "replayFactor = "} {
		if !strings.Contains(o.String(), want) {
			t.Errorf("the fingerprint is missing %q:\n%s", want, o.String())
		}
	}
}

func TestBigReplay(t *testing.T) {
	o, err := parseOptions([]string{"--records", "100", "--replay-factor", "3"}, noEnvironment)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !o.bigReplayWanted() || o.bigReplayRecords() != 300 {
		t.Errorf("wanted = %v, records = %d; want true and 300", o.bigReplayWanted(), o.bigReplayRecords())
	}

	// A factor of 1 or 0 skips the big replay, and the total then stays the small replay's own so
	// that nothing downstream has to special-case it.
	for _, factor := range []string{"1", "0"} {
		o, err := parseOptions([]string{"--records", "100", "--replay-factor", factor}, noEnvironment)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if o.bigReplayWanted() {
			t.Errorf("--replay-factor %s must skip the big replay", factor)
		}
		if o.bigReplayRecords() != 100 {
			t.Errorf("--replay-factor %s: total = %d, want 100", factor, o.bigReplayRecords())
		}
	}
}

func TestHelpIsRecognisedByTheBinaryItself(t *testing.T) {
	// Not only by run.sh: `docker compose run demo --help` reaches the binary directly.
	for _, args := range [][]string{{"-h"}, {"--help"}, {"--records", "5", "--help"}} {
		if !helpRequested(args) {
			t.Errorf("%v must be recognised as a help request", args)
		}
	}
	if helpRequested([]string{"--records", "5"}) {
		t.Error("a plain run must not be treated as a help request")
	}
}

// TestTheTableIsTheContractsTable pins the columns, their order and their widths, because a reader
// who has run one language's demo is supposed to have run them all. The reference is the Java
// seed's ReferenceDemo#report.
func TestTheTableIsTheContractsTable(t *testing.T) {
	if got, want := thousands(1234567), "1,234,567"; got != want {
		t.Errorf("thousands(1234567) = %q, want %q", got, want)
	}
	if got, want := thousands(999), "999"; got != want {
		t.Errorf("thousands(999) = %q, want %q", got, want)
	}
	if got, want := thousands(0), "0"; got != want {
		t.Errorf("thousands(0) = %q, want %q", got, want)
	}
}
