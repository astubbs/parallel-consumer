// Copyright (C) 2026 Antony Stubbs and contributors

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// envPrefix is the prefix of every environment variable this demo reads, so a reader can grep one
// string. It is part of the shared contract, not a Go choice.
const envPrefix = "PC_DEMO_"

// options are the demo's dials, and the surface every language's demo mirrors:
// parallel-consumer-proxy/demo/README.md is the contract, and the Java reference implementation of
// this exact struct is DemoOptions in parallel-consumer-proxy-client-java-demo.
//
// PRECEDENCE IS FLAGS BEAT ENVIRONMENT BEATS DEFAULTS, and it is stated rather than implied
// because eleven demos have to agree on it: a container passes configuration by environment while
// a person at a terminal passes flags, and both must be able to override the other's layer.
//
// R39 constrains how configuration reaches the PROXY. A demo is an application, so these flags are
// not a breach of it - the note lives here because without it someone reads --records as breaking
// the plan's own rule and deletes it.
type options struct {
	records        int
	delayMs        int
	maxConcurrency int
	partitions     int
	replayFactor   int

	// bootstrap is empty when the caller supplied no broker. See run.sh: on this client the broker
	// is started by the entry point rather than by the demo binary, which is the one place Go
	// diverges from the Java reference - demo/README.md says why.
	bootstrap string

	// topic is empty when the caller supplied none, in which case the demo names its own.
	topic string
}

func defaults() options {
	return options{
		records:        2000,
		delayMs:        2,
		maxConcurrency: 100,
		partitions:     10,
		replayFactor:   20,
	}
}

// helpRequested reports whether the caller asked for the usage text rather than a run.
//
// Handled in the binary and not only in run.sh, because the script is not the only way in:
// `docker compose run demo --help` reaches this binary directly, and answering that with
// "unknown option: --help" would be a poor first impression of a demo ten other languages copy.
func helpRequested(args []string) bool {
	for _, a := range args {
		if a == "-h" || a == "--help" {
			return true
		}
	}
	return false
}

// parseOptions applies the environment over the defaults and then the flags over both.
//
// It REFUSES an unknown flag, a missing value and a value that is not a number in range. A demo
// that silently ignored a misspelled flag would report numbers for settings the user did not ask
// for, which is worse than not running.
func parseOptions(args []string, env func(string) string) (options, error) {
	o := defaults()
	if err := o.applyEnvironment(env); err != nil {
		return o, err
	}

	for i := 0; i < len(args); i++ {
		flag := args[i]
		if !takesAValue(flag) {
			return o, fmt.Errorf("unknown option: %s", flag)
		}
		// Every flag this demo has takes a value, so the unknown flag is rejected FIRST and a
		// missing value is always the remaining error. Doing it the other way round consumes the
		// next argument on behalf of a flag that does not exist, and the message then names the
		// wrong thing.
		if i+1 >= len(args) {
			return o, fmt.Errorf("%s needs a value", flag)
		}
		i++
		raw := args[i]

		var err error
		switch flag {
		case "--records":
			o.records, err = positive(flag, raw)
		case "--delay-ms":
			o.delayMs, err = nonNegative(flag, raw)
		case "--concurrency":
			o.maxConcurrency, err = positive(flag, raw)
		case "--partitions":
			o.partitions, err = positive(flag, raw)
		case "--replay-factor":
			// 1 or less skips the big replay, so this one is allowed to be zero
			o.replayFactor, err = nonNegative(flag, raw)
		case "--bootstrap":
			o.bootstrap = raw
		case "--topic":
			o.topic = raw
		}
		if err != nil {
			return o, err
		}
	}
	return o, o.validate()
}

// flags is the demo's whole command line, in the order the contract lists it. One list, so the
// parser and the rejection of an unknown flag cannot disagree about what exists.
var flags = []string{
	"--records", "--delay-ms", "--concurrency", "--partitions", "--replay-factor",
	"--bootstrap", "--topic",
}

func takesAValue(flag string) bool {
	for _, known := range flags {
		if known == flag {
			return true
		}
	}
	return false
}

func (o *options) applyEnvironment(env func(string) string) error {
	type binding struct {
		suffix string
		apply  func(string, string) error
	}
	bindings := []binding{
		{"RECORDS", func(name, raw string) (err error) { o.records, err = positive(name, raw); return }},
		{"DELAY_MS", func(name, raw string) (err error) { o.delayMs, err = nonNegative(name, raw); return }},
		{"CONCURRENCY", func(name, raw string) (err error) { o.maxConcurrency, err = positive(name, raw); return }},
		{"PARTITIONS", func(name, raw string) (err error) { o.partitions, err = positive(name, raw); return }},
		{"REPLAY_FACTOR", func(name, raw string) (err error) { o.replayFactor, err = nonNegative(name, raw); return }},
		{"BOOTSTRAP", func(_, raw string) error { o.bootstrap = raw; return nil }},
		{"TOPIC", func(_, raw string) error { o.topic = raw; return nil }},
	}
	for _, b := range bindings {
		name := envPrefix + b.suffix
		raw := strings.TrimSpace(env(name))
		if raw == "" {
			// An EMPTY variable is not a setting. Compose substitutes an unset variable as the
			// empty string - PC_DEMO_ARGS: ${PC_DEMO_ARGS:-} is in the compose file beside this -
			// so treating "" as a value would make an unset variable override a default with zero.
			continue
		}
		if err := b.apply(name, raw); err != nil {
			return err
		}
	}
	return nil
}

func (o options) validate() error {
	// Checked as a 64-bit product rather than trusted as an int later: on a 32-bit build
	// records * replayFactor overflows silently, and a wrapped value turns the big replay into a
	// tiny one that still prints a confident throughput figure.
	big := int64(o.records) * int64(max(1, o.replayFactor))
	if big > int64(^uint32(0)>>1) {
		return fmt.Errorf("--records times --replay-factor is %d, which is more records than the "+
			"demo can count; lower one of them", big)
	}
	return nil
}

// bigReplayRecords is the records the big replay consumes in total, including the small replay's.
func (o options) bigReplayRecords() int { return o.records * max(1, o.replayFactor) }

// bigReplayWanted reports whether the big replay is worth running; a factor of 1 or less skips it.
func (o options) bigReplayWanted() bool { return o.replayFactor > 1 }

// String is the effective-configuration fingerprint, printed before the run.
//
// A number without its settings is not reproducible, so this is part of the contract every
// language's demo keeps rather than a debugging aid. THE BOOTSTRAP ADDRESS IS DELIBERATELY ABSENT:
// own-cluster mode puts a user's real broker there, and the credential-hygiene rule that binds the
// proxy binds a demo too - nothing logged, nothing echoed.
func (o options) String() string {
	return fmt.Sprintf("records = %d\n  delayMs = %d\n  maxConcurrency = %d\n  partitions = %d\n  replayFactor = %d",
		o.records, o.delayMs, o.maxConcurrency, o.partitions, o.replayFactor)
}

func positive(name, raw string) (int, error) {
	n, err := number(name, raw)
	if err != nil {
		return 0, err
	}
	if n < 1 {
		return 0, fmt.Errorf("%s must be at least 1, got %d", name, n)
	}
	return n, nil
}

func nonNegative(name, raw string) (int, error) {
	n, err := number(name, raw)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("%s must not be negative, got %d", name, n)
	}
	return n, nil
}

func number(name, raw string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("%s needs a whole number, got %q", name, raw)
	}
	return n, nil
}

// environment is os.Getenv as the func the parser takes, so the parser is testable without
// mutating this process's environment.
func environment(name string) string { return os.Getenv(name) }

const usageText = `usage: demo/run.sh [options]

  --records N        records in the comparison replay   (default 2000)
  --delay-ms N       simulated work per record, ms      (default 2)
  --concurrency N    max in-flight records              (default 100)
  --partitions N     partitions on the demo topic       (default 10)
  --replay-factor N  big replay = records x N; 1 skips  (default 20)
  --bootstrap ADDR   an existing broker; omit and run.sh starts one
  --topic NAME       an existing topic; omit to create one

Every flag has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
Flags beat the environment beats the defaults.`
