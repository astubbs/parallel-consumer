// Copyright (C) 2026 Antony Stubbs and contributors

// Command pc-go-demo is the Go demo of the Parallel Consumer language proxy: the same records
// through Go's own Kafka client and through Go over the sidecar (astubbs#242, plan unit U35).
//
// THE CONTRACT IT KEEPS IS parallel-consumer-proxy/demo/README.md, and the reference
// implementation is the Java seed beside the Java client. Same flags, same environment variables,
// same precedence, same defaults, same two tables in the same order, the effective configuration
// printed first and the bootstrap address never printed at all. What is specific to Go - and the
// short list of places it diverges - is in demo/README.md beside this file.
//
// TWO ARMS, WHICH IS THE WHOLE CONTRACT OUTSIDE JAVA, AND EACH NAMES THE CLIENT IT ACTUALLY RAN:
//
//   - AK core (franz-go)    - Go's own Kafka client, one record at a time. "AK core" is a
//     CATEGORY, not a client, so the library is named beside it - a reader cannot judge a
//     comparison without knowing what produced it. Always spelled "AK core", never bare "core",
//     which reads as parallel-consumer-core (CONCEPTS.md).
//   - pc-go-grpc (this client) - this application as a FOREIGN CLIENT, through the Go client library,
//     over a real sidecar the library spawns as a child process. The application does no Kafka I/O
//     on that path: the sidecar owns the consumer, the producer, the group membership and the
//     offsets.
//
// Java carries four more arms because one JVM can hold every engine at once and price the hop
// exactly. Go cannot - its two arms are different client libraries as well as different engines -
// so two arms is the whole comparison here, by design rather than by omission.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// banner is the demo's opening lines, fixed by the shared contract and identical in all eleven
// languages except for the language's own name. Copied verbatim rather than composed, including
// the sixty-four rule characters and the two spaces either side of the dash, because eleven demos
// drifting apart on their own banner would be a poor advertisement for a contract about output.
const banner = `
================================================================
  PARALLEL CONSUMER  -  Go demo
  The same records, twice: one at a time, then all at once.
================================================================`

func main() {
	os.Exit(run(os.Args[1:]))
}

// demo is one run: the settings, the cluster, the topic and the sidecar command, resolved once.
type demo struct {
	options options
	broker  broker
	topic   string
	sidecar sidecarCommand
}

func run(args []string) int {
	if helpRequested(args) {
		logf("%s", usageText)
		return 0
	}

	// THE BANNER IS THE FIRST THING PRINTED, before the settings and before any arm says anything
	// about itself. A reader who runs this and meets `pc-go-grpc: the proxy granted 100 executor
	// threads` has been told nothing about what they are looking at; the words "Parallel Consumer"
	// have to appear before anything else does. Identical in every language bar the language's own
	// name - see the contract's "It opens by saying what it is".
	logf("%s", banner)

	opts, err := parseOptions(args, environment)
	if err != nil {
		// A misspelled flag must never be reported as a result for settings nobody asked for.
		errorf("%v", err)
		errorf("%s", usageText)
		return 2
	}

	if opts.bootstrap == "" {
		errorf("No broker address was supplied. Unlike the Java seed this demo never starts one " +
			"itself - demo/run.sh does, and then passes it here. Run demo/run.sh, or pass " +
			"--bootstrap ADDR / PC_DEMO_BOOTSTRAP for a cluster you already have.")
		return 2
	}

	sidecar, err := resolveSidecar()
	if err != nil {
		errorf("%v", err)
		return 2
	}

	topic := opts.topic
	if topic == "" {
		topic = fmt.Sprintf("pc-demo-%d", time.Now().UnixNano())
	}

	// Ctrl-C ends the run rather than orphaning a sidecar: the client library reaps its child on
	// Close, and cancelling the context is what gets us there.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	d := demo{options: opts, broker: broker{bootstrap: opts.bootstrap}, topic: topic, sidecar: sidecar}
	if err := d.run(ctx); err != nil {
		errorf("%v", err)
		return 1
	}
	return 0
}

func (d demo) run(ctx context.Context) error {
	// THE FINGERPRINT COMES FIRST, and it never carries the bootstrap address: own-cluster mode
	// puts a user's real broker there. A number without its settings is not reproducible.
	logf("\nEffective configuration:\n  %s\n  topic = %s", d.options, d.topic)

	if err := d.broker.ensureTopic(ctx, d.topic, d.options.partitions); err != nil {
		return err
	}
	if err := d.broker.seed(ctx, d.topic, 0, d.options.records); err != nil {
		return err
	}

	akCore, err := d.akCore(ctx, d.options.records)
	if err != nil {
		return err
	}
	sidecarArm, err := d.goSidecar(ctx, d.options.records)
	if err != nil {
		return err
	}
	small := []armResult{akCore, sidecarArm}
	report(fmt.Sprintf("Small replay - every arm over the same %d records (the comparison)",
		d.options.records), small, &akCore, false)

	if !d.options.bigReplayWanted() {
		logf("\nBig replay skipped (--replay-factor %d).", d.options.replayFactor)
		return nil
	}

	total := d.options.bigReplayRecords()
	if err := d.broker.seed(ctx, d.topic, d.options.records, total); err != nil {
		return err
	}

	// AK CORE IS EXCLUDED HERE BECAUSE IT DOES NOT GO PARALLEL. It would need total * delayMs
	// milliseconds to finish a backlog the sidecar arm clears in seconds, and a demo that makes a
	// reader wait that long to learn nothing new is not worth the wall clock. That leaves one row,
	// which is what the contract's "every arm that goes parallel" means in a language whose only
	// other arm is serial.
	big, err := d.goSidecar(ctx, total)
	if err != nil {
		return err
	}
	// the unit is chosen so the figure is never zero - see the demo contract.
	serialMillis := total * d.options.delayMs
	serialCost := fmt.Sprintf("%dms", serialMillis)
	if serialMillis >= 1000 {
		serialCost = fmt.Sprintf("%ds", serialMillis/1000)
	}
	report(fmt.Sprintf("Big replay - %d records, parallel arms only (AK core is serial and would take %s+)",
		total, serialCost), []armResult{big}, &akCore, true)
	return nil
}
