// Copyright (C) 2026 Antony Stubbs and contributors
//
// PRIVATE RESEARCH ONLY. llingr-demux is AGPL-3.0 (or a commercial licence) and patent pending.
// Nothing measured here may be published, quoted externally, or used in marketing. See
// bench/README.md and docs/inflight/next-competitor-llingr.md for the owner's decision on that.
//
// # WHAT THIS IS FOR
//
// The Java arms of this harness can only tell you how Parallel Consumer moved against ITSELF. They
// share a JVM, a client library and a control loop, so every arm carries the same floor and the
// same ceiling, and a number like "26,000 msg/s" has nothing to be read against. llingr-demux is
// the closest external implementation of the same processing model - key-ordered concurrency past
// partition count, offsets committed after out-of-order completion - so it supplies the outside
// reference point the sweep has never had.
//
// # THE MEASUREMENT IS THE DELAY SWEEP, NOT A SINGLE NUMBER
//
// At one fixed delay this arm answers nothing useful: a Go engine will beat a JVM one and everybody
// already knows it. What is worth having is the SHAPE of each engine against simulated work time.
// Per-record framework overhead is what a comparison at delay 0 exposes; at 100ms the sleep
// dominates and both engines converge on (records * delay / concurrency), so the gap closing - or
// failing to - is the actual result. That is why -delay is the primary axis and the harness sweeps
// it, rather than pinning it the way the version bisect did.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/llingr/llingr-adapter-franz/franzadapter"
	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Matches Bench.java.template's accounting exactly, so the two arms' peak columns mean the same
// thing: incremented on entry to the handler, decremented on exit, running maximum kept. This is
// the engine's ACTUAL concurrency, not what it was configured to allow - an engine that quietly
// ignores its concurrency dial looks fast for a reason that has nothing to do with being fast, and
// the peak column is the only thing that catches it.
var (
	inFlight  atomic.Int64
	peak      atomic.Int64
	processed atomic.Int64
)

// The ConcurrentKeys dial's hard ceiling; exceeding it panics inside the library rather than
// clamping, which would abort a sweep halfway through. Clamp loudly instead - a sweep that runs to
// completion with one arm annotated beats one that dies at arm three. Package scope because
// scenarios.go clamps against the same ceiling, and a library's hard limit copied twice drifts.
const concurrentKeysMax = 5000

func main() {
	bootstrap := flag.String("bootstrap", "localhost:19092", "broker to consume from - the one the Java arms used")
	topic := flag.String("topic", "", "topic holding the dataset produced by the Java harness")
	count := flag.Int("count", 0, "records to process before stopping the clock")
	delay := flag.Duration("delay", 2*time.Millisecond, "simulated per-record work; THE primary axis of this arm")
	concurrency := flag.Int("concurrency", 100, "llingr ConcurrentKeys; the counterpart of PC's maxConcurrency")
	group := flag.String("group", "", "consumer group; defaults to a fresh one so the run re-reads from the start")
	timeout := flag.Duration("timeout", 15*time.Minute, "give up rather than hanging a sweep forever")
	flag.Parse()

	if *topic == "" || *count <= 0 {
		fmt.Fprintln(os.Stderr, "llingr-bench: -topic and a positive -count are required")
		os.Exit(2)
	}

	// The divergence scenarios ask a different question from throughput - what committing past gaps
	// buys - and own their own runners in scenarios.go. Dispatched here so both arms share one flag
	// set, one broker contract and one RESULT line format.
	if *scenario != "" {
		runScenario(scenarioConfig{
			bootstrap:   *bootstrap,
			topic:       *topic,
			group:       *group,
			count:       *count,
			delay:       *delay,
			concurrency: *concurrency,
			timeout:     *timeout,
		})
		return
	}

	if *concurrency > concurrentKeysMax {
		fmt.Fprintf(os.Stderr, "llingr-bench: ConcurrentKeys capped at %d by the library; %d requested\n",
			concurrentKeysMax, *concurrency)
		*concurrency = concurrentKeysMax
	}

	groupID := *group
	if groupID == "" {
		// Fresh group per run, exactly as Bench.java.template does, so every repeat re-reads the
		// same bytes from offset zero instead of inheriting the previous run's committed position.
		groupID = fmt.Sprintf("bench-llingr-%d", time.Now().UnixNano())
	}

	finishedAt := make(chan time.Time, 1)

	process := func(_ context.Context, _ *nexus.Message[*kgo.Record]) error {
		now := inFlight.Add(1)
		for {
			p := peak.Load()
			if now <= p || peak.CompareAndSwap(p, now) {
				break
			}
		}
		// The payload is deliberately never touched. llingr recycles Message pointers through a
		// sync.Pool, so reading the record would measure deserialisation the Java arms do not do,
		// and holding it past return is a documented data-corruption trap.
		time.Sleep(*delay)
		inFlight.Add(-1)

		// Timestamped here rather than in a polling loop in main, because the Java arm's 25ms poll
		// interval is slop this side does not need to reproduce; on a run of seconds it is noise
		// either way, and being right is free.
		if processed.Add(1) == int64(*count) {
			finishedAt <- time.Now()
		}
		return nil
	}

	// Never invoked - process never returns an error - but the engine requires one, and a dead
	// letter that silently succeeded would hide a processing failure as a completed record.
	deadLetter := func(_ context.Context, _ *nexus.Message[*kgo.Record], reason error) error {
		fmt.Fprintf(os.Stderr, "llingr-bench: unexpected dead letter: %v\n", reason)
		return nil
	}

	builder := demux.NewBuilder[*kgo.Record](*topic, process, deadLetter).
		WithLogger(quietLogger{}).
		// Without this the library's default handler waits 15 seconds and then sends itself
		// os.Interrupt, which would kill the process before it printed its result line.
		WithShutdownCallback(func(_ context.Context, reason error) {
			if reason != nil {
				fmt.Fprintf(os.Stderr, "llingr-bench: emergency shutdown: %v\n", reason)
			}
		}).
		// Zero fields take the library's production defaults; only the concurrency dial is set,
		// because it is the one the Java arms also set and the only one worth matching.
		WithDemuxConfig(config.DemuxConfig{ConcurrentKeys: *concurrency})

	// AtStart explicitly, rather than relying on the client default, because it is the direct
	// counterpart of the Java arm's auto.offset.reset=earliest and a benchmark should not depend on
	// a third-party default staying put.
	adapter := franzadapter.NewWithOptions(context.Background(), groupID, []string{*bootstrap},
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))

	consumer, err := adapter.CreateConsumer(builder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "llingr-bench: cannot create consumer: %v\n", err)
		os.Exit(1)
	}

	// Clock starts here, immediately before the group is joined, which is where the Java arms start
	// theirs too: PC's subscribe() only records the subscription and the join happens inside the
	// control loop that poll() starts. Both arms therefore pay for group join, and the harness's
	// 350,000-record dataset is sized so that cost is not what is being measured.
	startedAt := time.Now()
	if err := consumer.Subscribe(); err != nil {
		fmt.Fprintf(os.Stderr, "llingr-bench: subscribe failed: %v\n", err)
		os.Exit(1)
	}

	var elapsed time.Duration
	select {
	case at := <-finishedAt:
		elapsed = at.Sub(startedAt)
	case <-time.After(*timeout):
		fmt.Fprintf(os.Stderr, "llingr-bench: timed out after %s with %d/%d processed\n",
			*timeout, processed.Load(), *count)
		os.Exit(1)
	}

	// Same line, same field order as Bench.java.template, so one parser reads both arms.
	fmt.Printf("RESULT llingr %d %d %.1f peak=%d\n",
		*count, elapsed.Milliseconds(), float64(*count)*1000.0/float64(elapsed.Milliseconds()), peak.Load())

	// Shutdown drains and commits, which happens after the clock has stopped and so cannot inflate
	// the result. Errors are reported but not fatal: the measurement is already printed.
	if err := consumer.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "llingr-bench: shutdown: %v\n", err)
	}
}

// quietLogger is the counterpart of bench/conf/logback.xml, and exists for the same reason: this
// harness has already been caught measuring its own logging configuration once, where default DEBUG
// cost a PC arm 6x. Errors and warnings still reach stderr so a broken run is not mistaken for a
// slow one; info and debug are dropped, which also silences the library's log-only licence notice.
// stdout is left clean for the RESULT line.
type quietLogger struct{}

func (quietLogger) Error(_ context.Context, format string, args ...any) {
	fmt.Fprintf(os.Stderr, "llingr ERROR "+format+"\n", args...)
}

func (quietLogger) Warn(_ context.Context, format string, args ...any) {
	fmt.Fprintf(os.Stderr, "llingr WARN "+format+"\n", args...)
}

func (quietLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (quietLogger) Debug(_ context.Context, _ string, _ ...any) {}
