// Copyright (C) 2026 Antony Stubbs and contributors
//
// # WHAT THIS IS FOR: it is the control the llingr comparison did not have
//
// The engine comparison in bench/ runs Parallel Consumer on the Java Kafka client and llingr-demux
// on franz-go. Any gap between them is therefore some mixture of two differences - the engine and
// the client - and nothing in that sweep can separate them. On the Java side that confound has a
// dedicated dimension, CLIENT_PINS, precisely because it was caught once already; across languages
// there is no equivalent, because you cannot pin a Go program to the Java client.
//
// So this arm supplies the missing floor. It is franz-go with NO engine at all: poll, sleep, count.
// No key routing, no shard, no offset reconstruction, no dead-letter path, no concurrency
// controller. Whatever it scores is what the CLIENT can do on this machine against this dataset,
// and only what llingr scores above this floor is attributable to llingr. It is the Go-side
// counterpart of Bench.java.template's "vanilla" arm and it should be read the same way: as a
// ceiling nobody's engine can beat, not as a competitor.
//
// # THE CONCURRENCY MODEL, STATED RATHER THAN IMPLIED
//
// A FIXED WORKER POOL of -concurrency goroutines, fed by an UNBUFFERED channel from the poll loop.
// Unbuffered is deliberate: a buffer would let the poll loop run ahead of the workers, and then the
// peak-in-flight column would measure the buffer instead of the pool. With the channel unbuffered,
// a dispatch blocks until a worker takes it, so in-flight can never exceed the pool size and the
// peak column means the same thing here as in every other arm.
//
// -concurrency 1 is not a pool of one. It runs the handler INLINE in the poll loop, with no
// goroutine and no channel, so the serial arm pays no dispatch cost at all. That matters at delay 0,
// where a channel handoff costs more than the work does, and a "serial" number that included a
// handoff would be measuring the harness.
//
// # WHERE THIS ARM IS WEAKER THAN THE ENGINES IT BOUNDS, AND IT IS NOT A LITTLE
//
// Name it rather than let a fast number imply otherwise. This arm does not order by key: records
// are handed to whichever worker is free, so two records sharing a key can run concurrently and in
// either order. That is the entire problem both engines exist to solve, and it is free here.
//
// It also commits nothing it has processed. franz-go's default autocommit commits what has been
// POLLED, so on a crash this arm loses records that were fetched and never ran - the failure mode
// PC's offset encoding and llingr's contiguous-offset reconstruction both exist to prevent. Left at
// the client default on purpose: changing it would make this a third engine rather than a floor.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Identical accounting to Bench.java.template and to the llingr arm - incremented on entry to the
// handler, decremented on exit, running maximum kept - so the peak column compares across all three.
// Here it also serves as a self-check on the pool: a peak above -concurrency would mean the
// unbuffered channel is not doing what the comment above claims.
var (
	inFlight   atomic.Int64
	peak       atomic.Int64
	processed  atomic.Int64
	dispatched atomic.Int64
)

func main() {
	bootstrap := flag.String("bootstrap", "localhost:19092", "broker to consume from - the one every other arm used")
	topic := flag.String("topic", "", "topic holding the dataset produced by the Java harness")
	count := flag.Int("count", 0, "records to process before stopping the clock")
	delay := flag.Duration("delay", 2*time.Millisecond, "simulated per-record work")
	concurrency := flag.Int("concurrency", 100, "worker pool size; 1 means inline in the poll loop, no pool at all")
	group := flag.String("group", "", "consumer group; defaults to a fresh one so the run re-reads from the start")
	timeout := flag.Duration("timeout", 15*time.Minute, "give up rather than hanging a sweep forever")
	flag.Parse()

	if *topic == "" || *count <= 0 {
		fmt.Fprintln(os.Stderr, "franz-bench: -topic and a positive -count are required")
		os.Exit(2)
	}
	if *concurrency < 1 {
		fmt.Fprintln(os.Stderr, "franz-bench: -concurrency must be at least 1")
		os.Exit(2)
	}

	groupID := *group
	if groupID == "" {
		// Fresh group per run, as every other arm does, so each repeat re-reads the same bytes from
		// offset zero rather than inheriting the previous run's committed position.
		groupID = fmt.Sprintf("bench-franz-%d", time.Now().UnixNano())
	}

	finishedAt := make(chan time.Time, 1)

	// The clock is stopped HERE, by the goroutine that finishes the last record, not by the poll
	// loop that dispatched it. With a worker pool those are different instants, and timing the
	// dispatch would report a throughput the machine never achieved.
	handle := func() {
		now := inFlight.Add(1)
		for {
			p := peak.Load()
			if now <= p || peak.CompareAndSwap(p, now) {
				break
			}
		}
		// The payload is deliberately never touched, matching the llingr arm: reading it would put
		// deserialisation into a number the Java arms do not pay either.
		time.Sleep(*delay)
		inFlight.Add(-1)
		if processed.Add(1) == int64(*count) {
			finishedAt <- time.Now()
		}
	}

	var jobs chan struct{}
	if *concurrency > 1 {
		jobs = make(chan struct{})
		for i := 0; i < *concurrency; i++ {
			go func() {
				for range jobs {
					handle()
				}
			}()
		}
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(*bootstrap),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumerGroup(groupID),
		// Stated explicitly rather than relying on the client default, exactly as the llingr arm
		// does, because it is the counterpart of the Java arms' auto.offset.reset=earliest and a
		// benchmark should not depend on a third party's default staying put.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "franz-bench: cannot create client: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Started before the first poll, which is where franz-go joins the group. Every other arm also
	// starts its clock before its group join, and the 350,000-record dataset is sized so that cost
	// is not what is being measured.
	startedAt := time.Now()

	go func() {
		for {
			fetches := client.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			// Fetch errors are reported, never swallowed: a silently truncated fetch would surface
			// as a timeout and be misread as a slow engine.
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					fmt.Fprintf(os.Stderr, "franz-bench: fetch error on %s[%d]: %v\n", e.Topic, e.Partition, e.Err)
				}
				os.Exit(1)
			}
			fetches.EachRecord(func(_ *kgo.Record) {
				// The topic holds at least -count records and usually exactly that, but a sweep
				// that re-produced would leave more; count the dispatches so a long topic cannot
				// keep the workers busy past the measurement.
				if dispatched.Add(1) > int64(*count) {
					return
				}
				if jobs == nil {
					handle()
					return
				}
				// Blocks until a worker is free. That block IS the backpressure - see the header.
				jobs <- struct{}{}
			})
		}
	}()

	var elapsed time.Duration
	select {
	case at := <-finishedAt:
		elapsed = at.Sub(startedAt)
	case <-time.After(*timeout):
		fmt.Fprintf(os.Stderr, "franz-bench: timed out after %s with %d/%d processed\n",
			*timeout, processed.Load(), *count)
		os.Exit(1)
	}

	// Same line, same field order as every other arm, so one parser in run-bisect.sh reads them all.
	fmt.Printf("RESULT franz %d %d %.1f peak=%d\n",
		*count, elapsed.Milliseconds(), float64(*count)*1000.0/float64(elapsed.Milliseconds()), peak.Load())

	// After the clock has stopped, so it cannot inflate the result. Close rather than leak: the
	// group member should leave cleanly, or the next run's join waits out a session timeout.
	cancel()
	client.Close()
}
