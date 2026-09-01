// Copyright (C) 2026 Antony Stubbs and contributors

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/parallelconsumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The arm names, which are also the row labels in both tables.
//
// EACH NAMES THE CLIENT IT ACTUALLY RAN, because "AK core" is a CATEGORY and not a client: the
// answer is franz-go here, rdkafka in Ruby, kafkajs in TypeScript, and a reader cannot judge a
// comparison without knowing which one produced it. "AK core" is still spelled out in full - bare
// "core" reads as parallel-consumer-core (CONCEPTS.md) - and the sidecar row names what drives it,
// which on that path is this repository's own Go client library rather than any Kafka client.
const (
	armAKCore    = "AK core (franz-go)"
	armGoSidecar = "pc-go-grpc (this client)"
)

// armBudget is how long an arm may take before the demo calls it stalled rather than slow.
const armBudget = 10 * time.Minute

// armResult is what one arm achieved: over how many records, spread over how many keys, in how
// long.
//
// PROCESSED AND KEYS ARE THE DETERMINISTIC PAIR, and that is why they are reported rather than
// throughput alone. A rate cannot show the work happened - a short arm looks like a fast one - and
// elapsed and msg/s can never be compared between languages or between machines. These two can:
// every language seeding the same backlog reports the same records and the same distinct keys, so
// bin/ci-demo-conformance.sh has something to hold every demo to.
type armResult struct {
	arm       string
	elapsed   time.Duration
	processed int
	keys      int
}

// keySet counts the DISTINCT keys an arm observed, which is what shows the backlog was really
// spread rather than one key repeated.
//
// Guarded because the sidecar arm's function runs on many executors at once; the serial arm shares
// the type rather than keeping a bare map of its own, so both arms count the same thing the same
// way. A nil key and an empty one both land on "", which is the honest answer - this demo seeds
// neither, and a language that did would have to say which it meant.
type keySet struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func newKeySet() *keySet { return &keySet{seen: make(map[string]struct{})} }

func (k *keySet) add(key []byte) {
	k.mu.Lock()
	defer k.mu.Unlock()
	// string(key) on a []byte map index does not allocate - the compiler special-cases it - so
	// counting keys costs nothing measurable next to the simulated work.
	k.seen[string(key)] = struct{}{}
}

func (k *keySet) size() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.seen)
}

// ratePerSecond is throughput, which is the only SPEED figure this demo reports - see broker.seed
// for why latency is not one of them. The records and keys beside it are not speed at all; they are
// the evidence that the work happened.
func (r armResult) ratePerSecond() float64 {
	seconds := r.elapsed.Seconds()
	if seconds <= 0 {
		return 0
	}
	return float64(r.processed) / seconds
}

// akCore is the serial arm: Go's own Kafka client, one record at a time, the same sleep.
//
// A BLOCKING SLEEP IS THE RIGHT SIMULATED WORK IN GO and is what both arms use. The contract's
// non-occupying-wait rule singles out Python (worker processes) and TypeScript (one event loop);
// Go's goroutines make a sleeping worker as cheap as a sleeping thread, so time.Sleep is the
// idiomatic and honest choice here.
func (d demo) akCore(ctx context.Context, target int) (armResult, error) {
	logf("\n=== %s starting over %d records ===", armAKCore, target)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(d.broker.bootstrap),
		kgo.ClientID("pc-go-demo-ak-core"),
		kgo.ConsumerGroup(groupID("ak-core")),
		kgo.ConsumeTopics(d.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Parallel Consumer owns commits on the other arm; this arm owns nothing but its own
		// progress, and committing would only add broker round trips to the thing being timed.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return armResult{}, fmt.Errorf("%s: connecting: %w", armAKCore, err)
	}
	defer client.Close()

	// The clock starts AFTER the client is built and stops before it closes, because this arm is
	// the denominator of every ratio in both tables and the other arm does not charge itself for
	// client construction or teardown either.
	started := time.Now()
	deadline := started.Add(armBudget)
	processed := 0
	keys := newKeySet()
	for processed < target {
		// The arm that waits on nothing still needs the budget armBudget promises, or a backlog
		// shorter than the target spins here forever with no output.
		if time.Now().After(deadline) {
			return armResult{}, fmt.Errorf("%s stalled at %d of %d", armAKCore, processed, target)
		}
		pollCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fetches := client.PollFetches(pollCtx)
		cancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				// A poll that timed out is this loop's own deadline expiring on an empty backlog,
				// not a fault: the arm re-polls until the target or armBudget decides.
				if errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				if errors.Is(e.Err, context.Canceled) {
					return armResult{}, fmt.Errorf("%s: cancelled at %d of %d", armAKCore, processed, target)
				}
				return armResult{}, fmt.Errorf("%s: fetching: %w", armAKCore, e.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			keys.add(r.Key)
			time.Sleep(time.Duration(d.options.delayMs) * time.Millisecond)
			processed++
		})
	}
	return finished(armAKCore, started, processed, keys.size()), nil
}

// goSidecar is the arm the whole design exists for: this application as a FOREIGN CLIENT.
//
// THROUGH THE CLIENT LIBRARY, NEVER BY HAND. An earlier version of the Java seed spoke the protocol
// directly; it proved the engine worked and said nothing about the client library, which is the
// artifact users actually touch. So this arm calls parallelconsumer.Open/Poll/Close exactly as a
// user's program would, and the only Go code between the user's function and the engine is the
// library itself.
//
// ON THIS PATH THE APPLICATION DOES NO KAFKA I/O. The library spawns the sidecar, records arrive
// over a socket, this process's own function runs on them and reports outcomes back; the sidecar
// owns the consumer, the producer, the group membership and the offsets. In a genuinely foreign
// language that is the whole story - the application needs no Kafka client library at all. Here it
// is a statement about the PATH and not about the process: the same binary creates the topic,
// produces the backlog and runs the AK core arm with franz-go, because a comparison needs both
// sides.
func (d demo) goSidecar(ctx context.Context, target int) (armResult, error) {
	logf("\n=== %s starting over %d records ===", armGoSidecar, target)

	client, err := parallelconsumer.Open(ctx, parallelconsumer.Options{
		SidecarPath:     d.sidecar.path,
		SidecarArgs:     d.sidecar.args,
		SidecarStderr:   os.Stderr,
		Topics:          []string{d.topic},
		Ordering:        parallelconsumer.OrderUnordered,
		MaxConcurrency:  int32(d.options.maxConcurrency),
		KafkaProperties: d.broker.consumerProperties(groupID("pc-go-grpc")),
		InstanceTag:     "pc-go-demo",
	})
	if err != nil {
		return armResult{}, fmt.Errorf("%s: opening the session: %w", armGoSidecar, err)
	}
	defer func() {
		// Close is what reaps the sidecar, so it runs even on the failure paths below.
		if err := client.Close(); err != nil {
			logf("%s: closing the session: %v", armGoSidecar, err)
		}
	}()

	var processed atomic.Int64
	keys := newKeySet()
	// done is closed by whichever executor goroutine counts the target record. Every executor can
	// reach it, and closing a channel twice panics, so the close is behind a sync.Once.
	done := make(chan struct{})
	var reachedTarget sync.Once

	started := time.Now()
	pollErr := client.Poll(ctx, func(_ context.Context, rec parallelconsumer.InboundRecord) (parallelconsumer.Outcome, error) {
		// PLACE SERDE SETUP IN YOUR LANGUAGE HERE - keys and values are bytes, and this demo needs
		// only the key, and only to count how many distinct ones the backlog really spread over.
		// The sleep is the same simulated work the AK core arm runs, so the two arms differ by
		// transport and engine and by nothing else.
		//
		// The key is recorded BEFORE the count is incremented, which is what makes the set safe to
		// read the moment the count reaches the target: a record that has been counted has already
		// contributed its key, so nothing can still be arriving when the wait below ends.
		keys.add(rec.Key)
		time.Sleep(time.Duration(d.options.delayMs) * time.Millisecond)
		if processed.Add(1) >= int64(target) {
			reachedTarget.Do(func() { close(done) })
		}
		return parallelconsumer.Succeed(), nil
	})
	if pollErr != nil {
		return armResult{}, fmt.Errorf("%s: starting the poll: %w", armGoSidecar, pollErr)
	}

	select {
	case <-done:
	case <-client.Done():
		// Reaching the target is not the only thing that ends the wait: a session that faulted or
		// completed ends it too. Without this a broken run would print a plausible row at a
		// plausible rate and exit 0, which is the worst thing a demo whose shape ten other
		// languages copy can do.
		count := int(processed.Load())
		if err := client.Err(); err != nil {
			return armResult{}, fmt.Errorf("%s: the session ended at %d of %d: %w",
				armGoSidecar, count, target, err)
		}
		return armResult{}, fmt.Errorf("%s: the session ended cleanly at %d of %d", armGoSidecar, count, target)
	case <-time.After(armBudget):
		return armResult{}, fmt.Errorf("%s stalled at %d of %d", armGoSidecar, processed.Load(), target)
	case <-ctx.Done():
		return armResult{}, fmt.Errorf("%s: cancelled at %d of %d", armGoSidecar, processed.Load(), target)
	}
	return finished(armGoSidecar, started, int(processed.Load()), keys.size()), nil
}

func finished(arm string, started time.Time, processed, keys int) armResult {
	elapsed := time.Since(started)
	logf("=== %s finished: %d records over %d keys in %dms ===", arm, processed, keys, elapsed.Milliseconds())
	return armResult{arm: arm, elapsed: elapsed, processed: processed, keys: keys}
}

// groupID names a fresh consumer group per arm per replay, so every arm reads the same records from
// the beginning.
func groupID(arm string) string {
	return fmt.Sprintf("pc-demo-%s-%d", arm, time.Now().UnixNano())
}
