// Copyright (C) 2026 Antony Stubbs and contributors

package parallelconsumer_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/internal/harness"
	"github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/parallelconsumer"
)

// redeliverySettle is how long the test watches for a second delivery after reporting success.
// The harness's redelivery path is fast (the scenario's retry delay is short), so this is a wait
// for an event that should never come, not a race against one that should.
const redeliverySettle = 3 * time.Second

// TestAProcessedRecordAdvancesTheCommittedOffset is wave one's whole claim: one record, end to
// end, against the real wire.
//
// The scenario name is the conformance suite's identity, so this test carries it verbatim. The
// committed offset itself is engine state no client can see, and the harness has no verdict
// channel - it exits 0 whatever happened. So the client-side assertion is the wire-observable
// consequence: the record arrives once, the success report is followed by silence rather than a
// redelivery, and the session closes cleanly.
func TestAProcessedRecordAdvancesTheCommittedOffset(t *testing.T) {
	scenario := harness.ScenarioProcessedRecordAdvancesOffset
	sidecar, err := harness.ForScenario(scenario)
	if err != nil {
		t.Fatalf("locating the conformance harness: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client, err := parallelconsumer.Open(ctx, parallelconsumer.Options{
		SidecarPath: sidecar.Path,
		SidecarArgs: sidecar.Args,
		// THE SCENARIO NAME IS ALSO THE TOPIC NAME - the harness seeds its records on the topic it
		// is named after.
		Topics: []string{scenario},
		// The mock harness builds mock Kafka clients and reads no properties. Real credentials
		// never belong in a conformance test.
		KafkaProperties: map[string]string{},
		InstanceTag:     "go-client-wave-one",
	})
	if err != nil {
		t.Fatalf("opening the session: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("closing the session: %v", err)
		}
	}()

	session := client.Session()
	if session.MaxConcurrency < 1 {
		t.Fatalf("effective max_concurrency was %d, want >= 1", session.MaxConcurrency)
	}
	if session.ExecutorCount < 1 {
		t.Fatalf("effective executor_count was %d, want >= 1", session.ExecutorCount)
	}
	if !session.Negotiated(parallelconsumer.CapabilityDispatch) {
		t.Fatalf("dispatch was not negotiated; the session's capabilities were %v", session.Capabilities)
	}

	var mu sync.Mutex
	var seen []parallelconsumer.InboundRecord
	first := make(chan struct{})
	var once sync.Once

	err = client.Poll(ctx, func(_ context.Context, record parallelconsumer.InboundRecord) (parallelconsumer.Outcome, error) {
		mu.Lock()
		seen = append(seen, record)
		mu.Unlock()
		once.Do(func() { close(first) })
		return parallelconsumer.Succeed(), nil
	})
	if err != nil {
		t.Fatalf("starting the poll: %v", err)
	}

	select {
	case <-first:
	case <-ctx.Done():
		t.Fatalf("no record was dispatched before the deadline: %v", ctx.Err())
	}

	// A success is followed by silence. If the report had not landed, or had not been honoured,
	// the record would come back.
	time.Sleep(redeliverySettle)

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 {
		t.Fatalf("the record was delivered %d times, want exactly 1: %+v", len(seen), seen)
	}
	got := seen[0]
	if got.Topic != scenario {
		t.Errorf("record topic was %q, want the scenario topic %q", got.Topic, scenario)
	}
	if got.Attempt != 1 {
		t.Errorf("record attempt was %d, want 1 on a first delivery", got.Attempt)
	}
	if got.HasFailedBefore() {
		t.Errorf("a first delivery reported a previous failure at %v, reason %q", got.LastFailureAt, got.LastFailureReason)
	}
	if len(got.Value) == 0 {
		t.Errorf("the seeded record carried no value; got key=%q value=%q", got.Key, got.Value)
	}
}
