// Copyright (C) 2026 Antony Stubbs and contributors

package parallelconsumer

import (
	"context"
	"fmt"
	"strings"
	"testing"

	proxyv1 "github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/gen/parallelconsumer/proxy/v1"
)

// dispatchOf builds a wave of n records with consecutive record ids, in order. The record ids are
// the only thing the tests read back, because FIFO is the one property expressible as an order.
func dispatchOf(n int, from int) *proxyv1.Dispatch {
	d := &proxyv1.Dispatch{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("record-%d", from+i)
		topic, offset := "a-topic", int64(from+i)
		d.Records = append(d.Records, &proxyv1.DispatchRecord{
			Token:  &proxyv1.Token{RecordId: &id},
			Record: &proxyv1.Record{Topic: &topic, Offset: &offset, Value: []byte("v")},
		})
	}
	return d
}

// takeFromQueue is an executor taking a record, and nothing more: it leaves the queue and starts
// running, which is precisely the moment a queue-length bound wrongly frees a slot.
func takeFromQueue(t *testing.T, c *Client) *proxyv1.DispatchRecord {
	t.Helper()
	select {
	case rec := <-c.queue:
		return rec
	default:
		t.Fatal("an executor found the queue empty")
		return nil
	}
}

// TestARecordOutWithAnExecutorStillOccupiesTheCeiling is the client-authoring guide's own worked
// example, and the shape that discriminates: max_concurrency 3, A B and C admitted, two of them
// taken by executors so the queue is nearly empty while the ceiling is full, nothing reported, and
// a fourth record arriving. A client bounding its QUEUE has two free slots and admits D without a
// murmur - which is the defect this test exists for, and why the wave-larger-than-the-ceiling test
// below cannot stand in for it.
func TestARecordOutWithAnExecutorStillOccupiesTheCeiling(t *testing.T) {
	c := newTestClient(newFakeStream(), 3, 2)

	if err := c.enqueue(dispatchOf(3, 1)); err != nil {
		t.Fatalf("a wave exactly filling the ceiling was refused: %v", err)
	}
	takeFromQueue(t, c) // executor-1 takes A
	takeFromQueue(t, c) // executor-2 takes B

	if got := c.unresolved.Load(); got != 3 {
		t.Fatalf("unresolved was %d after two records left the queue for executors, want 3 - "+
			"leaving the queue is not a verdict and does not free a slot", got)
	}

	err := c.enqueue(dispatchOf(1, 4))
	if err == nil {
		t.Fatal("a fourth record was admitted while three were unresolved - the proxy exceeded the " +
			"ceiling it declared itself and this client could not tell")
	}
	for _, want := range []string{"3 were already unresolved", "max_concurrency of 3", "record_id"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the violation did not name %q: %v", want, err)
		}
	}
}

// TestReportingIsWhatFreesTheSlot is the other half of the same rule: only a verdict makes room.
func TestReportingIsWhatFreesTheSlot(t *testing.T) {
	c := newTestClient(newFakeStream(), 1, 1)

	if err := c.enqueue(dispatchOf(1, 1)); err != nil {
		t.Fatalf("the first record was refused: %v", err)
	}
	takeFromQueue(t, c)

	if err := c.enqueue(dispatchOf(1, 2)); err == nil {
		t.Fatal("a second record was admitted while the first was still executing")
	}

	c.settle()
	if err := c.enqueue(dispatchOf(1, 3)); err != nil {
		t.Fatalf("the slot was not freed by the verdict: %v", err)
	}
}

// TestAReportedRecordSettlesOnTheWayOut runs the real report path, because the accounting is only
// as good as the place it is called from: the decrement sits in a defer so that an executor dying
// between the send and the decrement cannot skip it, and a defer is invisible to a test that only
// calls settle by hand.
func TestAReportedRecordSettlesOnTheWayOut(t *testing.T) {
	stream := newFakeStream()
	c := newTestClient(stream, 2, 1)

	if err := c.enqueue(dispatchOf(1, 1)); err != nil {
		t.Fatalf("the record was refused: %v", err)
	}
	rec := takeFromQueue(t, c)

	c.runOne(context.Background(), func(context.Context, InboundRecord) (Outcome, error) {
		if got := c.unresolved.Load(); got != 1 {
			t.Errorf("unresolved was %d while the record was executing, want 1", got)
		}
		return Succeed(), nil
	}, rec)

	if got := c.unresolved.Load(); got != 0 {
		t.Errorf("unresolved was %d after the report was sent, want 0", got)
	}
	if sent := stream.sentMessages(); len(sent) != 1 || sent[0].GetReport() == nil {
		t.Fatalf("want exactly one Report on the stream, got %d message(s): %v", len(sent), sent)
	}
}

// TestDiscardingTheQueueAtShutdownFreesItsSlots covers the second of the guide's decrement points
// this client can reach: a session without the `shutdown` capability drops its queued records and
// reports nothing for them, so nothing else will ever free them.
func TestDiscardingTheQueueAtShutdownFreesItsSlots(t *testing.T) {
	c := newTestClient(newFakeStream(), 3, 1)

	if err := c.enqueue(dispatchOf(3, 1)); err != nil {
		t.Fatalf("the wave was refused: %v", err)
	}
	takeFromQueue(t, c) // one is out with an executor; two are still queued

	c.discardQueue()

	if got := c.unresolved.Load(); got != 1 {
		t.Errorf("unresolved was %d after discarding two queued records, want 1 - the executing "+
			"record still holds its slot until it reports", got)
	}
}

// TestSettlingBelowZeroIsSaturating keeps a double settle from wrapping the count into a ceiling
// nothing could ever overflow, which would turn this check back into one that cannot fire.
func TestSettlingBelowZeroIsSaturating(t *testing.T) {
	c := newTestClient(newFakeStream(), 1, 1)

	c.settle()
	c.settle()

	if got := c.unresolved.Load(); got != 0 {
		t.Fatalf("unresolved was %d after settling an empty count, want 0", got)
	}
	if err := c.enqueue(dispatchOf(1, 1)); err != nil {
		t.Fatalf("the queue would not admit a record after a double settle: %v", err)
	}
	if err := c.enqueue(dispatchOf(1, 2)); err == nil {
		t.Fatal("the ceiling stopped firing after a double settle")
	}
}

// TestAWaveLargerThanTheCeilingOverflows is the test shape the guide names as the one that LOOKS
// like the negative control and is not: a single wave of four against a ceiling of three trips any
// bound at all, including a bound on the queue's own length, so it passes identically against the
// defect it appears to be named for. Kept because the case is real, labelled because it proves
// nothing on its own.
func TestAWaveLargerThanTheCeilingOverflows(t *testing.T) {
	c := newTestClient(newFakeStream(), 3, 2)

	if err := c.enqueue(dispatchOf(4, 1)); err == nil {
		t.Fatal("a wave of four against a ceiling of three was admitted")
	}
}

// TestHandOutIsFifo is rule 3: by arrival, and within one wave by the wave's own order.
func TestHandOutIsFifo(t *testing.T) {
	c := newTestClient(newFakeStream(), 4, 1)

	if err := c.enqueue(dispatchOf(2, 1)); err != nil {
		t.Fatalf("the first wave was refused: %v", err)
	}
	if err := c.enqueue(dispatchOf(2, 3)); err != nil {
		t.Fatalf("the second wave was refused: %v", err)
	}
	for want := 1; want <= 4; want++ {
		if got := takeFromQueue(t, c).GetToken().GetRecordId(); got != fmt.Sprintf("record-%d", want) {
			t.Fatalf("hand-out order reached %q where FIFO wants record-%d", got, want)
		}
	}
}

// sentMessages is a copy, so an assertion never reads the slice the client is appending to.
func (f *fakeStream) sentMessages() []*proxyv1.ClientMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*proxyv1.ClientMessage(nil), f.sent...)
}
