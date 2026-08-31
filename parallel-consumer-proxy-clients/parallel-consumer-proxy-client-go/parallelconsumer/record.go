// Copyright (C) 2026 Antony Stubbs and contributors

package parallelconsumer

import (
	"time"

	proxyv1 "github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/gen/parallelconsumer/proxy/v1"
)

// InboundRecord is one Kafka record as the user's function sees it, plus the delivery state an
// in-process function would have had.
//
// Keys and values are BYTES. The proxy never deserializes and neither does this library:
// deserialization is the user's code, in the user's language.
//
// Nil and empty are different, deliberately, and both fields preserve it: a nil Key is a null key
// and a nil Value is a tombstone, neither of which is the same as a zero-length slice.
type InboundRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte

	// Attempt is 1 on first delivery, 2 on the first redelivery. It is product data: distinct from
	// the fencing epoch, which also counts redeliveries that consumed no attempt.
	Attempt int32

	// LastFailureAt is the zero time on a first delivery. A non-zero value is the wire's way of
	// saying "this has failed before".
	LastFailureAt time.Time

	// LastFailureReason is the previous failure's text, verbatim. It is worker-supplied and may
	// embed record payload: treat it as untrusted input wherever it is handled.
	LastFailureReason string
}

// HasFailedBefore reports whether this delivery follows a recorded failure.
func (r InboundRecord) HasFailedBefore() bool { return !r.LastFailureAt.IsZero() }

// OutboundRecord is a record the user's function asks the proxy to produce on success. Workers
// never touch Kafka themselves; output rides the success report and the proxy produces it with its
// own producer before the input record's offset may become eligible to commit.
type OutboundRecord struct {
	Topic string
	Key   []byte
	Value []byte
}

func inboundOf(d *proxyv1.DispatchRecord) InboundRecord {
	rec := d.GetRecord()
	in := InboundRecord{
		Topic:             rec.GetTopic(),
		Partition:         rec.GetPartition(),
		Offset:            rec.GetOffset(),
		Key:               rec.GetKey(),
		Value:             rec.GetValue(),
		Attempt:           d.GetAttempt(),
		LastFailureReason: d.GetLastFailureReason(),
	}
	if ts := d.GetLastFailureAt(); ts != nil {
		in.LastFailureAt = ts.AsTime()
	}
	return in
}

func (o OutboundRecord) produceRecord() *proxyv1.ProduceRecord {
	p := &proxyv1.ProduceRecord{Key: o.Key, Value: o.Value}
	if o.Topic != "" {
		topic := o.Topic
		p.Topic = &topic
	}
	return p
}
