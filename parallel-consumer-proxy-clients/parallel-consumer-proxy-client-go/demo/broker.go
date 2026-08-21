// Copyright (C) 2026 Antony Stubbs and contributors

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// keySpace is the key space the seeded records spread over. Ordering is unordered in both arms, so
// it changes nothing today; it exists so that a key-ordered lane added later has more than one key
// to shard across rather than needing the seeding rewritten first.
const keySpace = 1000

// broker is the cluster both arms read from.
//
// UNLIKE THE JAVA REFERENCE, IT NEVER STARTS ONE. Java's DemoBroker falls back to Testcontainers
// when no address was supplied; here run.sh starts the broker - as a container natively, as a
// compose sibling in the container - and this type is always handed an address. demo/README.md
// records that divergence and why it was taken. The rule that produced it is the same one either
// way: a demo container is never granted the host Docker socket, so a containerised demo reaches a
// broker it did not start.
//
// The address is never logged or echoed: the same door serves own-cluster mode, where it is the
// user's real cluster.
type broker struct {
	bootstrap string
}

// ensureTopic creates the demo's topic, tolerating one a previous run left behind.
//
// A topic that already exists with a DIFFERENT partition count is refused rather than reused: the
// effective-configuration block would otherwise print a --partitions value that never applied, and
// that block is the demo's whole reproducibility promise.
func (b broker) ensureTopic(ctx context.Context, topic string, partitions int) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(b.bootstrap), kgo.ClientID("pc-go-demo-admin"))
	if err != nil {
		return fmt.Errorf("connecting to the broker: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	created, err := admin.CreateTopics(ctx, int32(partitions), 1, nil, topic)
	if err != nil {
		return fmt.Errorf("creating the demo topic %s: %w", topic, err)
	}
	switch createErr := created[topic].Err; {
	case createErr == nil:
		logf("Created topic %s with %d partitions", topic, partitions)
		return nil
	case errors.Is(createErr, kerr.TopicAlreadyExists):
		details, err := admin.ListTopics(ctx, topic)
		if err != nil {
			return fmt.Errorf("describing the existing topic %s: %w", topic, err)
		}
		existing := len(details[topic].Partitions)
		if existing != partitions {
			return fmt.Errorf("topic %s already exists with %d partitions, but this run asked for "+
				"%d - pass --topic to name a fresh one, or --partitions %d",
				topic, existing, partitions, existing)
		}
		logf("Topic %s already exists with the requested %d partitions, reusing it", topic, partitions)
		return nil
	default:
		return fmt.Errorf("creating the demo topic %s: %w", topic, createErr)
	}
}

// seed produces the backlog both arms then replay.
//
// PRE-PRODUCED RATHER THAN PRODUCED ALONGSIDE THE ARMS, which is what makes the workload
// closed-loop - and in turn why no arm reports latency. A per-record timing here would be flattered
// by however far an arm had fallen behind, so throughput is the only honest number this shape can
// produce.
func (b broker) seed(ctx context.Context, topic string, from, to int) error {
	if to <= from {
		return nil
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(b.bootstrap),
		kgo.ClientID("pc-go-demo-seed"),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerLinger(20*time.Millisecond),
	)
	if err != nil {
		return fmt.Errorf("connecting to the broker to seed: %w", err)
	}
	defer client.Close()

	logf("Producing records %d to %d...", from, to)
	// A discarded promise swallows the reason, and Flush does not report a send that failed - so
	// without this the demo would report a full backlog, run both arms against a short one, and
	// print numbers for a workload that never existed.
	var once sync.Once
	var firstFailure error
	for i := from; i < to; i++ {
		client.Produce(ctx, &kgo.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i%keySpace)),
			Value: []byte(fmt.Sprintf("record-%d", i)),
		}, func(_ *kgo.Record, err error) {
			if err != nil {
				once.Do(func() { firstFailure = err })
			}
		})
	}
	if err := client.Flush(ctx); err != nil {
		return fmt.Errorf("the demo could not seed its backlog: %w", err)
	}
	if firstFailure != nil {
		return fmt.Errorf("the demo could not seed its backlog: %w", firstFailure)
	}
	logf("Produced %d records", to-from)
	return nil
}

// consumerProperties is what an arm's consumer needs to reach this broker, as the string map the
// protocol's Configure carries.
//
// enable.auto.commit is false because Parallel Consumer owns offset commits and refuses a consumer
// with auto-commit on. The sidecar forces it itself, so this is belt and braces on that path; it is
// set here so the two arms are configured from one place and read as the same cluster.
func (b broker) consumerProperties(groupID string) map[string]string {
	return map[string]string{
		"bootstrap.servers":  b.bootstrap,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	}
}
