// Copyright (C) 2026 Antony Stubbs and contributors

// Command conformance-runner is Go's half of the shared cross-language conformance suite
// (astubbs#242, confluentinc#154).
//
// IT ASSERTS NOTHING, DELIBERATELY. The suite that knows what correct looks like - offset
// frontiers, ordering, redelivery, attempt counts - is the Java module
// parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance, and it keeps owning that
// knowledge for every language. This binary's whole job is to DO WHAT THE SCENARIO SAYS and then
// exit; if it were free to decide what "correct" means, ten languages would each decide it
// slightly differently and the agreement between them would prove nothing.
//
// Its contract - flags, exit codes, the stdout line, the behaviour tokens - is documented once, in
// parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance/README.md, and is identical
// in every language. Read that before writing the next one.
//
// THIS DOES NOT REPLACE THE PACKAGE'S OWN TESTS, and must not be read as doing so. The shared
// suite proves every client behaves identically on the protocol; parallelconsumer's own Go tests
// catch what is invisible from outside the process - a blocked transport goroutine, a swallowed
// context cancellation, a leaked child - none of which is expressible as a protocol scenario. The
// two layers answer different questions and both are load-bearing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/parallelconsumer"
)

// Exit statuses ARE the verdict channel. There is no results file, no report message and no second
// protocol: a scenario passed if this process exited 0 and the Java suite's own assertions about
// engine state held. Carrying test results over a wire would be the whole wire problem again,
// multiplied by ten languages, to say something an exit status already says.
const (
	exitOK              = 0
	exitBehaviourFailed = 1
	exitUsage           = 2
)

// The behaviour tokens. A scenario names one; this binary implements exactly these and rejects
// anything else, so a scenario can never be run by a behaviour nobody wrote.
const (
	behaviourSucceed              = "succeed"
	behaviourReportNothing        = "report-nothing"
	behaviourFailThenSucceed      = "fail-then-succeed"
	behaviourHoldFirstUntilSecond = "hold-first-until-second"
)

// prescribedFailureReason is the exact text a fail-then-succeed run reports as its failure reason.
// The Java suite asserts the redelivery carries it back VERBATIM, so it is a fixed literal of the
// contract in every language, never a message this runner composes.
const prescribedFailureReason = "conformance-prescribed-failure"

// Fixed session tunables, part of the contract rather than of this runner's judgement: they exist
// only so scenarios converge at unit-test speed against the engine's much slower production
// defaults (a 5s commit interval, a 1s retry delay). Every language sets the same two values.
const (
	commitInterval = 100 * time.Millisecond
	retryDelay     = 50 * time.Millisecond
)

// reportNothingHold is how long a report-nothing run keeps its session OPEN after its last
// observation, before exiting.
//
// IT IS WHAT MAKES THE NEGATIVE CONTROL A CONTROL. Without it the runner exits the instant the
// record arrives, and a sabotaged runner that DID report success has its report killed in flight
// by the process exit - so the suite sees an unadvanced offset either way and the scenario passes
// for a broken client. Measured, not reasoned about: reporting success from this behaviour left
// the suite green until this hold existed. Thirty of the contract's commit intervals, so a report
// that was sent has been committed and seen long before the process goes away.
const reportNothingHold = 3 * time.Second

// dispatchLineFormat is the ONE line this runner prints per delivery, on stdout. It is an
// observation, not a verdict: the Java suite parses these and decides what they mean. reason comes
// last because it is worker-supplied text that may contain spaces.
const dispatchLineFormat = "dispatch key=%s offset=%d attempt=%d reason=%s\n"

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("conformance-runner", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	scenario := fs.String("scenario", "", "the conformance scenario's name, which is also the topic to subscribe to")
	behaviour := fs.String("behaviour", "", "what to do with each dispatch: succeed | report-nothing | fail-then-succeed | hold-first-until-second")
	sidecar := fs.String("sidecar", "", "absolute path of the sidecar command to spawn")
	expect := fs.Int("expect-dispatches", 0, "how many dispatches the scenario prescribes before this runner exits")
	budget := fs.Int("timeout-seconds", 0, "wall-clock budget; exceeding it without completing the behaviour exits 1")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	switch {
	case *scenario == "":
		return usage(fs, "--scenario is required")
	case *behaviour == "":
		return usage(fs, "--behaviour is required")
	case *sidecar == "":
		return usage(fs, "--sidecar is required")
	case !filepath.IsAbs(*sidecar):
		return usage(fs, fmt.Sprintf("--sidecar must be absolute, got %q", *sidecar))
	case *expect < 1:
		return usage(fs, "--expect-dispatches must be at least 1")
	case *budget < 1:
		return usage(fs, "--timeout-seconds must be at least 1")
	}
	switch *behaviour {
	case behaviourSucceed, behaviourReportNothing, behaviourFailThenSucceed, behaviourHoldFirstUntilSecond:
	default:
		return usage(fs, fmt.Sprintf("unknown behaviour %q", *behaviour))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*budget)*time.Second)
	defer cancel()

	client, err := parallelconsumer.Open(ctx, parallelconsumer.Options{
		SidecarPath: *sidecar,
		// The suite hands the sidecar over already parameterised; a runner never chooses the
		// fixture, because a runner that could choose it could choose an easier one.
		SidecarArgs:   nil,
		SidecarStderr: os.Stderr,
		// THE SCENARIO NAME IS ALSO THE TOPIC NAME.
		Topics: []string{*scenario},
		// The mock lane builds mock Kafka clients and reads no properties. Real credentials never
		// belong in a conformance test.
		KafkaProperties: map[string]string{},
		// Enough executors for every dispatch the scenario prescribes, so a scenario that holds a
		// record cannot deadlock on an executor count smaller than its own shape.
		MaxConcurrency:           int32(*expect),
		CommitInterval:           commitInterval,
		DefaultMessageRetryDelay: retryDelay,
		InstanceTag:              "conformance-runner-go",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "conformance-runner: opening the session for scenario %q: %v\n", *scenario, err)
		return exitBehaviourFailed
	}

	t := newTracker(*expect)

	if err := client.Poll(ctx, processorFor(*behaviour, t)); err != nil {
		fmt.Fprintf(os.Stderr, "conformance-runner: starting the poll: %v\n", err)
		_ = client.Close()
		return exitBehaviourFailed
	}

	// report-nothing completes at OBSERVATION, because by prescription its records are never
	// reported and so can never complete. Every other behaviour completes when the last record it
	// was handed has had its outcome decided.
	waitOn := t.allCompleted
	if *behaviour == behaviourReportNothing {
		waitOn = t.allObserved
	}

	select {
	case <-waitOn:
	case <-ctx.Done():
		fmt.Fprintf(os.Stderr, "conformance-runner: scenario %q behaviour %q did not complete within %ds "+
			"- observed %d of %d dispatches, completed %d\n",
			*scenario, *behaviour, *budget, t.observedCount(), *expect, t.completedCount())
		_ = client.Close()
		return exitBehaviourFailed
	}

	if *behaviour == behaviourReportNothing {
		// Hold the session open, reporting nothing, so the suite is watching a LIVE client rather
		// than the wreckage of one - see reportNothingHold.
		select {
		case <-time.After(reportNothingHold):
		case <-ctx.Done():
		}
		// PRESCRIBED: the record is never reported and the session is abandoned rather than
		// drained - a worker that vanished mid-record is exactly what this scenario models. Exiting
		// closes the sidecar's lifecycle pipe, which reaps it, so nothing is leaked by not closing.
		return exitOK
	}

	if err := client.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "conformance-runner: closing the session: %v\n", err)
		return exitBehaviourFailed
	}
	return exitOK
}

func processorFor(behaviour string, t *tracker) parallelconsumer.Processor {
	return func(ctx context.Context, record parallelconsumer.InboundRecord) (parallelconsumer.Outcome, error) {
		ordinal := t.observe(record)

		switch behaviour {
		case behaviourSucceed:
			defer t.complete()
			return parallelconsumer.Succeed(), nil

		case behaviourReportNothing:
			// Never report. Blocking here is how a Go worker says "this record's function has not
			// returned"; the process exits with the record still in flight.
			<-ctx.Done()
			return parallelconsumer.Succeed(), nil

		case behaviourFailThenSucceed:
			defer t.complete()
			if record.Attempt == 1 {
				return parallelconsumer.Outcome{}, errors.New(prescribedFailureReason)
			}
			return parallelconsumer.Succeed(), nil

		case behaviourHoldFirstUntilSecond:
			defer t.complete()
			if ordinal == 1 {
				// Hold the first record until a SECOND is dispatched. Whether one arrives at all,
				// and which key it carries, is the whole of what the scenario is asking - and it is
				// the Java suite that decides what the answer means.
				select {
				case <-t.secondArrived:
				case <-ctx.Done():
					return parallelconsumer.Outcome{}, errors.New("conformance: no second dispatch arrived while the first was held")
				}
			}
			return parallelconsumer.Succeed(), nil
		}

		// unreachable: run() rejects an unknown behaviour before the session opens
		return parallelconsumer.Outcome{}, fmt.Errorf("conformance: unknown behaviour %q", behaviour)
	}
}

// tracker counts deliveries and outcomes, and prints the observation line. It holds no per-record
// state - only counts - because the client library holds none either and this runner must not
// become the place where a client's missing bookkeeping is quietly supplied.
type tracker struct {
	expected int

	mu        sync.Mutex
	observed  int
	completed int

	secondOnce    sync.Once
	observedOnce  sync.Once
	completedOnce sync.Once

	secondArrived chan struct{}
	allObserved   chan struct{}
	allCompleted  chan struct{}
}

func newTracker(expected int) *tracker {
	return &tracker{
		expected:      expected,
		secondArrived: make(chan struct{}),
		allObserved:   make(chan struct{}),
		allCompleted:  make(chan struct{}),
	}
}

// observe prints the delivery and returns its 1-based ordinal in arrival order.
func (t *tracker) observe(record parallelconsumer.InboundRecord) int {
	t.mu.Lock()
	t.observed++
	ordinal := t.observed
	t.mu.Unlock()

	fmt.Printf(dispatchLineFormat, string(record.Key), record.Offset, record.Attempt, record.LastFailureReason)

	if ordinal >= 2 {
		t.secondOnce.Do(func() { close(t.secondArrived) })
	}
	if ordinal >= t.expected {
		t.observedOnce.Do(func() { close(t.allObserved) })
	}
	return ordinal
}

func (t *tracker) complete() {
	t.mu.Lock()
	t.completed++
	reached := t.completed >= t.expected
	t.mu.Unlock()
	if reached {
		t.completedOnce.Do(func() { close(t.allCompleted) })
	}
}

func (t *tracker) observedCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.observed
}

func (t *tracker) completedCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.completed
}

func usage(fs *flag.FlagSet, problem string) int {
	fmt.Fprintf(os.Stderr, "conformance-runner: %s\n\n", problem)
	fs.SetOutput(os.Stderr)
	fs.PrintDefaults()
	return exitUsage
}
