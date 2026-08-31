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
// Its contract - flags, exit codes, the stdout lines, the behaviour tokens - is documented once, in
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
	behaviourHoldUntilCeilingFull = "hold-until-ceiling-full"
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

// ceilingSettle is how long hold-until-ceiling-full keeps a FULL group held before releasing it.
//
// IT IS WHAT TURNS "THE CEILING WAS NEVER EXCEEDED" FROM A RACE INTO A MEASUREMENT. Release the
// group the instant it fills and a client that declared a larger ceiling still passes - its extra
// records arrive a few milliseconds later, by which time the outstanding count has already fallen
// back. Holding the full ceiling still means the extra dispatch arrives INSIDE the window and prints
// its line while every other record is unresolved. A correct engine cannot dispatch anything during
// the window at all, so the wait costs a conforming client nothing but time.
const ceilingSettle = 250 * time.Millisecond

// The two lines this runner prints per delivery, on stdout: one the moment the record arrives, and
// one the moment the prescribed behaviour has DECIDED that record's outcome. Both are observations,
// never verdicts - the Java suite parses them and decides what they mean. reason comes last because
// it is worker-supplied text that may contain spaces; on a dispatch it is the history the record
// ARRIVED with, on a settled line it is the failure this runner REPORTED, empty for a success.
//
// THE ORDER OF THESE LINES IS THE WHOLE OF WHAT THE SUITE READS, and no clock is involved: a
// dispatch opens a record's unresolved window and its settled line closes it, so the running
// difference between the two counts, in line order, is how many records this client was holding at
// that instant - which is what max concurrency bounds. Both are therefore printed under the
// tracker's mutex, because two goroutines that took their counts in one order must not then reach
// stdout in the other.
//
// report-nothing prints no settled line, EVER: by prescription its record is never resolved, and the
// absence is the observation.
const (
	dispatchLineFormat = "dispatch key=%s offset=%d attempt=%d reason=%s\n"
	settledLineFormat  = "settled key=%s offset=%d attempt=%d reason=%s\n"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("conformance-runner", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	scenario := fs.String("scenario", "", "the conformance scenario's name, which is also the topic to subscribe to")
	behaviour := fs.String("behaviour", "", "what to do with each dispatch: succeed | report-nothing | fail-then-succeed | hold-first-until-second | hold-until-ceiling-full")
	sidecar := fs.String("sidecar", "", "absolute path of the sidecar command to spawn")
	expect := fs.Int("expect-dispatches", 0, "how many dispatches the scenario prescribes before this runner exits")
	maxConcurrency := fs.Int("max-concurrency", 0, "the in-flight ceiling to configure on the session; the only thing it is set from")
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
	case *maxConcurrency < 1:
		return usage(fs, "--max-concurrency must be at least 1")
	case *budget < 1:
		return usage(fs, "--timeout-seconds must be at least 1")
	}
	switch *behaviour {
	case behaviourSucceed, behaviourReportNothing, behaviourFailThenSucceed, behaviourHoldFirstUntilSecond,
		behaviourHoldUntilCeilingFull:
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
		// THE CEILING IS THE SCENARIO'S TO CHOOSE, and this runner never derives one. It used to be
		// set from --expect-dispatches, which is by construction a ceiling no scenario can reach - so
		// no scenario could ask a client to prove it respected one, and none did.
		MaxConcurrency:           int32(*maxConcurrency),
		CommitInterval:           commitInterval,
		DefaultMessageRetryDelay: retryDelay,
		InstanceTag:              "conformance-runner-go",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "conformance-runner: opening the session for scenario %q: %v\n", *scenario, err)
		return exitBehaviourFailed
	}

	t := newTracker(*expect, *maxConcurrency)

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

		// Each branch settles the record - prints its settled line and counts it - IMMEDIATELY BEFORE
		// returning the outcome, so the line lands while the record is still unresolved to the engine.
		// report-nothing is the one behaviour that never settles: its record is never resolved.
		switch behaviour {
		case behaviourSucceed:
			t.settle(record, "")
			return parallelconsumer.Succeed(), nil

		case behaviourReportNothing:
			// Never report. Blocking here is how a Go worker says "this record's function has not
			// returned"; the process exits with the record still in flight.
			<-ctx.Done()
			return parallelconsumer.Succeed(), nil

		case behaviourFailThenSucceed:
			if record.Attempt == 1 {
				t.settle(record, prescribedFailureReason)
				return parallelconsumer.Outcome{}, errors.New(prescribedFailureReason)
			}
			t.settle(record, "")
			return parallelconsumer.Succeed(), nil

		case behaviourHoldFirstUntilSecond:
			if ordinal == 1 {
				// Hold the first record until a SECOND is dispatched. Whether one arrives at all,
				// and which key it carries, is the whole of what the scenario is asking - and it is
				// the Java suite that decides what the answer means.
				select {
				case <-t.secondArrived:
				case <-ctx.Done():
					reason := "conformance: no second dispatch arrived while the first was held"
					t.settle(record, reason)
					return parallelconsumer.Outcome{}, errors.New(reason)
				}
			}
			t.settle(record, "")
			return parallelconsumer.Succeed(), nil

		case behaviourHoldUntilCeilingFull:
			// Hold until --max-concurrency records are held AT ONCE, keep the full group still for
			// the settle window, then succeed the whole group. Blocking is how a Go worker says the
			// record is still out, so a held record is genuinely unresolved for as long as its
			// dispatch line says it is - which is the property the scenario measures.
			if err := t.enterCeilingGroup(ctx); err != nil {
				t.settle(record, err.Error())
				return parallelconsumer.Outcome{}, err
			}
			t.settle(record, "")
			return parallelconsumer.Succeed(), nil
		}

		// unreachable: run() rejects an unknown behaviour before the session opens
		return parallelconsumer.Outcome{}, fmt.Errorf("conformance: unknown behaviour %q", behaviour)
	}
}

// tracker counts deliveries and outcomes, prints both observation lines, and owns the ceiling
// group's barrier. It holds no per-record state - only counts - because the client library holds
// none either and this runner must not become the place where a client's missing bookkeeping is
// quietly supplied.
//
// ITS MUTEX IS ALSO THE STDOUT LOCK. The suite reads the client's outstanding count from nothing but
// the order of the dispatch and settled lines, so the print has to happen in the same critical
// section as the count it corresponds to - see dispatchLineFormat.
type tracker struct {
	expected       int
	maxConcurrency int

	mu        sync.Mutex
	observed  int
	completed int

	// The hold-until-ceiling-full barrier's state: how many records are held right now, and the
	// channel every one of them is waiting on - see enterCeilingGroup.
	ceilingHeld    int
	ceilingRelease chan struct{}

	secondOnce    sync.Once
	observedOnce  sync.Once
	completedOnce sync.Once

	secondArrived chan struct{}
	allObserved   chan struct{}
	allCompleted  chan struct{}
}

func newTracker(expected, maxConcurrency int) *tracker {
	return &tracker{
		expected:       expected,
		maxConcurrency: maxConcurrency,
		ceilingRelease: make(chan struct{}),
		secondArrived:  make(chan struct{}),
		allObserved:    make(chan struct{}),
		allCompleted:   make(chan struct{}),
	}
}

// observe prints the delivery and returns its 1-based ordinal in arrival order.
func (t *tracker) observe(record parallelconsumer.InboundRecord) int {
	t.mu.Lock()
	t.observed++
	ordinal := t.observed
	// PRINTED INSIDE THE LOCK, not after it. Several shards deliver concurrently here, and a print
	// moved outside would let two goroutines take their ordinals in one order and print in the other,
	// which is the suite reading an overlap that never happened - or missing one that did.
	fmt.Printf(dispatchLineFormat, string(record.Key), record.Offset, record.Attempt, record.LastFailureReason)
	t.mu.Unlock()

	if ordinal >= 2 {
		t.secondOnce.Do(func() { close(t.secondArrived) })
	}
	if ordinal >= t.expected {
		t.observedOnce.Do(func() { close(t.allObserved) })
	}
	return ordinal
}

// settle prints the record's decided outcome - the moment it stops being unresolved - and counts it
// as completed. reason is the failure this runner is REPORTING, empty for a success, and never the
// reason the record arrived with.
func (t *tracker) settle(record parallelconsumer.InboundRecord, reason string) {
	t.mu.Lock()
	fmt.Printf(settledLineFormat, string(record.Key), record.Offset, record.Attempt, reason)
	t.completed++
	reached := t.completed >= t.expected
	t.mu.Unlock()
	if reached {
		t.completedOnce.Do(func() { close(t.allCompleted) })
	}
}

// enterCeilingGroup is the cyclic barrier at the heart of hold-until-ceiling-full: block until this
// record is one of maxConcurrency held at once, keep the full group still for ceilingSettle, and
// release every member of it. It is called AFTER the dispatch line has been printed, so the arrivals
// the scenario is looking for are on stdout before anything waits on anything.
//
// THE GENERATION IS THE CHANNEL, not a counter. A waiter captures the current release channel under
// the mutex; the releaser closes that channel and installs a fresh one for the next group. A closed
// channel wakes every waiter at once and stays closed, so this needs neither the re-check loop nor
// the spurious-wakeup guard that a condition variable would - which is what the other languages'
// "wait until generation != myGeneration" spells out by hand.
//
// A group also releases once every prescribed delivery has been observed, so a scenario whose record
// count is not a multiple of its ceiling cannot strand its last, short group.
//
// The wait is bounded by ctx, which carries the runner's whole --timeout-seconds budget: a group that
// never fills fails this record AND leaves run()'s own select on ctx.Done to exit 1, which is the
// same path every other uncompletable prescription already takes.
func (t *tracker) enterCeilingGroup(ctx context.Context) error {
	t.mu.Lock()
	t.ceilingHeld++
	releasing := t.ceilingHeld >= t.maxConcurrency || t.observed >= t.expected
	if !releasing {
		release := t.ceilingRelease
		t.mu.Unlock()
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("conformance: the ceiling group of %d never filled", t.maxConcurrency)
		}
	}
	t.mu.Unlock()

	// THE SETTLE WINDOW, SLEPT OUTSIDE THE LOCK. A record the engine should not be dispatching still
	// has to be able to print its arrival line during the window - that arrival is the whole thing
	// the scenario looks for - and holding the mutex across the sleep would block exactly that print.
	time.Sleep(ceilingSettle)

	t.mu.Lock()
	t.ceilingHeld = 0
	close(t.ceilingRelease) // wakes every waiter of this generation, and only them
	t.ceilingRelease = make(chan struct{})
	t.mu.Unlock()
	return nil
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
