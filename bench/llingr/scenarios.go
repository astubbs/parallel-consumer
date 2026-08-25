// Copyright (C) 2026 Antony Stubbs and contributors
//
// PRIVATE RESEARCH ONLY. llingr-demux is AGPL-3.0 (or a commercial licence) and patent pending.
// Nothing measured here may be published, quoted externally, or used in marketing. See
// bench/llingr/NOTICE.md and docs/inflight/next-competitor-llingr.md.
//
// # WHAT THIS ADDS TO THE THROUGHPUT ARM IN main.go
//
// The throughput arm measures a clean run in which every record succeeds. That is the one workload
// where the two engines' commit strategies are indistinguishable, and it is also the workload that
// flatters the leaner engine - so measuring only there answers a question nobody is asking.
//
// llingr commits the highest CONTIGUOUS offset and holds out-of-order completions in memory;
// Parallel Consumer encodes the incomplete offset set into the commit metadata and commits PAST the
// gaps. Three scenarios separate them:
//
//   stuck   - one record in N takes far longer than the rest, while the committed offset is sampled
//             from the BROKER (not from the engine) throughout.
//   crash   - the same, killed with os.Exit mid-flight: no drain, no shutdown callback, no final
//             commit. Paired with resume, which rejoins the same group and counts what comes back.
//   retry   - a percentage of records fail on first delivery. llingr routes those to the dead-letter
//             handler and commits them anyway ("Note: failed messages will still be committed so the
//             pipeline can keep processing" - nexus.WriteDeadLetter's own doc comment), so the
//             measurement is how much work reaches a dead-letter path that a retry would have saved.
//
// STATE THE BIAS: these scenarios are chosen because they favour Parallel Consumer, exactly as the
// pure-throughput comparison is chosen because it favours a leaner engine. bench/README.md lists the
// workloads where this comparison is unfair to llingr, and that list is part of the result.
//
// # THE SAMPLER READS THE BROKER, NOT THE ENGINE
//
// Committed offsets come from kadm.FetchOffsets against the consumer group - the same question a
// user asks with kafka-consumer-groups.sh. Neither engine is allowed to answer it about itself, so
// the number means the same thing on both sides, and the Java arm's sampler asks it identically
// through the Java Admin client.

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/llingr/llingr-adapter-franz/franzadapter"
	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Declared at package level so they register with the default FlagSet before main's flag.Parse(),
// which keeps main.go's edit to the four lines that dispatch on -scenario.
var (
	scenario    = flag.String("scenario", "", "stuck|crash|resume|retry; empty runs the throughput arm in main.go")
	stall       = flag.Duration("stall", 0, "how long the stuck record takes; the whole point of the stuck scenario")
	stallEvery  = flag.Int("stall-every", 0, "one record in this many stalls; 0 disables")
	seriesPath  = flag.String("series", "", "write the committed-offset time series here as CSV")
	sampleEvery = flag.Duration("sample-every", 250*time.Millisecond, "committed-offset sampling interval")
	haltAfter   = flag.Int("halt-after", 0, "crash scenario: os.Exit after this many completions")
	processedTo = flag.String("processed-out", "", "crash scenario: write completed offsets here")
	processedIn = flag.String("processed-in", "", "resume scenario: offsets the crashed run had completed")
	failPercent = flag.Int("fail-percent", 0, "retry scenario: percentage of records that fail on first delivery")
	quiesce     = flag.Duration("quiesce", 8*time.Second, "resume scenario: stop after this long with no delivery")
	// COMMIT CADENCE IS A CONTROL. Both engines default to 5s (llingr's AutoCommitInterval, PC's
	// DEFAULT_COMMIT_INTERVAL); at that default a crash test measures commit LAG rather than commit
	// STRATEGY, which is what the first restart run actually measured. Zero keeps the library default.
	commitInterval = flag.Duration("commit-interval", 0, "AutoCommitInterval; matched to the Java arm's commitInterval")
)

var (
	delivered    atomic.Int64
	completedCnt atomic.Int64
	deadLettered atomic.Int64
	redelivered  atomic.Int64
	lastDelivery atomic.Int64 // unix millis

	committedOffset atomic.Int64
	metadataBytes   atomic.Int64
	// The raw committed metadata; "40 bytes" is not evidence of WHAT is in those bytes.
	metadataRaw       atomic.Value
	maxDivergence     atomic.Int64
	maxCommitFreezeMs atomic.Int64
	highestCompleted  atomic.Int64
)

// The counters as they stood when the run's stop condition was met. Not cosmetic: with 100 workers
// in flight the engine keeps completing records between the target being hit and the summary being
// printed, so a summary read live reports whatever the engine reached rather than what was asked
// for. The Java arm snapshots at the same point, for the same reason.
var (
	snapOnce      sync.Once
	snapTaken     bool
	snapCompleted int64
	snapDelivered int64
	snapDead      int64
	// The COMMITTED side is frozen at the same instant, and that is not tidiness: sampling on after
	// the run stops lets a commit that happened AFTER it land in the summary, which the Java arm was
	// caught doing - one run reported a fully-advanced committed offset that contradicted its own
	// time series. The last reading taken while the run was live is at most one interval stale and
	// cannot be contaminated.
	snapCommitted int64
	snapMetaBytes int64
	snapMetaRaw   string
)

func takeSnapshot() {
	snapCommitted = committedOffset.Load()
	snapMetaBytes = metadataBytes.Load()
	snapMetaRaw, _ = metadataRaw.Load().(string)
	snapDelivered = delivered.Load()
	snapDead = deadLettered.Load()
	snapCompleted = completedCnt.Load() // last, so completed is never ahead of delivered
}

// finish takes the snapshot and stops the clock, exactly once however many workers reach the target
// together.
func finish(done chan time.Time) {
	snapOnce.Do(func() {
		takeSnapshot()
		snapTaken = true
		select {
		case done <- time.Now():
		default:
		}
	})
}

// scenarioConfig carries what every scenario needs, so a new scenario cannot silently disagree with
// the others about the broker, the topic or the group.
type scenarioConfig struct {
	bootstrap   string
	topic       string
	group       string
	count       int
	delay       time.Duration
	concurrency int
	timeout     time.Duration
}

func runScenario(cfg scenarioConfig) {
	committedOffset.Store(-1)
	highestCompleted.Store(-1)
	lastDelivery.Store(time.Now().UnixMilli())

	if cfg.group == "" {
		cfg.group = fmt.Sprintf("bench-llingr-%s-%d", *scenario, time.Now().UnixNano())
	}
	if cfg.concurrency > concurrentKeysMax {
		fmt.Fprintf(os.Stderr, "llingr-bench: ConcurrentKeys capped at %d by the library; %d requested\n",
			concurrentKeysMax, cfg.concurrency)
		cfg.concurrency = concurrentKeysMax
	}

	switch *scenario {
	case "stuck":
		runStuck(cfg)
	case "crash":
		runCrash(cfg)
	case "resume":
		runResume(cfg)
	case "retry":
		runRetry(cfg)
	default:
		fmt.Fprintf(os.Stderr, "llingr-bench: unknown scenario %q\n", *scenario)
		os.Exit(2)
	}
}

// --- scenarios ------------------------------------------------------------------------------------

func runStuck(cfg scenarioConfig) {
	done := make(chan time.Time, 1)
	process := func(_ context.Context, msg *nexus.Message[*kgo.Record]) error {
		doWork(msg.Offset, cfg.delay)
		if completedCnt.Load() >= int64(cfg.count) {
			finish(done)
		}
		return nil
	}

	consumer, stopSampling := start(cfg, process, deadLetterCounting)
	started := time.Now()
	subscribe(consumer)
	elapsed := await(done, cfg.timeout, started)
	stopSampling()

	emit("stuck", cfg, elapsed, fmt.Sprintf(" stallMs=%d stallEvery=%d delayMs=%d",
		stall.Milliseconds(), *stallEvery, cfg.delay.Milliseconds()))
	shutdown(consumer)
}

// os.Exit rather than an orderly Shutdown: the shutdown path drains and commits, which is exactly
// what a crashed process does not do. Whatever the broker holds at that instant is what a restart
// has to work from, and that is the thing being measured.
func runCrash(cfg scenarioConfig) {
	logEnd := cfg.count
	var mu sync.Mutex
	finished := make([]bool, logEnd+1)
	done := make(chan time.Time, 1)

	process := func(_ context.Context, msg *nexus.Message[*kgo.Record]) error {
		off := msg.Offset
		doWork(off, cfg.delay)
		if off >= 0 && int(off) < len(finished) {
			mu.Lock()
			finished[off] = true
			mu.Unlock()
		}
		if completedCnt.Load() >= int64(*haltAfter) {
			finish(done)
		}
		return nil
	}

	consumer, stopSampling := start(cfg, process, deadLetterCounting)
	started := time.Now()
	subscribe(consumer)
	elapsed := await(done, cfg.timeout, started)
	stopSampling()

	if *processedTo != "" {
		mu.Lock()
		var sb strings.Builder
		for i, ok := range finished {
			if ok {
				sb.WriteString(strconv.Itoa(i))
				sb.WriteByte('\n')
			}
		}
		mu.Unlock()
		if err := os.WriteFile(*processedTo, []byte(sb.String()), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "llingr-bench: cannot write %s: %v\n", *processedTo, err)
			os.Exit(1)
		}
	}

	emit("crash", cfg, elapsed, fmt.Sprintf(" stallMs=%d stallEvery=%d delayMs=%d haltAfter=%d",
		stall.Milliseconds(), *stallEvery, cfg.delay.Milliseconds(), *haltAfter))
	os.Stdout.Sync()
	os.Exit(0)
}

// Rejoins the group the crashed run left behind and counts what comes back. redelivered is the
// number of deliveries here that the crashed run had ALREADY completed - wasted work, the number a
// user pays for. No stall this time: the run has to reach the end of the log for the count to mean
// anything.
func runResume(cfg scenarioConfig) {
	wasDone := readOffsets(*processedIn)
	process := func(_ context.Context, msg *nexus.Message[*kgo.Record]) error {
		off := msg.Offset
		// No stall on a resume: passing 0/0 rather than the scenario's values, deliberately.
		delivered.Add(1)
		lastDelivery.Store(time.Now().UnixMilli())
		enter()
		time.Sleep(cfg.delay)
		leave()
		completedCnt.Add(1)
		bumpHighest(off)
		if off >= 0 && int(off) < len(wasDone) && wasDone[off] {
			redelivered.Add(1)
		}
		return nil
	}

	consumer, stopSampling := start(cfg, process, deadLetterCounting)
	lastDelivery.Store(0)
	subscribedAt := time.Now()
	subscribe(consumer)
	// The clock starts at the FIRST redelivered record, not at subscribe: the crashed member's group
	// slot has to expire before this one is assigned anything, and that wait is a property of the
	// broker's session timeout, not of either engine's reprocessing cost.
	for lastDelivery.Load() == 0 && time.Since(subscribedAt) < cfg.timeout {
		time.Sleep(50 * time.Millisecond)
	}
	if lastDelivery.Load() == 0 {
		fmt.Fprintln(os.Stderr, "llingr-bench: no records redelivered within the timeout")
	}
	started := time.UnixMilli(lastDelivery.Load())
	if lastDelivery.Load() == 0 {
		started = time.Now()
	}
	// Quiescence, not a target count: how many records come back is the unknown being measured, so
	// the run cannot be told in advance when to stop.
	for {
		idle := time.Since(time.UnixMilli(lastDelivery.Load()))
		if idle > *quiesce && inFlight.Load() == 0 {
			break
		}
		if time.Since(subscribedAt) > cfg.timeout {
			fmt.Fprintf(os.Stderr, "llingr-bench: resume timed out after %s\n", cfg.timeout)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	elapsed := time.Since(started) - *quiesce
	takeSnapshot()
	stopSampling()

	emit("resume", cfg, elapsed, fmt.Sprintf(" alreadyDoneBeforeCrash=%d delayMs=%d",
		countTrue(wasDone), cfg.delay.Milliseconds()))
	shutdown(consumer)
}

// A percentage of records fail on first delivery. In PC those records are retried and eventually
// succeed; here the engine has no retry, so each one goes to the dead-letter handler and is
// committed anyway. deadLettered is therefore the count of work that reached a dead-letter path a
// retry would have saved.
func runRetry(cfg scenarioConfig) {
	every := 0
	if *failPercent > 0 {
		every = 100 / *failPercent
		if every < 1 {
			every = 1
		}
	}
	done := make(chan time.Time, 1)
	process := func(_ context.Context, msg *nexus.Message[*kgo.Record]) error {
		off := msg.Offset
		delivered.Add(1)
		lastDelivery.Store(time.Now().UnixMilli())
		enter()
		time.Sleep(cfg.delay)
		leave()
		if every > 0 && off%int64(every) == 0 {
			// The same predicate the Java arm uses, so both engines fail the same offsets. There it
			// is qualified by "first attempt"; here there is only ever one attempt.
			return fmt.Errorf("transient failure, offset %d", off)
		}
		completedCnt.Add(1)
		bumpHighest(off)
		if completedCnt.Load()+deadLettered.Load() >= int64(cfg.count) {
			finish(done)
		}
		return nil
	}
	deadLetter := func(_ context.Context, _ *nexus.Message[*kgo.Record], _ error) error {
		deadLettered.Add(1)
		if completedCnt.Load()+deadLettered.Load() >= int64(cfg.count) {
			finish(done)
		}
		return nil
	}

	consumer, stopSampling := start(cfg, process, deadLetter)
	started := time.Now()
	subscribe(consumer)
	elapsed := await(done, cfg.timeout, started)
	stopSampling()

	emit("retry", cfg, elapsed, fmt.Sprintf(" failPercent=%d retryDelayMs=0 delayMs=%d",
		*failPercent, cfg.delay.Milliseconds()))
	shutdown(consumer)
}

// --- shared ---------------------------------------------------------------------------------------

// doWork is the handler body every scenario except retry shares: the same accounting as
// Bench.java.template and Divergence.java.template, so the peak and completed columns mean the same
// thing on both sides of the comparison.
func doWork(offset int64, delay time.Duration) {
	delivered.Add(1)
	lastDelivery.Store(time.Now().UnixMilli())
	enter()
	// offset%stallEvery == 1 rather than == 0, so the stalled record sits near the START of the log.
	// A stall at the end leaves nothing behind it and no divergence to measure.
	if *stallEvery > 0 && *stall > 0 && offset%int64(*stallEvery) == 1 {
		time.Sleep(*stall)
	} else {
		time.Sleep(delay)
	}
	leave()
	completedCnt.Add(1)
	bumpHighest(offset)
}

func enter() {
	now := inFlight.Add(1)
	for {
		p := peak.Load()
		if now <= p || peak.CompareAndSwap(p, now) {
			break
		}
	}
}

func leave() { inFlight.Add(-1) }

func bumpHighest(off int64) {
	for {
		h := highestCompleted.Load()
		if off <= h || highestCompleted.CompareAndSwap(h, off) {
			return
		}
	}
}

// Never expected to fire outside the retry scenario; counted rather than ignored, because a dead
// letter that silently succeeded would show up as a completed record and quietly corrupt a result.
func deadLetterCounting(_ context.Context, _ *nexus.Message[*kgo.Record], reason error) error {
	deadLettered.Add(1)
	fmt.Fprintf(os.Stderr, "llingr-bench: unexpected dead letter: %v\n", reason)
	return nil
}

func start(cfg scenarioConfig, process nexus.ProcessMessage[*kgo.Record],
	deadLetter nexus.WriteDeadLetter[*kgo.Record]) (nexus.Consumer[*kgo.Record], func()) {

	builder := demux.NewBuilder[*kgo.Record](cfg.topic, process, deadLetter).
		WithLogger(quietLogger{}).
		WithShutdownCallback(func(_ context.Context, reason error) {
			if reason != nil {
				fmt.Fprintf(os.Stderr, "llingr-bench: emergency shutdown: %v\n", reason)
			}
		}).
		// Only the concurrency dial and the commit cadence are set; everything else takes the
		// library's production defaults. Both are matched to the Java arm, and both are recorded in
		// the result line rather than left for a reader to assume.
		WithDemuxConfig(config.DemuxConfig{
			ConcurrentKeys:     cfg.concurrency,
			AutoCommitInterval: *commitInterval,
		})

	adapter := franzadapter.NewWithOptions(context.Background(), cfg.group, []string{cfg.bootstrap},
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Matched to the Java arm's consumer, for the reason recorded there: the crash scenario kills
		// the process outright, so the dead member holds its assignment until the session expires,
		// and at a 45s default the restart measurement would be 45 seconds of waiting with no records
		// in it. 6s is the broker's group.min.session.timeout.ms.
		kgo.SessionTimeout(6*time.Second),
		kgo.HeartbeatInterval(2*time.Second))

	consumer, err := adapter.CreateConsumer(builder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "llingr-bench: cannot create consumer: %v\n", err)
		os.Exit(1)
	}
	stop := startSampler(cfg)
	return consumer, stop
}

func subscribe(consumer nexus.Consumer[*kgo.Record]) {
	if err := consumer.Subscribe(); err != nil {
		fmt.Fprintf(os.Stderr, "llingr-bench: subscribe failed: %v\n", err)
		os.Exit(1)
	}
}

func shutdown(consumer nexus.Consumer[*kgo.Record]) {
	// After the clock has stopped and after the result is printed, so a drain cannot inflate either.
	if err := consumer.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "llingr-bench: shutdown: %v\n", err)
	}
}

func await(done chan time.Time, timeout time.Duration, started time.Time) time.Duration {
	select {
	case at := <-done:
		return at.Sub(started)
	case <-time.After(timeout):
		fmt.Fprintf(os.Stderr, "llingr-bench: timed out after %s with %d completed, %d dead lettered\n",
			timeout, completedCnt.Load(), deadLettered.Load())
		return time.Since(started)
	}
}

// startSampler polls the group's COMMITTED offset from the broker and returns a stop function.
// A separate client, in no group, so sampling cannot perturb the consumer being measured.
func startSampler(cfg scenarioConfig) func() {
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.bootstrap))
	if err != nil {
		fmt.Fprintf(os.Stderr, "llingr-bench: sampler client: %v\n", err)
		os.Exit(1)
	}
	adm := kadm.NewClient(cl)
	stopped := make(chan struct{})
	t0 := time.Now()

	go func() {
		var w *bufio.Writer
		if *seriesPath != "" {
			f, err := os.Create(*seriesPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "llingr-bench: cannot write series: %v\n", err)
			} else {
				defer f.Close()
				w = bufio.NewWriter(f)
				fmt.Fprintln(w, "t_ms,completed,delivered,committed_offset,divergence,in_flight,metadata_bytes")
			}
		}
		defer func() {
			if w != nil {
				w.Flush()
			}
			adm.Close()
		}()

		lastCommitted := int64(-2)
		lastMoved := t0
		for {
			select {
			case <-stopped:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			resps, err := adm.FetchOffsets(ctx, cfg.group)
			cancel()
			if err == nil {
				if r, ok := resps.Lookup(cfg.topic, 0); ok && r.Err == nil {
					committedOffset.Store(r.At)
					metadataBytes.Store(int64(len(r.Metadata)))
					metadataRaw.Store(r.Metadata)
				}
			}
			// A missed sample is a gap in a chart, not a failed measurement; losing a run because
			// the coordinator was briefly unavailable would be worse.
			now := time.Now()
			done := completedCnt.Load()
			committed := committedOffset.Load()
			var committedCount int64
			if committed > 0 {
				committedCount = committed
			}
			div := done - committedCount
			for {
				m := maxDivergence.Load()
				if div <= m || maxDivergence.CompareAndSwap(m, div) {
					break
				}
			}
			if committed != lastCommitted {
				lastCommitted = committed
				lastMoved = now
			} else if done > 0 {
				frozen := now.Sub(lastMoved).Milliseconds()
				if frozen > maxCommitFreezeMs.Load() {
					maxCommitFreezeMs.Store(frozen)
				}
			}
			if w != nil {
				fmt.Fprintf(w, "%d,%d,%d,%d,%d,%d,%d\n", now.Sub(t0).Milliseconds(), done,
					delivered.Load(), committed, div, inFlight.Load(), metadataBytes.Load())
				w.Flush() // per sample: the crash scenario kills the process by design
			}
			time.Sleep(*sampleEvery)
		}
	}()

	return func() { close(stopped) }
}

// emit prints the two machine-readable lines, in the same shape and field order the Java arm uses,
// so one parser reads both engines. RESULT stays byte-compatible with what run-bisect.sh already
// parses; RESULT2 is key=value so a field can be added without invalidating a stored results file.
func emit(name string, cfg scenarioConfig, elapsed time.Duration, extra string) {
	ms := elapsed.Milliseconds()
	if ms <= 0 {
		ms = 1
	}
	if !snapTaken {
		// The resume scenario stops on quiescence rather than on a target, so it has no earlier
		// snapshot point; taking one here is correct for it and a no-op for the others.
		takeSnapshot()
	}
	done := snapCompleted
	committed := snapCommitted
	var committedCount int64
	if committed > 0 {
		committedCount = committed
	}
	fmt.Printf("RESULT %s %d %d %.1f peak=%d\n", name, done, ms, float64(done)*1000.0/float64(ms), peak.Load())
	fmt.Printf("RESULT2 scenario=%s engine=llingr ms=%d completed=%d delivered=%d redelivered=%d retries=0"+
		" committedOffset=%d highestCompletedOffset=%d divergence=%d maxDivergence=%d maxCommitFreezeMs=%d"+
		" metadataBytes=%d peakInFlight=%d deadLettered=%d commitIntervalMs=%d%s\n",
		name, ms, done, snapDelivered, redelivered.Load(),
		committed, highestCompleted.Load(), done-committedCount, maxDivergence.Load(), maxCommitFreezeMs.Load(),
		snapMetaBytes, peak.Load(), snapDead, commitIntervalMsOrDefault(), extra)
	// Its own line, not a RESULT2 field: base64 padding contains '=', which would truncate the value
	// in any key=value parser - including this harness's own.
	fmt.Printf("METADATA %s\n", snapMetaRaw)
}

func readOffsets(path string) []bool {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "llingr-bench: cannot read %s: %v\n", path, err)
		os.Exit(1)
	}
	lines := strings.Fields(string(data))
	max := 0
	nums := make([]int, 0, len(lines))
	for _, l := range lines {
		n, err := strconv.Atoi(l)
		if err != nil {
			continue
		}
		nums = append(nums, n)
		if n > max {
			max = n
		}
	}
	out := make([]bool, max+1)
	for _, n := range nums {
		out[n] = true
	}
	return out
}

func countTrue(b []bool) int {
	n := 0
	for _, v := range b {
		if v {
			n++
		}
	}
	return n
}

// The library's own default when the flag leaves it at zero, so a results row always states the
// cadence that was actually in force rather than "0" meaning "whatever the library felt like".
func commitIntervalMsOrDefault() int64 {
	if *commitInterval > 0 {
		return commitInterval.Milliseconds()
	}
	return 5000
}
