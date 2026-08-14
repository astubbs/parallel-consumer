// Copyright (C) 2026 Antony Stubbs and contributors

package parallelconsumer

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	proxyv1 "github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/gen/parallelconsumer/proxy/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ProcessingOrder mirrors the engine's ordering modes. The zero value means "take the proxy's
// default", which the proxy reports back in the effective session - so a caller asserts what it
// got, never what it asked for.
type ProcessingOrder int32

const (
	OrderDefault   ProcessingOrder = ProcessingOrder(proxyv1.ProcessingOrder_PROCESSING_ORDER_UNSPECIFIED)
	OrderUnordered ProcessingOrder = ProcessingOrder(proxyv1.ProcessingOrder_PROCESSING_ORDER_UNORDERED)
	OrderPartition ProcessingOrder = ProcessingOrder(proxyv1.ProcessingOrder_PROCESSING_ORDER_PARTITION)
	OrderKey       ProcessingOrder = ProcessingOrder(proxyv1.ProcessingOrder_PROCESSING_ORDER_KEY)
)

// Capability tokens this client can declare. The negotiated set is the intersection the proxy
// replies with, and neither side sends a message whose token fell out of it.
const (
	CapabilityDispatch    = "dispatch"
	CapabilityHeartbeat   = "heartbeat"
	CapabilityManifest    = "manifest"
	CapabilityWorkerDeath = "worker-death"
	CapabilityShutdown    = "shutdown"
	CapabilityTerminal    = "terminal"
)

// Options is the whole of a session's configuration. A struct rather than a builder: Go reads a
// literal with named fields more plainly than a chain, and the zero value of every tunable already
// means "the proxy's default", which is exactly the wire's own convention.
//
// Everything here except the Sidecar fields travels in Configure and nowhere else. Nothing reaches
// the proxy by argv, environment or file.
type Options struct {
	// SidecarPath is the ABSOLUTE path of the sidecar binary. It is never resolved through PATH or
	// relative to the working directory: this process hands the sidecar the Kafka credentials, so
	// which binary runs is security-relevant.
	SidecarPath string
	// SidecarArgs are passed to that binary verbatim. They carry no proxy configuration - the
	// conformance harness takes its fixture selection this way, which is its own documented
	// exception, not a licence to configure a shipped sidecar by flag.
	SidecarArgs []string
	// SidecarStderr, when set, receives the sidecar's stderr. Its stdout is the lifecycle channel
	// and belongs to this library.
	SidecarStderr interface{ Write([]byte) (int, error) }

	// Topics and TopicPattern are the subscription, fixed for the sidecar's lifetime. Exactly one
	// must be set.
	Topics       []string
	TopicPattern string

	// MaxConcurrency is the proxy's in-flight ceiling, and therefore this client's dispatch-queue
	// depth. Zero means the proxy's default. There is no "unlimited".
	MaxConcurrency int32

	// KafkaProperties carries credentials. This library never logs it, never echoes an entry of it
	// in an error, and never writes it anywhere but the stream.
	KafkaProperties map[string]string

	// Capabilities are the tokens this client implements. Nil declares only what this client can
	// actually honour today rather than the v1 baseline, because an empty list on the wire means
	// "the whole baseline" and this client is not there yet.
	Capabilities []string

	Ordering ProcessingOrder

	CommitInterval           time.Duration
	DefaultMessageRetryDelay time.Duration
	DrainTimeout             time.Duration

	// TerminalTopic asks for terminal-outcome resolution. It only takes effect when the session
	// also negotiates the terminal capability; the effective session reports whether it did.
	TerminalTopic string

	// InstanceTag tags the engine's metrics and logging.
	InstanceTag string
}

// implementedCapabilities is what this client honours today. Wave one implements the dispatch
// wave, the queue and per-record reporting; heartbeats, the manifest reconnect, worker-death
// reporting, the shutdown drain and terminal outcomes are later waves, so their tokens are not
// declared. Declaring a token the client does not honour would be worse than declaring none: the
// proxy would be entitled to send its messages.
var implementedCapabilities = []string{CapabilityDispatch}

func (o Options) validate() error {
	if o.SidecarPath == "" {
		return errors.New("parallelconsumer: SidecarPath is required")
	}
	if !filepath.IsAbs(o.SidecarPath) {
		return fmt.Errorf("parallelconsumer: SidecarPath must be absolute, got %q - a relative or "+
			"PATH-resolved sidecar is a binary an attacker can influence", o.SidecarPath)
	}
	if (len(o.Topics) == 0) == (o.TopicPattern == "") {
		return errors.New("parallelconsumer: exactly one of Topics or TopicPattern must be set")
	}
	if o.MaxConcurrency < 0 {
		return fmt.Errorf("parallelconsumer: MaxConcurrency must be >= 1 or 0 for the proxy default, got %d", o.MaxConcurrency)
	}
	return nil
}

// configure renders the options as the first message of a fresh session.
func (o Options) configure() *proxyv1.Configure {
	c := &proxyv1.Configure{
		Topics:          o.Topics,
		KafkaProperties: o.KafkaProperties,
		Capabilities:    o.Capabilities,
	}
	if c.Capabilities == nil {
		c.Capabilities = implementedCapabilities
	}
	if o.TopicPattern != "" {
		c.TopicPattern = &o.TopicPattern
	}
	if o.MaxConcurrency > 0 {
		c.MaxConcurrency = &o.MaxConcurrency
	}
	if o.Ordering != OrderDefault {
		ordering := proxyv1.ProcessingOrder(o.Ordering)
		c.Ordering = &ordering
	}
	if o.CommitInterval > 0 {
		c.CommitInterval = durationpb.New(o.CommitInterval)
	}
	if o.DefaultMessageRetryDelay > 0 {
		c.DefaultMessageRetryDelay = durationpb.New(o.DefaultMessageRetryDelay)
	}
	if o.DrainTimeout > 0 {
		c.DrainTimeout = durationpb.New(o.DrainTimeout)
	}
	if o.TerminalTopic != "" {
		c.TerminalTopic = &o.TerminalTopic
	}
	if o.InstanceTag != "" {
		c.PcInstanceTag = &o.InstanceTag
	}
	return c
}

// Session is the effective configuration the proxy replied with: what it is actually running,
// after its own defaults and the capability negotiation. Assert on this, never on Options.
type Session struct {
	Topics         []string
	TopicPattern   string
	MaxConcurrency int32
	ExecutorCount  int32
	Capabilities   []string
	TerminalTopic  string
}

// Negotiated reports whether a capability token survived the handshake. Every duty in this
// protocol is gated by one, so this is how a client decides what it owes.
func (s Session) Negotiated(token string) bool {
	for _, c := range s.Capabilities {
		if c == token {
			return true
		}
	}
	return false
}

func sessionOf(c *proxyv1.Configured) Session {
	return Session{
		Topics:         c.GetTopics(),
		TopicPattern:   c.GetTopicPattern(),
		MaxConcurrency: c.GetMaxConcurrency(),
		ExecutorCount:  c.GetExecutorCount(),
		Capabilities:   c.GetCapabilities(),
		TerminalTopic:  c.GetTerminalTopic(),
	}
}
