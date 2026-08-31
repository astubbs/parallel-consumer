// Copyright (C) 2026 Antony Stubbs and contributors

package parallelconsumer

import "context"

// Outcome is what a Processor produces on the success path: nothing, or records for the proxy to
// produce. Failure is not a variant of it, because Go already has a way to say "this failed" - see
// Processor.
type Outcome struct {
	produce []OutboundRecord
}

// Succeed reports the record processed, with no output.
func Succeed() Outcome { return Outcome{} }

// Produce reports the record processed, asking the proxy to produce these records with its own
// producer before the input record's offset may become eligible to commit. This is the only
// sanctioned route for worker output to Kafka.
func Produce(records ...OutboundRecord) Outcome { return Outcome{produce: records} }

// Processor is the user's function.
//
// ERRORS ARE RETURNED, NOT THROWN, and that is the whole of the failure path: a non-nil error is a
// failure outcome and its Error() text is the reason that rides the redelivery. There is no
// separate Fail() constructor to keep in step with it, and no way to return both a success and an
// error. A panic is recovered and reported as a failure too - a worker crash must not tear down
// the stream - but returning an error is the supported spelling.
//
// The reason text is worker-supplied and reaches the proxy's logs and the next delivery: do not
// put record payload or credentials in it.
type Processor func(ctx context.Context, record InboundRecord) (Outcome, error)
