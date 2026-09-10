/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The fluent API: a modern way to define a Parallel Consumer as connection properties in, one typed route per
 * topic with one processing function each, per-route policy with instance defaults, and a handle out. It is a
 * facade over the engine in the parent package and ships beside the classic options-builder API as an equal;
 * neither is deprecated.
 *
 * <p><b>Incubating.</b> This package is excluded from the API-compatibility gate while its surface settles, so its
 * types may change shape between releases until that exclusion is lifted. Every public type here, and the
 * {@code define} factory that opens a definition, is marked
 * {@link org.apache.kafka.common.annotation.InterfaceStability.Unstable} to say so at the point of use; they are
 * promoted to {@link org.apache.kafka.common.annotation.InterfaceStability.Evolving}, the classic API's level, when
 * that exclusion is lifted. Every behaviour it offers is specified
 * as a record outcome in {@code docs/plans/2026-09-09-002-feat-ux-modernisation-plan.md}, so the engine can take
 * each one over natively later without the surface moving.
 *
 * <p><b>A parked record is reported as slow work in this release.</b> Parking a record is, over today's engine, a
 * retry with a far-future delay - so the engine's shard scan, which measures how long each record it cannot yet
 * take has been waiting, counts every parked record against its partition's slow-records meter and names its topic
 * in the periodic "records in the queue have been waiting longer than" warning, from about ten seconds after it
 * parks until it is resumed or exported. The facade cannot suppress it: the scan has no way to tell a park from a
 * long backoff. It is noise rather than a symptom, and the figures that mean something are the parked count and
 * the parked set. Removing it is a small engine change - the scan skipping a record whose retry delay has not
 * elapsed - and {@code ParkedRecordsAreSlowWorkForNowTest} pins today's behaviour so that change shows up rather
 * than happening in silence.
 */
package bz.stub.parallelconsumer.fluent;
