/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The fluent API: a modern way to define a Parallel Consumer as connection properties in, one typed route per
 * topic with one processing function each, per-route policy with instance defaults, and a running instance out. It is a
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
 * <p><b>What a running instance offers.</b> The instance closes on its declared
 * {@link bz.stub.parallelconsumer.fluent.ClosePath} - draining by default - waits for shutdown, and says which of
 * the three ways the instance ended: closed, stopped by a route, or failed. It answers the parked set per route
 * and instance-wide, read from the engine's own retry queue, which is where a parked record lives. The same
 * figures are published as meters under the {@code routes} subsystem, tagged by topic and outcome for the counters
 * and by topic and partition for the parked gauges; supply a registry with
 * {@code meterRegistry(...)} or nothing is published.
 *
 * <p><b>A parked record is not slow work.</b> The engine's shard scan measures how long each record it cannot yet
 * take has been waiting, and warns about the ones that have been waiting too long - but a parked record is work
 * the definition deliberately stopped, so the scan skips it and it appears in neither the warning nor the
 * slow-records meter. The figures that mean something for a parked record are the parked count and the parked set;
 * {@code ParkedRecordsAreNotSlowWorkTest} pins that.
 */
package bz.stub.parallelconsumer.fluent;
