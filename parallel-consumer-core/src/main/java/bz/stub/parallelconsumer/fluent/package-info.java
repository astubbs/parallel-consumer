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
 */
package bz.stub.parallelconsumer.fluent;
