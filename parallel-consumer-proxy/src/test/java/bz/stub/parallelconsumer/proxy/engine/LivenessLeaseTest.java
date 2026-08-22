package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The lease's own rules, on a clock the test moves: what a dispatch is leased for, what a heartbeat extends,
 * what suspension does, and what a session without the {@code heartbeat} capability gets - which is no clock
 * at all.
 *
 * @author Antony Stubbs
 */
class LivenessLeaseTest {

    private static final Duration LEASE = Duration.ofSeconds(60);

    private final EngineFixture.TestClock clock = new EngineFixture.TestClock();

    private LivenessLease enabledLease() {
        return new LivenessLease(new LivenessSettings(true, LEASE, Duration.ofSeconds(20),
                Duration.ofSeconds(30), clock));
    }

    @Test
    void aDeliveryIsLeasedFromItsDispatch() {
        var lease = enabledLease();

        var deadline = lease.deadlineAtDispatch();

        assertThat(deadline).isEqualTo(clock.instant().plus(LEASE));
        assertThat(lease.hasExpired(deadline)).isFalse();
    }

    @Test
    void aLeaseWithNoHeartbeatExpires() {
        var lease = enabledLease();
        var deadline = lease.deadlineAtDispatch();

        clock.advance(LEASE.plusSeconds(1));

        assertThat(lease.hasExpired(deadline)).isTrue();
    }

    /**
     * AE21's first half, at the unit: the record's own dispatch deadline is long past, but the session keeps
     * heartbeating, so nothing expires. This is what "the lease is not a processing deadline" means - a
     * function running for hours over many lease periods keeps its record.
     */
    @Test
    void aRecordOutFarLongerThanTheLeaseSurvivesWhileHeartbeatsContinue() {
        var lease = enabledLease();
        var deadline = lease.deadlineAtDispatch();

        for (int leasePeriod = 0; leasePeriod < 10; leasePeriod++) {
            clock.advance(LEASE.dividedBy(2));
            lease.heartbeat();
            assertWithMessage("the record must survive lease period %s while its session heartbeats",
                    leasePeriod).that(lease.hasExpired(deadline)).isFalse();
        }

        // and the moment the heartbeats stop, it goes
        clock.advance(LEASE.plusSeconds(1));
        assertThat(lease.hasExpired(deadline)).isTrue();
    }

    /** R46's suspension rule: while the connection is down, the window governs and no lease may expire. */
    @Test
    void aSuspendedLeaseNeverExpires() {
        var lease = enabledLease();
        var deadline = lease.deadlineAtDispatch();

        lease.suspend();
        clock.advance(LEASE.multipliedBy(100));

        assertThat(lease.isSuspended()).isTrue();
        assertThat(lease.hasExpired(deadline)).isFalse();
    }

    /** Resuming is itself a heartbeat: otherwise every kept record would expire the instant the window lifts. */
    @Test
    void resumingAfterASuspensionRelicensesTheRecordsItKept() {
        var lease = enabledLease();
        var deadline = lease.deadlineAtDispatch();
        lease.suspend();
        clock.advance(LEASE.multipliedBy(3));

        lease.resume();

        assertThat(lease.isSuspended()).isFalse();
        assertWithMessage("a record kept by the manifest must not expire on the reconnect handshake itself")
                .that(lease.hasExpired(deadline)).isFalse();
    }

    /** A client that did not negotiate {@code heartbeat} sends none, so no lease clock may run against it. */
    @Test
    void aSessionWithoutTheCapabilityHasNoLeaseAtAll() {
        var lease = new LivenessLease(new LivenessSettings(false, LEASE, Duration.ofSeconds(20),
                Duration.ofSeconds(30), clock));

        var deadline = lease.deadlineAtDispatch();
        clock.advance(Duration.ofDays(365));

        assertThat(lease.enabled()).isFalse();
        assertThat(deadline).isEqualTo(Instant.MAX);
        assertThat(lease.hasExpired(deadline)).isFalse();
    }
}
