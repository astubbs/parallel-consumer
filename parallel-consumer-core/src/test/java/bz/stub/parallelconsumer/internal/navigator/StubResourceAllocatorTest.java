package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The registry half of {@link StubResourceAllocator} (R1, R4, R19). The minting, membership and conservation
 * mechanics live in {@link StubResourceAllocatorMintingTest} on the virtual clock; here only registration-time
 * fail-fast and the empty-before-any-pull lease read are exercised.
 */
class StubResourceAllocatorTest {

    private static final ResourceContract API_X = new ResourceContract("api-x", 2.0, 2, Duration.ofSeconds(1));

    private final StubResourceAllocator allocator = new StubResourceAllocator();

    @Test
    void lookupOfAnUnregisteredNameIsEmpty() {
        assertThat(allocator.lookup("api-y").isPresent()).isFalse();
    }

    @Test
    void registeringThenLookingUpReturnsTheSameContract() {
        allocator.register(API_X);

        assertThat(allocator.lookup("api-x")).hasValue(API_X);
    }

    /**
     * Covers AE3, R19. Several instances' constructions may each register the resources they share - repeat
     * registration under the IDENTICAL policy must be a no-op, never an error.
     */
    @Test
    void reRegisteringTheIdenticalPolicyIsAccepted() {
        allocator.register(API_X);

        allocator.register(new ResourceContract("api-x", 2.0, 2, Duration.ofSeconds(1)));

        assertThat(allocator.lookup("api-x")).hasValue(API_X);
    }

    /**
     * Covers AE3, R19. A second registration of an already-registered name under a DIFFERENT policy must fail,
     * naming the collision - never a silent overwrite.
     */
    @Test
    void reRegisteringADifferentPolicyFailsNamingTheCollision() {
        allocator.register(API_X);

        ResourceContract conflicting = new ResourceContract("api-x", 5.0, 2, Duration.ofSeconds(1));

        assertThatThrownBy(() -> allocator.register(conflicting))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("api-x");
    }

    @Test
    void currentLeaseOfAMemberThatNeverPulledAQuantumIsEmpty() {
        allocator.register(API_X);

        assertThat(allocator.currentLease("member-1", "api-x", Instant.now()).isPresent()).isFalse();
    }

    /**
     * Covers R19's fail-fast posture at the policy itself: a quantum the minting arithmetic cannot divide time
     * by must be rejected at registration, naming the field - never a runtime failure deep in the engine.
     */
    @Test
    void registeringANonPositiveQuantumFailsFastNamingTheField() {
        assertThatThrownBy(() -> allocator.register(new ResourceContract("api-z", 2.0, 2, Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("quantum");
    }
}
