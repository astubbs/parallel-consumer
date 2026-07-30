package io.confluent.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

/**
 * DEMO ONLY - deliberately violates the quarantine rules (annotated but NO registry entry, owner PR
 * does not exist) to watch the Quarantine Audit job fail fast on CI. This commit gets reverted.
 */
class QuarantineDemoViolationTest {

    @Quarantined(reason = "demo: deliberately unregistered quarantine to prove the audit fails fast",
            tracking = "none - this is the violation",
            fixedBy = "PR #99999")
    @Test
    void demoViolation() {
    }
}
