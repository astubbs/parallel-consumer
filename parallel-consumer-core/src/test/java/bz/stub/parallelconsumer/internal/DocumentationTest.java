package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins where {@link Documentation} sends a user.
 * <p>
 * {@code DOCS_ROOT} is the base of the link that runtime error messages carry - {@code PollContext} embeds it in
 * the {@code IllegalArgumentException} thrown when a batch size is configured but the non-batch polling methods
 * are called. It pointed at {@code confluentinc/parallel-consumer} after the fork, so the one link a confused
 * user is handed led to a project that states it is no longer maintained. Nothing asserted on the value, so
 * nothing noticed.
 * <p>
 * This is the assertion that would have caught it, and that stops a mechanical namespace sweep quietly
 * restoring it.
 */
class DocumentationTest {

    /**
     * The fork is where the maintained documentation lives, so that is where a runtime error must point.
     */
    @Test
    void docsRootAddressesThisFork() {
        assertWithMessage("runtime error messages must send users to the maintained fork, not to upstream")
                .that(Documentation.DOCS_ROOT)
                .isEqualTo("https://github.com/astubbs/parallel-consumer/");
    }

    /**
     * The only production call site passes an anchor, so the composed form is what a user actually clicks. The
     * root must therefore end in a separator - without it the anchor would fuse onto the repository name.
     */
    @Test
    void sectionLinksComposeIntoAResolvableUrl() {
        assertWithMessage("the batching anchor is the link PollContext hands to a user - see README.adoc [[batching]]")
                .that(Documentation.getLinkHtmlToDocSection("#batching"))
                .isEqualTo("https://github.com/astubbs/parallel-consumer/#batching");
    }

}
