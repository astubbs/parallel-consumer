package io.confluent.parallelconsumer.streams;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.List;

/**
 * The one shape every refusal message takes.
 * <p>
 * There are two refusal sites - {@link PcUnsupportedConstruct#refuse()} at the DSL call, and
 * {@link PcSupportedEnvelope} at task construction - and a user who hits both should not have to work out
 * that they are the same rule. Both come through here, so both name the construct, say why it is refused,
 * and end with the exact property that turns the seam off.
 * <p>
 * Whoever hits one of these messages is, by definition, surprised: they wrote ordinary Kafka Streams and it
 * refused. So the message has to carry the whole answer - not a code, not a doc link on its own.
 *
 * @author Antony Stubbs
 */
final class PcRefusalMessage {

    /**
     * Where a reader goes for the full list and its rationale. The plan is the living document; the README
     * points at it rather than duplicating it.
     */
    private static final String REFERENCE = "See parallel-consumer-streams/README.md and astubbs#255.";

    private PcRefusalMessage() {
    }

    /**
     * @param constructs every unsupported construct found, never just the first. Someone who removes their
     *                   windowed aggregation only to be refused again for a join has been made to pay twice for
     *                   one diagnosis.
     */
    static String forConstructs(final List<PcUnsupportedConstruct> constructs) {
        if (constructs.isEmpty()) {
            throw new IllegalArgumentException("Refusing nothing is not a refusal - this is a bug in "
                    + PcRefusalMessage.class.getName());
        }

        final StringBuilder message = new StringBuilder("PC dispatch (astubbs#255): ");
        if (constructs.size() == 1) {
            message.append(constructs.get(0).getDisplayName())
                    .append(" is not supported on the Parallel Consumer dispatch path, because ")
                    .append(constructs.get(0).getReason())
                    .append('.');
        } else {
            message.append(constructs.size())
                    .append(" constructs in this topology are not supported on the Parallel Consumer dispatch path:");
            for (final PcUnsupportedConstruct construct : constructs) {
                message.append("\n  - ")
                        .append(construct.getDisplayName())
                        .append(" - ")
                        .append(construct.getReason())
                        .append('.');
            }
        }

        return message.append("\n\nThis is refused rather than allowed to produce silently wrong results. "
                        + "Run with -D")
                .append(PcDispatchSwitch.ENABLED_PROPERTY)
                .append("=false for stock Kafka Streams dispatch, which supports all of the above. ")
                .append(REFERENCE)
                .toString();
    }
}
