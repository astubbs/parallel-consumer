package bz.stub.parallelconsumer.client.grpc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.Outcome;
import bz.stub.parallelconsumer.client.OutboundRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.Record;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The wire boundary's decided conversions, asserted without a connection - above all KTD8's discipline: the
 * fencing token is echoed <b>byte-identically</b>, whatever it contains, because the client must never rebuild
 * or interpret it. The conformance suite proves the echo end to end (a wrong epoch would be discarded by the
 * engine and the offset would never commit); this pins the mechanism the end-to-end proof rests on.
 *
 * @author Antony Stubbs
 */
class WireMappingTest {

    private static final Token TOKEN = Token.newBuilder()
            .setRecordId("some-topic/3/42")
            .setEpoch(7)
            .build();

    @Test
    void aReportEchoesTheDispatchedTokenByteIdentically() {
        var successReport = WireMapping.toReport(TOKEN, Outcome.success());
        assertWithMessage("the token is echoed verbatim, byte for byte (KTD8)")
                .that(successReport.getToken().toByteArray()).isEqualTo(TOKEN.toByteArray());

        var failureReport = WireMapping.toReport(TOKEN, Outcome.failure("did not work"));
        assertThat(failureReport.getToken().toByteArray()).isEqualTo(TOKEN.toByteArray());
        assertThat(failureReport.getFailure().getReason()).isEqualTo("did not work");
    }

    @Test
    void aSuccessOutcomeCarriesItsProducePayloadAndABareSuccessCarriesNone() {
        var bare = WireMapping.toReport(TOKEN, Outcome.success());
        assertThat(bare.getSuccess().getProduceCount()).isEqualTo(0);

        var withPayload = WireMapping.toReport(TOKEN, Outcome.success(List.of(
                OutboundRecord.of("responses", null, "world".getBytes(StandardCharsets.UTF_8)))));
        assertThat(withPayload.getSuccess().getProduceCount()).isEqualTo(1);
        var produce = withPayload.getSuccess().getProduce(0);
        assertThat(produce.getTopic()).isEqualTo("responses");
        assertWithMessage("a null key stays absent on the wire - the tombstone distinction")
                .that(produce.hasKey()).isFalse();
        assertThat(produce.getValue().toStringUtf8()).isEqualTo("world");
    }

    @Test
    void absentWireFieldsBecomeTheApiOwnAbsences() {
        var dispatch = Dispatch.newBuilder()
                .setToken(TOKEN)
                .setRecord(Record.newBuilder()
                        .setTopic("some-topic")
                        .setPartition(3)
                        .setOffset(42)
                        .setValue(ByteString.copyFromUtf8("hello")))
                .build();

        var record = WireMapping.toInboundRecord(dispatch);

        assertThat(record.topic()).isEqualTo("some-topic");
        assertThat(record.partition()).isEqualTo(3);
        assertThat(record.offset()).isEqualTo(42);
        assertWithMessage("an absent key is null, never empty").that(record.key()).isNull();
        assertThat(new String(record.value(), StandardCharsets.UTF_8)).isEqualTo("hello");
        assertWithMessage("an unset attempt defaults to the first delivery").that(record.attempt()).isEqualTo(1);
        assertThat(record.lastFailureAt()).isEmpty();
        assertThat(record.lastFailureReason()).isEmpty();
    }
}
