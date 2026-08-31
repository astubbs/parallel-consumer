package bz.stub.parallelconsumer.proxy.protocol;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the frozen wire bytes. The committed golden resources are the canonical serialized session every
 * language's generated parser must accept ({@link GoldenSessionFixture} explains why a same-runtime round trip
 * cannot stand in for this), and this test holds the Java side to them in both directions: the resources parse
 * to exactly the fixture's values, and the fixture serializes to exactly the resources' bytes. Either
 * direction going red after a schema edit means the edit moved the wire - which, post-freeze, is a break
 * unless it is a specified ADDITION regenerating the fixture and resources together.
 */
class GoldenSessionBytesTest {

    @Test
    void goldenClientBytesParseToTheFixtureSession() throws IOException {
        var parsed = new ArrayList<ClientMessage>();
        try (InputStream in = resource(GoldenSessionFixture.CLIENT_RESOURCE)) {
            ClientMessage message;
            while ((message = ClientMessage.parseDelimitedFrom(in)) != null) {
                parsed.add(message);
            }
        }
        assertWithMessage("the golden client stream must parse to exactly the scripted session, value for value")
                .that(parsed).isEqualTo(GoldenSessionFixture.clientMessages());
    }

    @Test
    void goldenProxyBytesParseToTheFixtureSession() throws IOException {
        var parsed = new ArrayList<ProxyMessage>();
        try (InputStream in = resource(GoldenSessionFixture.PROXY_RESOURCE)) {
            ProxyMessage message;
            while ((message = ProxyMessage.parseDelimitedFrom(in)) != null) {
                parsed.add(message);
            }
        }
        assertWithMessage("the golden proxy stream must parse to exactly the scripted session, value for value")
                .that(parsed).isEqualTo(GoldenSessionFixture.proxyMessages());
    }

    /**
     * The byte pin itself: serializing the fixture reproduces the committed resources exactly. This is the
     * direction that catches a schema change the parse direction would forgive - protobuf parsers tolerate
     * reordered or re-encoded input, so only byte equality proves the wire form is unchanged.
     */
    @Test
    void fixtureSerializesByteIdenticallyToTheCommittedResources() throws IOException {
        assertWithMessage("client-stream bytes moved - post-freeze that is a wire break unless this is a "
                + "specified addition regenerating fixture and resource together")
                .that(GoldenSessionFixture.delimitedClientBytes())
                .isEqualTo(resourceBytes(GoldenSessionFixture.CLIENT_RESOURCE));
        assertWithMessage("proxy-stream bytes moved - post-freeze that is a wire break unless this is a "
                + "specified addition regenerating fixture and resource together")
                .that(GoldenSessionFixture.delimitedProxyBytes())
                .isEqualTo(resourceBytes(GoldenSessionFixture.PROXY_RESOURCE));
    }

    /** Every message type in each envelope's oneof appears in the golden session - the corpus stays complete. */
    @Test
    void goldenSessionCoversEveryMessageType() {
        var clientCases = GoldenSessionFixture.clientMessages().stream()
                .map(ClientMessage::getMessageCase)
                .distinct()
                .toList();
        assertThat(clientCases).containsAtLeast(
                ClientMessage.MessageCase.CONFIGURE,
                ClientMessage.MessageCase.REPORT,
                ClientMessage.MessageCase.HEARTBEAT,
                ClientMessage.MessageCase.MANIFEST,
                ClientMessage.MessageCase.WORKER_DIED);

        var proxyCases = GoldenSessionFixture.proxyMessages().stream()
                .map(ProxyMessage::getMessageCase)
                .distinct()
                .toList();
        assertThat(proxyCases).containsAtLeast(
                ProxyMessage.MessageCase.CONFIGURED,
                ProxyMessage.MessageCase.DISPATCH,
                ProxyMessage.MessageCase.DROP,
                ProxyMessage.MessageCase.SHUTDOWN,
                ProxyMessage.MessageCase.SET_EXECUTOR_COUNT);
    }

    private static InputStream resource(String name) {
        var in = GoldenSessionBytesTest.class.getResourceAsStream(name);
        assertWithMessage("golden resource %s must be committed beside the test", name).that(in).isNotNull();
        return in;
    }

    private static byte[] resourceBytes(String name) throws IOException {
        try (InputStream in = resource(name)) {
            return in.readAllBytes();
        }
    }
}
