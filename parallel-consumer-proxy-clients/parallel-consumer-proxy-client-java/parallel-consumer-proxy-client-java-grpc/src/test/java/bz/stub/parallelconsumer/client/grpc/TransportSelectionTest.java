package bz.stub.parallelconsumer.client.grpc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.ClientOptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The client can dial a loopback port or a Unix domain socket. Those are two transports, and being
 * given both is a caller mistake worth failing on rather than resolving by precedence - whichever
 * one lost would be ignored in silence, and the caller would be measuring or operating something
 * other than what they asked for.
 *
 * <p>These assertions need no epoll transport and so run on every platform, unlike the domain socket
 * itself.
 *
 * @author Antony Stubbs
 */
class TransportSelectionTest {

    private static ClientOptions options() {
        return ClientOptions.builder().topics(Collections.singletonList("t")).build();
    }

    @Test
    void askingForBothAPortAndASocketIsRefusedRatherThanResolved() {
        var builder = GrpcParallelConsumerClient.builder()
                .port(1234)
                .socketPath(Paths.get("/tmp/does-not-need-to-exist.sock"))
                .options(options());

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not both");
    }

    @Test
    void askingForNeitherIsRefusedAndSaysBothAreAcceptable() {
        var builder = GrpcParallelConsumerClient.builder().options(options());

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("socket path");
    }
}
