package bz.stub.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import bz.stub.parallelconsumer.internal.utils.WireMockUtils;
import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.vertx.VertxParallelEoSStreamProcessor.RequestInfo;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Isolated;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bz.stub.parallelconsumer.internal.utils.LatchTestUtils.awaitLatch;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static pl.tlinkowski.unij.api.UniLists.of;

@Isolated
@Slf4j
@ExtendWith(VertxExtension.class)
class VertxTest extends VertxBaseUnitTest {

    WireMockServer stubServer;

    RequestInfo getGoodHost() {
        return new RequestInfo("localhost", stubServer.port(), "/", UniMaps.of());
    }

    RequestInfo getBadRequest() {
        // 127.0.0.1:1 gives an immediate, deterministic "connection refused" on every environment, with no
        // DNS involved. A bogus HOSTNAME instead relies on DNS *failing to resolve*, which is false on any
        // network with a local resolver + search domain (the name resolves and the failure mode changes),
        // and a dotless name additionally gets the search domain appended. Closed local port = no such coupling.
        int badPort = 1;
        String badHostname = "127.0.0.1";
        return new RequestInfo(badHostname, badPort, "/", UniMaps.of());
    }

    @BeforeEach
    void setupWireMock() {
        WireMockUtils wireMockUtils = new WireMockUtils();
        stubServer = wireMockUtils.setupWireMock();
    }

    @AfterEach
    void closeWireMock() {
        stubServer.stop();
    }

    @SneakyThrows
    @Test
    void sanityTest(Vertx vertx, VertxTestContext tc) {
        WebClient client = WebClient.create(vertx);
        HttpRequest<Buffer> bufferHttpRequest = client.get(getGoodHost().getPort(), getGoodHost().getHost(), "");
        bufferHttpRequest.send(tc.succeeding(response -> tc.verify(() -> {
            log.debug("callback {}", response.bodyAsString());
            tc.completeNow();
        })));
    }

    @Test
    void failingHttpCall() {
        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var tupleStream =
                vertxAsync.vertxHttpReqInfoStream((PollContext<String, String> rec) -> getBadRequest());

        //
        awaitLatch(latch);

        //
        assertCommits(of());

        // check results are failures
        var res = getResults(tupleStream);
        assertThat(res).doesNotContainNull();
        assertThat(res).extracting(AsyncResult::failed).containsOnly(true);
        // Deterministic across environments: a closed local port yields "Connection refused" (no DNS lookup).
        assertThat(res)
                .extracting(x -> x.cause().getMessage().toLowerCase())
                .allSatisfy(msg -> assertThat(msg).contains("connection refused"));
    }

    // todo how is this different from #failingHttpCall ?
    @SneakyThrows
    @Test
    void testVertxFunctionFail(Vertx vertx, VertxTestContext tc) {
        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var futureStream =
                vertxAsync.vertxHttpReqInfoStream((rec) -> {
                    log.debug("Inner user function");
                    return getBadRequest();
                });

        // wait
        awaitLatch(latch);

        // verify
        var collect = futureStream.map(JStreamVertxParallelEoSStreamProcessor.VertxCPResult::getAsr).collect(Collectors.toList());
        assertThat(collect).hasSize(1);
        Future<HttpResponse<Buffer>> actual = collect.get(0).onComplete(x -> {
        });
        await().until(actual::isComplete);
        assertThat(actual).isNotNull();

        actual.onComplete(tc.failing(ar -> tc.verify(() -> {
            // Deterministic across environments: closed local port (getBadRequest) -> "Connection refused"
            // (was the old DNS "Failed to resolve" message; both this and failingHttpCall share getBadRequest).
            assertThat(ar).hasMessageContaining("Connection refused");
            tc.completeNow();
        })));

        Assertions.assertThat(vertxAsync.workRemaining()).isEqualTo(1); // two failed requests still in queue for retry
    }

    @Test
    void testHttpMinimal() {
        vertxAsync.setTimeBetweenCommits(ofSeconds(1));

        var futureStream =
                vertxAsync.vertxHttpReqInfoStream((rec) -> {
                    log.debug("Inner user function");
                    RequestInfo goodHost = getGoodHost();
                    var params = UniMaps.of("randomParam", rec.value());
                    goodHost.setParams(params);

                    return goodHost;
                });

        //
        awaitForCommitExact(1);

        // verify
        // test results are successes
        var res = getResults(futureStream);
        assertThat(res).extracting(x -> x.result().statusCode()).containsOnly(200);
        assertThat(res).extracting(x -> x.result().bodyAsString()).contains(WireMockUtils.stubResponse);
    }

    @SneakyThrows
    @Test
    void testHttp() {
        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var futureStream =
                vertxAsync.vertxHttpRequestStream((webClient, rec) -> {
                    log.debug("Inner user function");
                    var data = rec.value();
                    RequestInfo reqInfo = getGoodHost();
                    var httpRequest = webClient.get(reqInfo.getPort(), reqInfo.getHost(), reqInfo.getContextPath());
                    httpRequest = httpRequest.addQueryParam("randomParam", data);

                    return httpRequest;
                });

        awaitLatch(latch);

        var res = getResults(futureStream);

        // test results are successes
        assertThat(res).hasSize(1).doesNotContainNull();
        assertThat(res).extracting(AsyncResult::cause).containsOnlyNulls();
        assertThat(res).extracting(x -> x.result().statusCode()).containsOnly(200);
        assertThat(res).extracting(x -> x.result().bodyAsString()).contains(WireMockUtils.stubResponse);
    }

    /**
     * Characterizes what a non-2xx status does, which nothing asserted before. The stub
     * {@code VertxTest.handleHttpResponseCodes} was named for this and was deleted in
     * {@code cadf4c95} - correctly, its body was {@code assertThat(true).isFalse()} behind
     * {@code @Disabled} and it had never run green. The gap it named was real: audit §1.3 records
     * that non-2xx handling is untested and that the nearest test asserts 200 on the happy path
     * only.
     * <p>
     * A Vert.x {@code WebClient} future completes successfully for <em>any</em> HTTP response and
     * fails only on transport errors, and
     * {@code VertxParallelEoSStreamProcessor}'s {@code send.onSuccess} calls
     * {@code onUserFunctionSuccess}. So a 500 is a delivered response, not a processing failure,
     * and the offset commits.
     * <p>
     * That is a contract, not a bug: parallel-consumer takes no position on status codes, the same
     * way it takes no position on how a value was serialised. A user who wants a 5xx retried says
     * so - with a response predicate, or by throwing from their own function. This test exists so
     * that contract fails loudly if it ever changes silently.
     */
    @SneakyThrows
    @Test
    void serverErrorStatusStillCommits() {
        stubServer.stubFor(WireMock.get(WireMock.urlPathEqualTo("/server-error"))
                .willReturn(WireMock.aResponse().withStatus(500).withBody("boom")));

        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var futureStream = vertxAsync.vertxHttpRequestStream((webClient, rec) -> {
            RequestInfo reqInfo = getGoodHost();
            return webClient.get(reqInfo.getPort(), reqInfo.getHost(), "/server-error");
        });

        awaitLatch(latch);

        var res = getResults(futureStream);
        assertThat(res).hasSize(1).doesNotContainNull();
        // the future succeeded - the response was delivered, so nothing failed as far as vertx is concerned
        assertThat(res).extracting(AsyncResult::cause).containsOnlyNulls();
        assertThat(res).extracting(x -> x.result().statusCode()).containsOnly(500);

        // and the work is treated as done: the offset commits
        awaitForCommitExact(1);
    }

    /**
     * The other side of the boundary in {@link #serverErrorStatusStillCommits}, and the reason that
     * contract is defensible: a transport failure - nothing delivered - does fail the future. The
     * distinction is what a user needs in order to decide where to put their own error handling.
     */
    @SneakyThrows
    @Test
    void transportFailureIsDistinctFromANonSuccessStatus() {
        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var futureStream = vertxAsync.vertxHttpRequestStream((webClient, rec) -> {
            RequestInfo unreachable = getBadRequest();
            return webClient.get(unreachable.getPort(), unreachable.getHost(), unreachable.getContextPath());
        });

        awaitLatch(latch);

        var res = getResults(futureStream);
        assertThat(res).hasSize(1).doesNotContainNull();
        assertThat(res).extracting(AsyncResult::cause).doesNotContainNull();

        // the half that makes this a contrast rather than a second success case: nothing commits, so
        // the record is retried. Asserting only that the future failed would leave the boundary
        // half-stated - and the boundary is the whole point of the pair.
        assertCommits(of());
    }

    private List<AsyncResult<HttpResponse<Buffer>>> getResults(
            Stream<JStreamVertxParallelEoSStreamProcessor.VertxCPResult<String, String>> futureStream) {
        var collect = futureStream.map(JStreamVertxParallelEoSStreamProcessor.VertxCPResult::getAsr).collect(Collectors.toList());
        return blockingGetResults(collect);
    }

    @SneakyThrows
    private <T> List<AsyncResult<T>> blockingGetResults(List<Future<T>> collect) {
        List<AsyncResult<T>> list = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(collect.size());
        for (Future<T> httpResponseFuture : collect) {
            httpResponseFuture.onComplete(x -> {
                list.add(x);
                countDownLatch.countDown();
            });
        }
        boolean success = countDownLatch.await(defaultTimeoutSeconds, SECONDS);
        if (!success)
            throw new AssertionError("Timeout reached");
        return list;
    }

    @SneakyThrows
    @Test
    void genericVertxFuture(Vertx vertx, VertxTestContext tc) {
        primeFirstRecord();
        primeFirstRecord();

        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var latchTwo = new CountDownLatch(1);
        // signals that a user function is genuinely running and parked on latchTwo - the state we want to
        // release from
        var innerFunctionStarted = new CountDownLatch(1);

        Checkpoint cp = tc.checkpoint(3);

        vertxAsync.vertxFuture(rec -> vertx.executeBlocking(event -> {
            log.debug("Inner user function {}", rec);
            var data = rec.value();

            try {
                log.info("Waiting");
                innerFunctionStarted.countDown();
                latchTwo.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            cp.flag();
            log.info("Finished waiting");

            event.complete();
        }));

        // wait for work to actually be in flight, instead of sleeping and hoping it is
        awaitLatch(innerFunctionStarted);
        latchTwo.countDown();
        log.info("Counted down");

        awaitLatch(latch);
        log.info("Latch gotten");
    }

}
