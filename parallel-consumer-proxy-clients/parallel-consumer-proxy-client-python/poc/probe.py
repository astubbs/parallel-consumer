# Copyright (C) 2026 Antony Stubbs and contributors

"""U26 specification probe. Ugly on purpose. Drives the test-mode sidecar
(--mock) from the documents alone; every guess is logged as QUESTION."""
import queue
import subprocess
import sys
import threading
import time

import grpc

sys.path.insert(0, '.')
from parallelconsumer.proxy.v1 import proxy_pb2 as pb
from parallelconsumer.proxy.v1 import proxy_pb2_grpc as pbg

JAVA = "/home/astubbs/.local/share/mise/installs/java/temurin-17/bin/java"
CP = open('full-cp.txt').read().strip()
MAIN = "bz.stub.parallelconsumer.proxy.testmode.TestModeMain"

QUESTIONS = []
def q(text):
    QUESTIONS.append(text)
    print(f"QUESTION: {text}")

def log(*a):
    print(f"[{time.monotonic():8.3f}]", *a, flush=True)


class Sidecar:
    def __init__(self, scenario):
        # spec: child process, launched directly not through a shell; parent
        # holds stdin write end and never writes
        self.proc = subprocess.Popen(
            [JAVA, "-cp", CP, MAIN, "--mock", "--scenario", scenario],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL)
        # SPEC SAYS the port line is the FIRST stdout line and nothing else is
        # written before it. The test-mode harness violates that: logback
        # status lines precede it. FINDING (harness defect, not doc): scan.
        seen = []
        while True:
            line = self.proc.stdout.readline().decode()
            if not line:
                raise AssertionError(f"stdout EOF before port line; saw {seen}")
            if line.startswith("port: "):
                break
            seen.append(line)
        if seen:
            q(f"harness wrote {len(seen)} stdout line(s) before 'port: <n>' "
              "(spec: nothing is ever written before the port line)")
        self.port = int(line.split()[1])
        log(f"sidecar up, port {self.port}")

    def kill_stdin(self):
        self.proc.stdin.close()  # parent-death signal

    def wait(self, t=15):
        try:
            return self.proc.wait(timeout=t)
        except subprocess.TimeoutExpired:
            q(f"sidecar did not exit within {t}s after the drain the spec "
              "says ends with the proxy exiting; killed it")
            self.proc.kill()
            return 'TIMEOUT-KILLED'


class Stream:
    """One Session stream. Sends via queue; receives on a thread."""
    def __init__(self, channel):
        self.sendq = queue.Queue()
        self.recvq = queue.Queue()
        self.stub = pbg.ProxyServiceStub(channel)
        self.call = self.stub.Session(iter(self.sendq.get, None))
        self.done = threading.Event()
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        try:
            for msg in self.call:
                self.recvq.put(msg)
            self.recvq.put(('CLOSED', grpc.StatusCode.OK, ''))
        except grpc.RpcError as e:
            self.recvq.put(('CLOSED', e.code(), e.details()))
        finally:
            self.done.set()

    def send(self, **kw):
        self.sendq.put(pb.ClientMessage(**kw))

    def half_close(self):
        self.sendq.put(None)

    def recv(self, timeout=20):
        return self.recvq.get(timeout=timeout)


def expect(msg, kind):
    assert not isinstance(msg, tuple), f"stream closed: {msg}"
    got = msg.WhichOneof('message')
    assert got == kind, f"expected {kind}, got {got}: {msg}"
    return getattr(msg, kind)


def minimal_configure(topic, **extra):
    # QUESTION: is an empty kafka_properties map acceptable in mock mode?
    # The spec never says what Configure the conformance harness expects.
    return pb.Configure(topics=[topic], **extra)


def scenario_failure_redelivery():
    log("=== probe 1: failure -> redelivery with attempt/history; heartbeats; client-initiated shutdown")
    topic = "a-failed-record-is-redelivered-with-its-failure-history"
    sc = Sidecar(topic)
    ch = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
    st = Stream(ch)
    st.send(configure=minimal_configure(topic, max_concurrency=4))
    cfd = expect(st.recv(), 'configured')
    log("Configured fields SET:", sorted(f.name for f, _ in cfd.ListFields()))
    all_fields = [f.name for f in cfd.DESCRIPTOR.fields]
    log("Configured fields ABSENT:",
        sorted(set(all_fields) - {f.name for f, _ in cfd.ListFields()}))
    log("Configured: executor_count", cfd.executor_count,
        "max_concurrency", cfd.max_concurrency,
        "capabilities", list(cfd.capabilities),
        "topics", list(cfd.topics))
    if not cfd.HasField('heartbeat_interval'):
        q("Configured.heartbeat_interval is ABSENT though the spec says every "
          "value the proxy computes is always set and the client must "
          "heartbeat every heartbeat_interval - the client cannot know the "
          "interval")
    if not cfd.HasField('lease_duration'):
        q("Configured.lease_duration is ABSENT")
    if list(cfd.capabilities) == ['dispatch']:
        q("empty client capability declaration (= v1 baseline) negotiated to "
          "['dispatch'] only - the proxy's own capability set is undocumented, "
          "and the duties sections say heartbeat/manifest are unconditional "
          "while the negotiation rule forbids sending outside the set")
    hb_secs = cfd.heartbeat_interval.seconds + cfd.heartbeat_interval.nanos / 1e9
    if hb_secs == 0:
        hb_secs = 2.0  # guessed: nothing in the documents says what to do
    log("heartbeat interval secs:", hb_secs)

    # heartbeat immediately and on interval from a thread
    stop_hb = threading.Event()
    def hb():
        while not stop_hb.is_set():
            st.send(heartbeat=pb.Heartbeat())
            stop_hb.wait(max(hb_secs, 0.5))
    threading.Thread(target=hb, daemon=True).start()

    d = expect(st.recv(), 'dispatch')
    log("wave 1:", len(d.records), "records")
    r = d.records[0]
    log("r: attempt", r.attempt, "token", r.token.record_id, r.token.epoch,
        "last_failure_at?", r.HasField('last_failure_at'))
    assert r.attempt == 1 and not r.HasField('last_failure_at')
    st.send(report=pb.Report(token=r.token,
                             failure=pb.Report.Failure(reason="probe boom")))
    d2 = expect(st.recv(), 'dispatch')
    r2 = d2.records[0]
    log("redelivery: attempt", r2.attempt, "epoch", r2.token.epoch,
        "last_failure_at", r2.last_failure_at.seconds,
        "reason", repr(r2.last_failure_reason))
    assert r2.attempt == 2, r2
    assert r2.last_failure_reason == "probe boom"
    assert r2.HasField('last_failure_at')
    assert r2.token.record_id == r.token.record_id
    log("epoch changed across redelivery:", r.token.epoch, "->", r2.token.epoch)
    st.send(report=pb.Report(token=r2.token, success=pb.Report.Success()))
    time.sleep(1.0)
    # client-initiated shutdown: everything reported; half-close is the signal
    stop_hb.set()
    st.half_close()
    end = st.recv()
    log("stream end:", end)
    code = sc.wait()
    log("sidecar exit code:", code)
    return cfd


def scenario_out_of_order_and_terminal():
    log("=== probe 2: multi-record wave, out-of-order success, terminal report")
    topic = "records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently"
    sc = Sidecar(topic)
    ch = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
    st = Stream(ch)
    st.send(configure=minimal_configure(
        topic, max_concurrency=8,
        terminal_topic="probe.terminal"))
    cfd = expect(st.recv(), 'configured')
    log("Configured terminal_topic present:", cfd.HasField('terminal_topic'),
        "max_concurrency", cfd.max_concurrency,
        "capabilities", list(cfd.capabilities))
    if not cfd.HasField('terminal_topic'):
        q("Configure named terminal_topic='probe.terminal' but Configured "
          "omits it (spec: 'set exactly when configured'); negotiated "
          f"capabilities were {list(cfd.capabilities)} - presumably gated "
          "by the un-negotiated 'terminal' token, which no document states")

    got = []
    waves = 0
    deadline = time.time() + 20
    while len(got) < 2 and time.time() < deadline:
        m = st.recv()
        d = expect(m, 'dispatch')
        waves += 1
        log(f"wave {waves}: {len(d.records)} records:",
            [(x.record.key, x.token.record_id, x.token.epoch) for x in d.records])
        got.extend(d.records)
    # report in REVERSE order (out-of-order completion), with a produce payload
    first = True
    for rec in reversed(got):
        if first:
            st.send(report=pb.Report(
                token=rec.token,
                success=pb.Report.Success(produce=[pb.ProduceRecord(
                    topic="probe.out", key=rec.record.key, value=b"probe-out")])))
            first = False
        else:
            st.send(report=pb.Report(token=rec.token, success=pb.Report.Success()))
        st.send(heartbeat=pb.Heartbeat())
    # drain further waves; terminal-report the first record of the next wave
    # and watch what a not-negotiated / not-configured Terminal does
    terminal_token = None
    while True:
        try:
            m = st.recv(timeout=5)
        except queue.Empty:
            log("no more dispatches after", waves, "waves")
            break
        if isinstance(m, tuple):
            log("stream closed early:", m)
            break
        d = expect(m, 'dispatch')
        waves += 1
        log(f"wave {waves}: {len(d.records)} records:",
            [(x.record.key, x.token.record_id, x.token.epoch, x.attempt) for x in d.records])
        for rec in d.records:
            if terminal_token is None:
                st.send(report=pb.Report(
                    token=rec.token,
                    terminal=pb.Report.Terminal(reason="probe poison pill")))
                terminal_token = rec.token
                log("sent Terminal for", rec.token.record_id, "epoch", rec.token.epoch)
            else:
                st.send(report=pb.Report(token=rec.token, success=pb.Report.Success()))
    if terminal_token is not None:
        # spec: a violating Terminal is a non-fatal discard - the record stays
        # in flight. A follow-up Success at the same epoch should then resolve it.
        log("following the discarded Terminal with a Success at the same epoch")
        st.send(report=pb.Report(token=terminal_token, success=pb.Report.Success()))
        try:
            m = st.recv(timeout=4)
            log("after follow-up success:", m if isinstance(m, tuple) else m.WhichOneof('message'))
        except queue.Empty:
            log("silence after follow-up success (expected: nothing to say)")
    else:
        q("could not exercise Terminal on a live record in this scenario "
          "(all records resolved before a later wave arrived)")
    st.half_close()
    log("stream end:", st.recv())
    log("sidecar exit:", sc.wait())


def scenario_proxy_shutdown():
    log("=== probe 3: proxy-initiated Shutdown on parent death (stdin EOF)")
    topic = "an-unreported-record-holds-back-the-commit"
    sc = Sidecar(topic)
    ch = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
    st = Stream(ch)
    st.send(configure=minimal_configure(topic))
    cfd = expect(st.recv(), 'configured')
    d = expect(st.recv(), 'dispatch')
    rec = d.records[0]
    log("holding record", rec.token.record_id, "unreported; closing sidecar stdin")
    sc.kill_stdin()
    # spec: proxy-initiated shutdown -> proxy sends Shutdown, stops dispatching, drains
    m = st.recv(timeout=15)
    if isinstance(m, tuple):
        log("STREAM CLOSED without Shutdown:", m)
        q("on parent-death (stdin EOF) the spec says the proxy sends Shutdown "
          "and drains, but the stream ended with no Shutdown message")
    else:
        sh = expect(m, 'shutdown')
        log("Shutdown received; releasing the held (queued) record")
        st.send(report=pb.Report(token=rec.token, released=pb.Report.Released()))
        st.half_close()
        log("stream end:", st.recv())
    log("sidecar exit:", sc.wait())


def scenario_reconnect_manifest():
    log("=== probe 4: connection loss + reconnect with Manifest")
    topic = "records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently"
    sc = Sidecar(topic)
    ch = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
    st = Stream(ch)
    st.send(configure=minimal_configure(topic, max_concurrency=8))
    cfd1 = expect(st.recv(), 'configured')
    d = expect(st.recv(), 'dispatch')
    held = list(d.records)
    log("holding", [(r.token.record_id, r.token.epoch) for r in held])
    # simulate connection loss: cancel the call abruptly
    st.call.cancel()
    ch.close()
    time.sleep(0.5)
    ch2 = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
    st2 = Stream(ch2)
    st2.send(manifest=pb.Manifest(tokens=[r.token for r in held]))
    m = st2.recv(timeout=15)
    if isinstance(m, tuple):
        log("RECONNECT REFUSED:", m)
        q(f"reconnect with Manifest was refused: {m}")
        log("sidecar exit:", sc.wait())
        return
    cfd2 = expect(m, 'configured')
    same = all(getattr(cfd2, f.name) == getattr(cfd1, f.name)
               for f, _ in cfd1.ListFields())
    log("reconnect Configured identical to fresh one:", same)
    # report the held records now
    for r in held:
        st2.send(report=pb.Report(token=r.token, success=pb.Report.Success()))
    # drain remaining waves
    while True:
        try:
            m = st2.recv(timeout=5)
        except queue.Empty:
            break
        if isinstance(m, tuple):
            log("closed:", m)
            break
        kind = m.WhichOneof('message')
        if kind == 'dispatch':
            for rec in m.dispatch.records:
                st2.send(report=pb.Report(token=rec.token, success=pb.Report.Success()))
        else:
            log("got", kind, m)
    st2.half_close()
    log("stream end:", st2.recv())
    log("sidecar exit:", sc.wait())


def scenario_violations():
    log("=== probe 5: handshake refusals")
    topic = "a-processed-record-advances-the-committed-offset"
    sc = Sidecar(topic)
    ch = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
    # first message is a Heartbeat: expect FAILED_PRECONDITION
    st = Stream(ch)
    st.send(heartbeat=pb.Heartbeat())
    m = st.recv(timeout=10)
    log("heartbeat-first:", m)
    assert isinstance(m, tuple) and m[1] == grpc.StatusCode.FAILED_PRECONDITION, m
    # slot released? corrected client connects again
    st2 = Stream(ch)
    st2.send(configure=pb.Configure())  # neither topics nor pattern
    m = st2.recv(timeout=10)
    log("empty-configure:", m)
    assert isinstance(m, tuple) and m[1] == grpc.StatusCode.INVALID_ARGUMENT, m
    # both topics and pattern
    st3 = Stream(ch)
    st3.send(configure=pb.Configure(topics=[topic], topic_pattern=".*"))
    m = st3.recv(timeout=10)
    log("both-forms:", m)
    assert isinstance(m, tuple) and m[1] == grpc.StatusCode.INVALID_ARGUMENT, m
    # transactional commit mode always refused
    st4 = Stream(ch)
    st4.send(configure=pb.Configure(
        topics=[topic], commit_mode=pb.COMMIT_MODE_PERIODIC_TRANSACTIONAL_PRODUCER))
    m = st4.recv(timeout=10)
    log("transactional:", m)
    assert isinstance(m, tuple) and m[1] == grpc.StatusCode.INVALID_ARGUMENT, m
    # now a good session, then a SECOND concurrent one: RESOURCE_EXHAUSTED
    st5 = Stream(ch)
    st5.send(configure=minimal_configure(topic))
    cfd = expect(st5.recv(), 'configured')
    st6 = Stream(ch)
    st6.send(configure=minimal_configure(topic))
    m = st6.recv(timeout=10)
    log("second-concurrent:", m)
    assert isinstance(m, tuple) and m[1] == grpc.StatusCode.RESOURCE_EXHAUSTED, m
    # drain the good session
    d = expect(st5.recv(), 'dispatch')
    for rec in d.records:
        st5.send(report=pb.Report(token=rec.token, success=pb.Report.Success()))
    st5.half_close()
    log("stream end:", st5.recv())
    log("sidecar exit:", sc.wait())


if __name__ == '__main__':
    which = sys.argv[1:] or ['1', '2', '3', '4', '5']
    fns = {'1': scenario_failure_redelivery,
           '2': scenario_out_of_order_and_terminal,
           '3': scenario_proxy_shutdown,
           '4': scenario_reconnect_manifest,
           '5': scenario_violations}
    for w in which:
        fns[w]()
    print()
    print("QUESTIONS RAISED:", len(QUESTIONS))
    for x in QUESTIONS:
        print(" *", x)
