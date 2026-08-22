# Copyright (C) 2026 Antony Stubbs and contributors

import sys, time, queue
sys.path.insert(0, '.')
import grpc
from probe import Sidecar, Stream, expect, minimal_configure
from parallelconsumer.proxy.v1 import proxy_pb2 as pb

topic = "a-failed-record-is-redelivered-with-its-failure-history"
sc = Sidecar(topic)
ch = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
st = Stream(ch)
st.send(configure=minimal_configure(topic))
cfd = expect(st.recv(), 'configured')
d = expect(st.recv(), 'dispatch')
r = d.records[0]
print("delivery 1: epoch", r.token.epoch)
st.send(report=pb.Report(token=r.token, failure=pb.Report.Failure(reason="fence probe")))
d2 = expect(st.recv(), 'dispatch')
r2 = d2.records[0]
print("delivery 2: epoch", r2.token.epoch, "attempt", r2.attempt)
# 1) stale-epoch success: must be discarded, live delivery untouched
st.send(report=pb.Report(token=r.token, success=pb.Report.Success()))
# 2) fabricated token: must be discarded, nothing disturbed
st.send(report=pb.Report(token=pb.Token(record_id="no/such/record", epoch=7),
                         success=pb.Report.Success()))
time.sleep(1.5)
# stream should still be open; the live delivery should still accept its report
st.send(report=pb.Report(token=r2.token, success=pb.Report.Success()))
try:
    m = st.recv(timeout=4)
    print("post-success traffic:", m if isinstance(m, tuple) else m.WhichOneof('message'))
except queue.Empty:
    print("silence after live-epoch success (good: resolved, no redelivery)")
st.half_close()
print("end:", st.recv())
sc.proc.kill()
print("FENCING PROBE DONE")
