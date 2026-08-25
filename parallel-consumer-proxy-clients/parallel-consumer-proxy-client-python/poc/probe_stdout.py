# Copyright (C) 2026 Antony Stubbs and contributors

import sys, time, queue
sys.path.insert(0, '.')
import grpc, threading
from probe import Sidecar, Stream, expect, minimal_configure
from parallelconsumer.proxy.v1 import proxy_pb2 as pb

topic = "a-processed-record-advances-the-committed-offset"
sc = Sidecar(topic)
lines = []
def drain():
    for line in sc.proc.stdout:
        lines.append(line.decode().rstrip())
threading.Thread(target=drain, daemon=True).start()
ch = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
st = Stream(ch)
st.send(configure=minimal_configure(topic))
cfd = expect(st.recv(), 'configured')
d = expect(st.recv(), 'dispatch')
for rec in d.records:
    st.send(report=pb.Report(token=rec.token, success=pb.Report.Success()))
time.sleep(3)
st.half_close()
print("end:", st.recv())
sc.kill_stdin()
print("exit:", sc.proc.wait(timeout=10))
print("--- stdout lines after port line ---")
for l in lines:
    print(repr(l))
