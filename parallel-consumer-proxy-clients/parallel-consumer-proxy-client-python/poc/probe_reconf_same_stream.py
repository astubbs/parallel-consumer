import sys, time, queue
sys.path.insert(0, '.')
import grpc
from probe import Sidecar, Stream, expect, minimal_configure
from parallelconsumer.proxy.v1 import proxy_pb2 as pb

topic = "a-processed-record-advances-the-committed-offset"
sc = Sidecar(topic)
ch = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
st = Stream(ch)
st.send(configure=minimal_configure(topic, max_concurrency=3))
c1 = expect(st.recv(), 'configured')
print("first Configured: max_concurrency", c1.max_concurrency)
d = expect(st.recv(), 'dispatch')
# ask again, with DIFFERENT values, on the same stream
st.send(configure=minimal_configure(topic, max_concurrency=9))
m = st.recv(timeout=10)
if isinstance(m, tuple):
    print("second Configure on same stream -> stream closed:", m[1], m[2][:120])
else:
    kind = m.WhichOneof('message')
    print("second Configure on same stream ->", kind)
    if kind == 'configured':
        print("re-sent max_concurrency:", m.configured.max_concurrency,
              "identical to original:", m.configured == c1)
for rec in d.records:
    st.send(report=pb.Report(token=rec.token, success=pb.Report.Success()))
st.half_close()
print("end:", st.recv())
sc.proc.kill()
