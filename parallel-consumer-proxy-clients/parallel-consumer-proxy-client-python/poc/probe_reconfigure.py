import sys, time, queue
sys.path.insert(0, '.')
import grpc
from probe import Sidecar, Stream, expect, minimal_configure, QUESTIONS
from parallelconsumer.proxy.v1 import proxy_pb2 as pb

topic = "a-processed-record-advances-the-committed-offset"
sc = Sidecar(topic)
ch = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
st = Stream(ch)
st.send(configure=minimal_configure(topic))
cfd = expect(st.recv(), 'configured')
print("session 1 configured OK")
# drop the connection without reporting
st.call.cancel(); ch.close(); time.sleep(0.5)
ch2 = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
st2 = Stream(ch2)
st2.send(configure=minimal_configure(topic))
m = st2.recv(timeout=10)
if isinstance(m, tuple):
    print("Configure on new stream while configured -> REFUSED:", m[1], m[2])
else:
    print("Configure on new stream while configured -> ACCEPTED:", m.WhichOneof('message'))
    # can we still get the record and finish?
    try:
        d = expect(st2.recv(timeout=5), 'dispatch')
        print("got dispatch again:", [(r.token.record_id, r.token.epoch, r.attempt) for r in d.records])
        for rec in d.records:
            st2.send(report=pb.Report(token=rec.token, success=pb.Report.Success()))
    except queue.Empty:
        print("no dispatch on the re-configured session (records still held for old session?)")
st2.half_close()
print("end:", st2.recv())
sc.proc.kill()
