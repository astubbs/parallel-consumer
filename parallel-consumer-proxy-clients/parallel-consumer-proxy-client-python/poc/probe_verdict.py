# Copyright (C) 2026 Antony Stubbs and contributors

import sys, time, queue, threading
sys.path.insert(0, '.')
import grpc
from probe import Sidecar, Stream, expect, minimal_configure
from parallelconsumer.proxy.v1 import proxy_pb2 as pb

topic = "an-unreported-record-holds-back-the-commit"
sc = Sidecar(topic)
lines = []
threading.Thread(target=lambda: [lines.append(l.decode().rstrip()) for l in sc.proc.stdout], daemon=True).start()
ch = grpc.insecure_channel(f"127.0.0.1:{sc.port}")
st = Stream(ch)
st.send(configure=minimal_configure(topic))
expect(st.recv(), 'configured')
d = expect(st.recv(), 'dispatch')
print("received", len(d.records), "record(s); reporting NOTHING")
time.sleep(2)
st.half_close()
print("end:", st.recv())
sc.kill_stdin()
print("exit code:", sc.proc.wait(timeout=15))
print("--- last stdout ---")
for l in lines[-6:]:
    print(repr(l[:200]))
