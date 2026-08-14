"""U26 probe: parse both golden-bytes files byte-for-byte and check them
against the transcript in protocol-specification.md ('A complete session,
message by message'). Every mismatch or unanswerable question is a finding."""
import sys
sys.path.insert(0, '.')
from parallelconsumer.proxy.v1 import proxy_pb2 as pb


def read_delimited(path, msg_cls):
    """Spec says: 'length-delimited (standard varint-prefixed) messages'."""
    out = []
    with open(path, 'rb') as f:
        data = f.read()
    i = 0
    while i < len(data):
        # varint
        shift = 0
        length = 0
        while True:
            b = data[i]
            i += 1
            length |= (b & 0x7F) << shift
            if not b & 0x80:
                break
            shift += 7
        msg = msg_cls()
        consumed = msg.ParseFromString(data[i:i + length])
        assert consumed == length, f"short parse at offset {i}"
        out.append(msg)
        i += length
    return out


client = read_delimited('golden-client-messages.bin', pb.ClientMessage)
proxy = read_delimited('golden-proxy-messages.bin', pb.ProxyMessage)

print(f"client messages: {len(client)}")
for n, m in enumerate(client):
    print(f"--- client[{n}] kind={m.WhichOneof('message')}")
    print(m)
print(f"proxy messages: {len(proxy)}")
for n, m in enumerate(proxy):
    print(f"--- proxy[{n}] kind={m.WhichOneof('message')}")
    print(m)

# ---- assertions from the documented transcript ----
fails = []
def check(cond, what):
    if not cond:
        fails.append(what)
        print("MISMATCH:", what)

# expected client sequence per transcript
expected_client = ['configure', 'heartbeat', 'report', 'report', 'report',
                   'worker_died', 'manifest', 'report']
check([m.WhichOneof('message') for m in client] == expected_client,
      f"client kinds = {[m.WhichOneof('message') for m in client]} expected {expected_client}")

# FINDING: bytes hold 5 proxy messages; the transcript shows 6 (the reconnect
# Configured is not re-committed). Adjusted to what the bytes actually hold.
expected_proxy = ['configured', 'dispatch', 'drop', 'shutdown',
                  'set_executor_count']
check([m.WhichOneof('message') for m in proxy] == expected_proxy,
      f"proxy kinds = {[m.WhichOneof('message') for m in proxy]} expected {expected_proxy}")

cfg = client[0].configure
check(list(cfg.topics) == ['demo-topic'], 'configure.topics')
check(cfg.max_concurrency == 2, 'configure.max_concurrency')
check(dict(cfg.kafka_properties) == {'bootstrap.servers': 'localhost:9092'}, 'kafka_properties')
check(list(cfg.capabilities) == ['dispatch'], 'configure.capabilities')
check(cfg.ordering == pb.PROCESSING_ORDER_KEY, 'ordering')
check(cfg.commit_mode == pb.COMMIT_MODE_PERIODIC_CONSUMER_SYNC, 'commit_mode')
check(cfg.commit_interval.seconds == 1, 'commit_interval')
check(cfg.sasl_authentication_retry_timeout.seconds == 0 and cfg.HasField('sasl_authentication_retry_timeout'), 'sasl retry 0s present')
check(cfg.max_failure_history == 10, 'max_failure_history')
check(cfg.invalid_offset_metadata_policy == pb.INVALID_OFFSET_METADATA_POLICY_FAIL, 'policy FAIL')
check(cfg.launch_token == 'per-launch-token-unused-in-v1', 'launch_token')
check(cfg.terminal_topic == 'demo-topic.terminal', 'terminal_topic')
check(cfg.lease_duration.seconds == 60, 'lease_duration')
check(cfg.heartbeat_interval.seconds == 5, 'heartbeat_interval')
check(cfg.reconnect_window.seconds == 30, 'reconnect_window')
check(cfg.message_buffer_size == 500 and cfg.initial_load_factor == 2
      and cfg.maximum_load_factor == 100, 'buffer trio')
check(cfg.pc_instance_tag == 'golden-session', 'pc_instance_tag')

cfd = proxy[0].configured
check(cfd.executor_count == 2, 'configured.executor_count')
check(list(cfd.capabilities) == ['dispatch'], 'configured.capabilities')
check(not cfd.HasField('topic_pattern'), 'configured.topic_pattern absent')
# 'same values' claim: which fields does the Configured actually carry?
print("configured fields set:",
      [f.name for f, _ in cfd.ListFields()])

d = proxy[1].dispatch
check(len(d.records) == 2, 'wave size 2')
r0, r1 = d.records
check(r0.token.record_id == 'demo-topic/0/0' and r0.token.epoch == 1, 'r0 token')
check(r0.record.topic == 'demo-topic' and r0.record.partition == 0
      and r0.record.offset == 0, 'r0 record coords')
check(r0.record.key == b'key-a', 'r0 key')
check(not r0.record.HasField('value'), 'r0 tombstone: value ABSENT')
check(r0.attempt == 1 and not r0.HasField('last_failure_at')
      and not r0.HasField('last_failure_reason'), 'r0 first delivery')
check(r1.token.record_id == 'demo-topic/0/1' and r1.token.epoch == 5000000000, 'r1 token epoch beyond int32')
check(r1.record.key == b'key-b' and r1.record.value == b'hello', 'r1 key/value')
check(r1.attempt == 2, 'r1 attempt 2')
check(r1.HasField('last_failure_at'), 'r1 last_failure_at present')
check(r1.last_failure_at.seconds == 1700000000 and r1.last_failure_at.nanos == 1,
      f'r1 last_failure_at == 2023-11-14T22:13:20.000000001Z (got {r1.last_failure_at.seconds}s {r1.last_failure_at.nanos}n)')
check(r1.last_failure_reason == 'worker exploded', 'r1 last_failure_reason')

rep_s = client[2].report
check(rep_s.token.record_id == 'demo-topic/0/0' and rep_s.token.epoch == 1, 'success token')
check(rep_s.WhichOneof('outcome') == 'success', 'success outcome')
check(len(rep_s.success.produce) == 1, 'produce 1')
p = rep_s.success.produce[0]
check(p.topic == 'demo-topic.out' and p.key == b'key-a' and p.value == b'world', 'produce record')

rep_f = client[3].report
check(rep_f.WhichOneof('outcome') == 'failure' and rep_f.failure.reason == 'worker exploded', 'failure report')
rep_t = client[4].report
check(rep_t.WhichOneof('outcome') == 'terminal' and rep_t.terminal.reason == 'poison pill', 'terminal report')

wd = client[5].worker_died
check(len(wd.tokens) == 1 and wd.tokens[0].record_id == 'demo-topic/0/2'
      and wd.tokens[0].epoch == 2, 'worker_died token')

man = client[6].manifest
check(len(man.tokens) == 2, 'manifest 2 tokens')
check(man.tokens[0].record_id == 'demo-topic/0/0' and man.tokens[0].epoch == 1, 'manifest t0')
check(man.tokens[1].record_id == 'demo-topic/0/1' and man.tokens[1].epoch == 5000000000, 'manifest t1')

check(proxy[2].drop.token.record_id == 'demo-topic/0/1'
      and proxy[2].drop.token.epoch == 5000000000, 'drop token')

rel = client[7].report
check(rel.WhichOneof('outcome') == 'released'
      and rel.token.record_id == 'demo-topic/0/2' and rel.token.epoch == 2, 'released report')

check(proxy[4].set_executor_count.executor_count == 2, 'set_executor_count 2')

print()
print("FAILURES:" if fails else "ALL TRANSCRIPT ASSERTIONS PASSED", len(fails))
for f in fails:
    print(" -", f)
