# Wire-format tests for roomload's mini protobuf (Python 3, no deps).
# Expected bytes are hand-computed from the protobuf spec.
import sys
sys.path.insert(0, r'c:\sdsd\dev\roomload')

import roomload as r


def check(name, ok):
    print('%s: %s' % (name, 'OK' if ok else 'FAIL'))
    if not ok:
        sys.exit(1)


# varint vectors
check('varint 1', r._varint(1) == b'\x01')
check('varint 300', r._varint(300) == b'\xac\x02')
check('varint 1302', r._varint(1302) == b'\x96\x0a')

# MsgC2gConnect{version:1, user_id:'42', cookie:'ck'}
ours = r.p_uint(1, 1) + r.p_str(2, '42') + r.p_bytes(3, b'ck')
check('C2gConnect bytes', ours == b'\x08\x01\x12\x02\x34\x32\x1a\x02\x63\x6b')

# parse it back
fields = r.parse_fields(ours)
check('C2gConnect parse',
      r.field_values(fields, 1) == [1]
      and r.field_values(fields, 2) == [b'42']
      and r.field_values(fields, 3) == [b'ck'])

# MsgC2gSubscribe{queues_with_results:[{name:'/chat/777', op_id:9}]}
sub = r.p_str(1, '/chat/777') + r.p_uint(2, 9)
check('Subscription bytes', sub == b'\x0a\x09/chat/777\x10\x09')
ours = r.p_bytes(2, sub)
check('C2gSubscribe bytes', ours == b'\x12\x0d' + sub)

# MsgC2gSendMessage{op_id:3, queue:'/chat/777', mount:'messages', message:'{}'}
ours = (r.p_uint(1, 3) + r.p_str(2, '/chat/777') + r.p_str(3, 'messages')
        + r.p_bytes(4, b'{}'))
check('C2gSendMessage bytes',
      ours == b'\x08\x03\x12\x09/chat/777\x1a\x08messages\x22\x02{}')

# MsgC2gChallengeResponse{op_id:5, response: 16 bytes}
ours = r.p_uint(1, 5) + r.p_bytes(2, b'x' * 16)
check('C2gChallengeResponse bytes', ours == b'\x08\x05\x12\x10' + b'x' * 16)

# Framing with a 2-byte-varint type (C2gConnect = 1302)
wire = r.frame_encode(r.C2G_CONNECT, b'\x08\x01')
check('frame bytes', wire == b'\x08\x96\x0a\x12\x02\x08\x01')
parsed = r.frame_try_parse(bytearray(wire))
check('frame parse', parsed == (1302, b'\x08\x01', len(wire)))

# G2cChallenge frame (3103): type varint is 0xdf 0x18
challenge = r.p_uint(1, 1) + r.p_bytes(2, b'CHALLENGE')
wire = r.frame_encode(3103, challenge)
parsed = r.frame_try_parse(bytearray(wire))
check('G2cChallenge frame', parsed is not None and parsed[0] == 3103)
fields = r.parse_fields(parsed[1])
check('G2cChallenge fields', r.field_values(fields, 2) == [b'CHALLENGE'])

# fragmented delivery: frame only completes at full length
whole = r.frame_encode(r.C2G_SUBSCRIBE, ours)
buf = bytearray()
count = 0
for i in range(len(whole)):
    buf.append(whole[i])
    if r.frame_try_parse(buf) is not None:
        count += 1
check('fragmented frame completes once', count == 1)

# two frames back-to-back in one buffer
buf = bytearray(whole + whole)
first = r.frame_try_parse(buf)
del buf[:first[2]]
second = r.frame_try_parse(buf)
check('two frames in stream', first[0] == 1309 and second[0] == 1309)

# G2cResult{op_id:5, status:0}
fields = r.parse_fields(b'\x08\x05\x10\x00')
check('G2cResult parse',
      r.field_values(fields, 1) == [5] and r.field_values(fields, 2) == [0])

print('all wire-format tests passed')
