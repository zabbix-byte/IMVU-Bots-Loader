# Offline test of the IMQ socket client against a mock gateway (Python 3).
import hashlib
import socket
import sys
import threading
import time

sys.path.insert(0, r'c:\sdsd\dev\roomload')
import roomload as r

TOKEN = b'token-xyz'
COOKIE = b'cookie-bytes'
CHALLENGE = b'server-challenge'
QUEUE = '/chat/777'

seen = {}
suicide_seen = threading.Event()
floodgates_seen = threading.Event()


def read_frame(conn):
    buf = bytearray()
    while True:
        parsed = r.frame_try_parse(buf)
        if parsed:
            return parsed[0], r.parse_fields(parsed[1])
        chunk = conn.recv(65536)
        if not chunk:
            raise EOFError('closed')
        buf.extend(chunk)


def mock_gateway(conn):
    try:
        # expect C2gConnect
        mtype, fields = read_frame(conn)
        assert mtype == r.C2G_CONNECT, mtype
        seen['connect'] = fields
        assert r.field_values(fields, 1) == [1]           # version
        assert r.field_values(fields, 2) == [b'361688157']  # user_id
        assert r.field_values(fields, 3) == [COOKIE]        # cookie

        # send G2cChallenge
        conn.sendall(r.frame_encode(
            r.G2C_CHALLENGE, r.p_uint(1, 1) + r.p_bytes(2, CHALLENGE)))

        # expect C2gChallengeResponse with md5(challenge + token)
        mtype, fields = read_frame(conn)
        assert mtype == r.C2G_CHALLENGE_RESPONSE, mtype
        op = r.field_values(fields, 1)[0]
        expected = hashlib.md5(CHALLENGE + TOKEN).digest()
        assert r.field_values(fields, 2)[0] == expected, 'bad challenge response'
        seen['auth_op'] = op

        # G2cResult status 0
        conn.sendall(r.frame_encode(
            r.G2C_RESULT, r.p_uint(1, op) + r.p_uint(2, 0)))

        # expect C2gOpenFloodgates right after auth
        mtype, fields = read_frame(conn)
        assert mtype == r.C2G_OPEN_FLOODGATES, mtype
        seen['floodgates'] = True
        floodgates_seen.set()

        # expect C2gSubscribe (the test drives ImqClient directly, so only
        # the chat queue is subscribed here; run_account adds /user/<cid>)
        mtype, fields = read_frame(conn)
        assert mtype == r.C2G_SUBSCRIBE, mtype
        sub_raw = r.field_values(fields, 2)[0]
        sub = r.parse_fields(sub_raw)
        assert r.field_values(sub, 1) == [QUEUE.encode()]
        sub_op = r.field_values(sub, 2)[0]
        seen['sub_op'] = sub_op

        # answer with G2cJoinedQueue only (no result) to test that success path
        conn.sendall(r.frame_encode(
            r.G2C_JOINED_QUEUE,
            r.p_str(1, '361688157') + r.p_str(2, QUEUE) + r.p_uint(3, 1)))

        # expect C2gSendMessage
        mtype, fields = read_frame(conn)
        assert mtype == r.C2G_SEND_MESSAGE, mtype
        seen['queue'] = r.field_values(fields, 2)[0]
        seen['mount'] = r.field_values(fields, 3)[0]
        seen['payload'] = r.field_values(fields, 4)[0]

        # echo it back as G2cSendMessage
        conn.sendall(r.frame_encode(
            r.G2C_SEND_MESSAGE,
            r.p_bytes(1, b'361688157') + r.p_str(2, QUEUE)
            + r.p_str(3, 'messages') + r.p_bytes(4, seen['payload'])
            + r.p_uint(5, 1)))

        # expect a ping within ~25s, reply pong, then suicide
        mtype, fields = read_frame(conn)
        assert mtype == r.C2G_PING, mtype
        seen['pinged'] = True
        conn.sendall(r.frame_encode(r.G2C_PONG, b''))

        mtype, fields = read_frame(conn)
        assert mtype == r.C2G_SUICIDE, mtype
        seen['suicide'] = True
        suicide_seen.set()
    finally:
        conn.close()


def check(name, ok):
    print('%s: %s' % (name, 'OK' if ok else 'FAIL'))
    if not ok:
        sys.exit(1)


listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen(1)
port = listener.getsockname()[1]


def serve():
    conn, _ = listener.accept()
    mock_gateway(conn)


threading.Thread(target=serve, daemon=True).start()

client = r.ImqClient('127.0.0.1', port, use_tls=False, insecure=False)
client.connect('361688157', COOKIE, TOKEN)
check('handshake', True)
floodgates_seen.wait(timeout=5)
check('floodgates opened', seen.get('floodgates') is True)

client.subscribe(QUEUE)
check('subscribe', True)

payload = b'{"userId": 361688157, "chatId": 777, "message": "hola", "to": 0}'
client.send_chat(QUEUE, payload)

stop = threading.Event()


def stopper():
    time.sleep(21.5)  # let one ping cycle happen (PING_INTERVAL = 20)
    stop.set()


threading.Thread(target=stopper, daemon=True).start()
client.run_until(stop)
client.close()
suicide_seen.wait(timeout=5)

check('queue name', seen.get('queue') == QUEUE.encode())
check('mount', seen.get('mount') == b'messages')
check('payload', seen.get('payload') == payload)
check('echo counted', client.echoes == 1)
check('ping sent', seen.get('pinged') is True)
check('pong counted', client.pongs == 1)
check('suicide sent', seen.get('suicide') is True)

listener.close()
print('all imq tests passed')
