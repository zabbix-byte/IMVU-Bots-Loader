#!/usr/bin/env python3
"""roomload - fill a chat room with test accounts for backend capacity testing.

Replicates the desktop client wire protocol against YOUR OWN backend:

  1. XML-RPC login:   test.avatarInfoForLogin2   (client.php endpoint)
  2. XML-RPC room:    chat.getOrMakeChat(publicroom=ROOM)  (chat.php endpoint,
                      with X-imvu-userid / X-imvu-csid / X-imvu-auth headers)
  3. IMQ socket:      Framing protobuf stream, C2gConnect -> G2cChallenge ->
                      C2gChallengeResponse (md5(challenge + imq_auth_token)) ->
                      G2cResult
  4. Subscribe /chat/<chatId>, send C2gSendMessage JSON payloads, ping to
     stay alive.

Only run this against infrastructure you own, with test accounts created
for this purpose.
"""

import argparse
import hashlib
import json
import os
import re
import random
import socket
import ssl
import sys
import threading
import time
import urllib.parse
import urllib.request
import xmlrpc.client

# --- IMQ message type codes (from the client's gw_messages.proto) -----------

C2G_PING = 1301
C2G_CONNECT = 1302
C2G_CHALLENGE_RESPONSE = 1303
C2G_SEND_MESSAGE = 1304
C2G_OPEN_FLOODGATES = 1306
C2G_SUICIDE = 1307
C2G_SUBSCRIBE = 1309

G2C_PONG = 3101
G2C_RESULT = 3102
G2C_CHALLENGE = 3103
G2C_SEND_MESSAGE = 3104
G2C_JOINED_QUEUE = 3106
G2C_CONNECTION_CLOSED = 3109

IMQ_PROTOCOL_VERSION = 1
PING_INTERVAL = 20.0

CID_KEYS = ('customer_id', 'cid', 'customerId', 'user_id', 'userId', 'id')
CHATID_KEYS = ('chatId', 'chat_id', 'chatid', 'id')


# --- minimal protobuf (only what gw_messages.proto needs) -------------------

def _varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _tag(field, wire):
    return _varint((field << 3) | wire)


def p_uint(field, n):
    return _tag(field, 0) + _varint(int(n))


def p_bytes(field, data):
    return _tag(field, 2) + _varint(len(data)) + bytes(data)


def p_str(field, s):
    return p_bytes(field, str(s).encode('utf-8'))


def read_varint(buf, pos):
    shift = 0
    result = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            return result, pos


def parse_fields(data):
    """Decode a protobuf message into [(field, wire, value)]."""
    fields = []
    pos = 0
    while pos < len(data):
        tag, pos = read_varint(data, pos)
        field, wire = tag >> 3, tag & 7
        if wire == 0:
            value, pos = read_varint(data, pos)
        elif wire == 2:
            length, pos = read_varint(data, pos)
            value = data[pos:pos + length]
            pos += length
        elif wire == 1:
            value = data[pos:pos + 8]
            pos += 8
        elif wire == 5:
            value = data[pos:pos + 4]
            pos += 4
        else:
            raise ValueError('unsupported wire type %d' % wire)
        fields.append((field, wire, value))
    return fields


def field_values(fields, number):
    return [v for f, w, v in fields if f == number]


def frame_encode(mtype, payload):
    return p_uint(1, mtype) + p_bytes(2, payload)


def frame_try_parse(buf):
    """Parse one Framing message from buf. Return (mtype, data, consumed) or None."""
    try:
        pos = 0
        tag, pos = read_varint(buf, pos)
        if tag >> 3 != 1 or tag & 7 != 0:
            raise ValueError('framing: expected field 1 varint')
        mtype, pos = read_varint(buf, pos)
        tag, pos = read_varint(buf, pos)
        if tag >> 3 != 2 or tag & 7 != 2:
            raise ValueError('framing: expected field 2 bytes')
        length, pos = read_varint(buf, pos)
        if len(buf) - pos < length:
            return None
        return mtype, bytes(buf[pos:pos + length]), pos + length
    except IndexError:
        return None


# --- XML-RPC with the client's auth headers ---------------------------------

class BackendError(Exception):
    pass


def xmlrpc_call(url, method, params, auth=None, insecure=False, timeout=30):
    body = xmlrpc.client.dumps(params, methodname=method).encode('utf-8')
    headers = {
        'Content-Type': 'text/xml',
        'User-Agent': 'IMVU Client',
    }
    if auth:
        cid, csid, key = auth
        headers['X-imvu-userid'] = str(cid)
        headers['X-imvu-csid'] = str(csid)
        headers['X-imvu-auth'] = hashlib.md5(
            str(cid).encode('utf-8') + str(key).encode('utf-8') + body
        ).hexdigest()
    req = urllib.request.Request(url, data=body, headers=headers)
    context = None
    if insecure and url.startswith('https'):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
            data = resp.read()
    except urllib.error.HTTPError as e:
        raise BackendError('HTTP %s from %s: %s' % (e.code, url, e.read()[:200]))
    except urllib.error.URLError as e:
        raise BackendError('cannot reach %s: %s' % (url, e.reason))
    try:
        out, _ = xmlrpc.client.loads(data)
    except xmlrpc.client.Fault as fault:
        raise BackendError('XML-RPC fault %s: %s' % (fault.faultCode, fault.faultString))
    return out[0] if out else None


def detect_key(info, candidates, override, what):
    if override:
        if override in info:
            return info[override]
        raise BackendError('%s key %r not in response; keys: %s'
                           % (what, override, sorted(info)))
    for key in candidates:
        if key in info:
            return info[key]
    raise BackendError('cannot find %s in response; keys: %s'
                       % (what, sorted(info)))


# --- IMQ socket client -------------------------------------------------------

class ImqClient(object):
    def __init__(self, host, port, use_tls, insecure, timeout=15.0):
        self.host = host
        self.port = port
        self.use_tls = use_tls
        self.insecure = insecure
        self.timeout = timeout
        self.sock = None
        self.buf = bytearray()
        self.op_id = 0
        self.echoes = 0
        self.pongs = 0
        self.closed_by_server = False
        self.debug_frames = False
        self.on_message = None
        self.send_lock = threading.Lock()

    def _next_op(self):
        self.op_id += 1
        return self.op_id

    def open(self):
        raw = socket.create_connection((self.host, self.port), timeout=self.timeout)
        if self.use_tls:
            if self.insecure:
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            else:
                context = ssl.create_default_context()
            self.sock = context.wrap_socket(raw, server_hostname=self.host)
        else:
            self.sock = raw
        self.sock.settimeout(self.timeout)

    def _send(self, mtype, payload):
        with self.send_lock:
            self.sock.sendall(frame_encode(mtype, payload))

    def _recv_more(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise BackendError('IMQ connection closed by peer')
        self.buf.extend(chunk)

    def read_frame(self):
        while True:
            parsed = frame_try_parse(self.buf)
            if parsed:
                mtype, data, consumed = parsed
                del self.buf[:consumed]
                return mtype, parse_fields(data)
            self._recv_more()

    def connect(self, user_id, cookie, auth_token):
        """C2gConnect -> G2cChallenge -> challenge response -> G2cResult."""
        self.open()
        connect = (p_uint(1, IMQ_PROTOCOL_VERSION)
                   + p_str(2, user_id)
                   + p_bytes(3, cookie))
        self._send(C2G_CONNECT, connect)

        mtype, fields = self.read_frame()
        if mtype != G2C_CHALLENGE:
            raise BackendError('expected G2cChallenge, got type %d' % mtype)
        challenge = field_values(fields, 2)[0]

        op = self._next_op()
        response = hashlib.md5(challenge + auth_token).digest()
        msg = p_uint(1, op) + p_bytes(2, response)
        self._send(C2G_CHALLENGE_RESPONSE, msg)

        mtype, fields = self.read_frame()
        if mtype != G2C_RESULT:
            raise BackendError('expected G2cResult, got type %d' % mtype)
        status = field_values(fields, 2)[0]
        if status != 0:
            error = field_values(fields, 3)
            raise BackendError('IMQ auth failed, status %d: %s'
                               % (status, error[0].decode('utf-8', 'replace') if error else ''))
        # The client opens the floodgates right after auth; the gateway holds
        # all traffic (including subscribe results) until it gets this.
        self._send(C2G_OPEN_FLOODGATES, b'')

    def subscribe(self, queue, retries=5):
        for attempt in range(retries):
            op = self._next_op()
            subscription = p_str(1, queue) + p_uint(2, op)
            msg = p_bytes(2, subscription)  # queues_with_results
            self._send(C2G_SUBSCRIBE, msg)
            try:
                while True:
                    mtype, fields = self.read_frame()
                    if self.debug_frames:
                        print('    [frame] type %d fields %s' % (mtype, fields))
                    if mtype == G2C_RESULT:
                        ops = field_values(fields, 1)
                        status = field_values(fields, 2)[0]
                        if ops and ops[0] == op:
                            if status != 0:
                                raise BackendError('subscribe %s failed, status %d' % (queue, status))
                            return
                    elif mtype == G2C_JOINED_QUEUE:
                        queues = field_values(fields, 2)
                        if queues and queues[0].decode('utf-8', 'replace') == queue:
                            return
                    else:
                        self._handle_async(mtype, fields)
            except socket.timeout:
                if attempt + 1 < retries:
                    continue
                raise BackendError('subscribe %s timed out after %d tries' % (queue, retries))
    def send_chat(self, queue, payload):
        op = self._next_op()
        msg = (p_uint(1, op)
               + p_str(2, queue)
               + p_str(3, 'messages')
               + p_bytes(4, payload))
        self._send(C2G_SEND_MESSAGE, msg)

    def ping(self):
        self._send(C2G_PING, b'')

    def _handle_async(self, mtype, fields):
        if mtype == G2C_SEND_MESSAGE:
            self.echoes += 1
            if self.on_message:
                user_id = field_values(fields, 1)
                queues = field_values(fields, 2)
                msgs = field_values(fields, 4)
                self.on_message(
                    user_id[0] if user_id else b'',
                    queues[0].decode('utf-8', 'replace') if queues else '',
                    msgs[0] if msgs else b'')
        elif mtype == G2C_PONG:
            self.pongs += 1
        elif mtype == G2C_CONNECTION_CLOSED:
            self.closed_by_server = True

    def run_until(self, stop_event, deadline=None):
        """Drain frames; ping after PING_INTERVAL idle seconds. Returns when
        stop_event is set or the server closes the connection."""
        self.sock.settimeout(2.0)
        last_traffic = time.monotonic()
        while not stop_event.is_set() and not self.closed_by_server:
            if deadline is not None and time.time() >= deadline:
                return
            try:
                mtype, fields = self.read_frame()
                self._handle_async(mtype, fields)
                last_traffic = time.monotonic()
            except socket.timeout:
                if time.monotonic() - last_traffic >= PING_INTERVAL:
                    self.ping()
                    last_traffic = time.monotonic()

    def close(self):
        if not self.sock:
            return
        try:
            self._send(C2G_SUICIDE, b'')
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass
        self.sock = None


# --- per-account session -----------------------------------------------------

class AccountSession(object):
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.cid = None
        self.chat_id = None
        self.seat = None
        self.imq = None
        self.error = None
        self.spam_stop = None
        self.login_ms = None
        self.imq_ms = None
        self.sent = 0
        self.info = None
        self.cycles = 0
        self.echoes = 0
        self.pongs = 0


def probe_chat(session, info, opts, chat_id):
    """Call chat.getParticipants to inspect what the backend knows about a
    chat id (looking for the room instance id / activity)."""
    url = '%s://%s%s' % (opts.chat_scheme, opts.chat_host, opts.chat_endpoint)
    cid = session.cid
    auth = (cid, info.get('clientSessionId', ''), info.get('securityKey', ''))
    args = {'userId': cid, 'chatId': int(chat_id)}
    return xmlrpc_call(url, 'chat.getParticipants', (args,), auth=auth,
                       insecure=opts.insecure)


def find_rooms(session, info, opts, keywords):
    """Search public rooms by name; return [{roomInstanceId, name, ...}].

    Uses the same endpoint as the client's room list:
    GET /api/rooms/rooms_list_paginated.php?search=...&cid=...
    """
    query = urllib.parse.urlencode({'search': keywords, 'cid': session.cid})
    url = '%s://%s/api/rooms/rooms_list_paginated.php?%s' % (
        opts.service_scheme, opts.service_host, query)
    # REST auth: X-imvu-auth = md5(userId + securityKey + url query string)
    cid = session.cid
    key = info.get('securityKey', '')
    headers = {
        'User-Agent': 'IMVU Client',
        'X-imvu-userid': str(cid),
        'X-imvu-csid': str(info.get('clientSessionId', '')),
        'X-imvu-auth': hashlib.md5(
            str(cid).encode('utf-8') + str(key).encode('utf-8')
            + query.encode('utf-8')).hexdigest(),
    }
    req = urllib.request.Request(url, headers=headers)
    context = insecure_context(opts)
    try:
        with urllib.request.urlopen(req, timeout=30, context=context) as resp:
            raw = resp.read().decode('utf-8', 'replace')
    except urllib.error.HTTPError as e:
        raise BackendError('HTTP %s from room search: %s'
                           % (e.code, e.read()[:200]))
    try:
        data = json.loads(raw)
    except ValueError:
        raise BackendError('room search did not return JSON: %r' % raw[:300])
    rooms = []
    cids = data.get('customers_id') or []
    for i in range(len(cids)):
        def col(key, default='?'):
            values = data.get(key)
            return values[i] if values and i < len(values) else default
        rooms.append({
            'roomInstanceId': '%s-%s' % (cids[i], col('customers_room_id')),
            'name': col('name'),
            'owner': col('customers_name'),
            'participants': col('num_participants'),
        })
    if not rooms:
        print('room search raw response: %r' % raw[:500])
    return rooms


def insecure_context(opts):
    if not opts.insecure:
        return None
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def load_accounts(path, count):
    accounts = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' not in line:
                raise BackendError('bad accounts.txt line (want user:pass): %r' % line)
            user, password = line.split(':', 1)
            accounts.append(AccountSession(user.strip(), password.strip()))
            if count and len(accounts) >= count:
                break
    return accounts


def login(session, opts):
    url = '%s://%s%s' % (opts.secure_scheme, opts.secure_host, opts.client_endpoint)
    params = {
        'avatarname': session.username,
        'client_version': opts.client_version,
        'system_info': {},
        'client_type': 'imvu',
        'client_experiments': [],
        'password': session.password,
    }
    info = xmlrpc_call(url, 'test.avatarInfoForLogin2', (params,),
                       insecure=opts.insecure)
    if not isinstance(info, dict):
        raise BackendError('unexpected login response: %r' % (info,))
    if opts.print_userinfo:
        log(session, 'userInfo keys: %s' % sorted(info))
    return info


def chat_url(opts):
    return '%s://%s%s' % (opts.chat_scheme, opts.chat_host, opts.chat_endpoint)


def chat_auth(session, info):
    return (session.cid, info.get('clientSessionId', ''), info.get('securityKey', ''))


def get_or_make_chat(session, info, opts, invite=None):
    """Register the account into the chat server-side and return the chat id.

    Mirrors JoinRoomSession.getOrMakeChat: activity is 'publicroom-<roomId>',
    chatId is 0 when unknown, publicroom=True. With --chat-id --register we
    pass the chat id instead of the room activity. Plain --chat-id skips the
    call entirely and subscribes to the queue directly.

    invite is (chat_id, from_user_id) when another agent already in the room
    invited this account.
    """
    if opts.chat_id and not getattr(opts, 'register', False) and not invite:
        return opts.chat_id
    url = chat_url(opts)
    cid = session.cid
    auth = chat_auth(session, info)
    args = {'userId': cid, 'version': opts.client_version,
            'publicroom': True, 'private': False}
    if opts.room:
        args['activity'] = 'publicroom-%s' % opts.room
    if invite:
        invite_chat_id, from_user = invite
        args['chatId'] = int(invite_chat_id)
        # Same instance as the host: activity + live chatId. No fromUserId /
        # invite flag — those make chat.php require an invite row (1012).
    elif opts.chat_id:
        args['chatId'] = int(opts.chat_id)
    else:
        args['chatId'] = 0
    result = xmlrpc_call(url, 'chat.getOrMakeChat', (args,), auth=auth,
                         insecure=opts.insecure)
    if not isinstance(result, dict):
        raise BackendError('unexpected getOrMakeChat response: %r' % (result,))
    if result.get('response') == 'declined':
        raise BackendError('join declined: %s %s'
                           % (result.get('reason'), result.get('explanation')))
    session.seat = result.get('seat')
    if opts.chat_id and not invite and not any(k in result for k in CHATID_KEYS):
        return opts.chat_id
    return detect_key(result, CHATID_KEYS, opts.chatid_key, 'chat id')


def load_wordlist(path):
    words = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            w = line.strip()
            if w:
                words.append(w)
    if not words:
        raise BackendError('wordlist %s is empty' % path)
    return words


def spam_wait(opts):
    lo = getattr(opts, 'spam_delay', 0.3)
    hi = getattr(opts, 'spam_delay_max', 0.5)
    if hi < lo:
        hi = lo
    return random.uniform(lo, hi)


def spam_loop(session, opts, queue, stop):
    while not stop.is_set():
        imq = session.imq
        if imq is None or imq.closed_by_server:
            return
        word = random.choice(opts.spam_words)
        payload = json.dumps({
            'userId': session.cid,
            'chatId': session.chat_id,
            'message': word,
            'to': 0,
        }).encode('utf-8')
        try:
            imq.send_chat(queue, payload)
            session.sent += 1
            with opts.stats_lock:
                opts.stats['sent'] += 1
        except Exception:
            return
        stop.wait(spam_wait(opts))


def make_trigger_handler(session, opts, queue):
    trigger_re = re.compile(r'\b' + re.escape(opts.trigger) + r'\b',
                            re.IGNORECASE)
    stop_re = (re.compile(r'\b' + re.escape(opts.stop_trigger) + r'\b',
                          re.IGNORECASE)
               if opts.stop_trigger else None)
    watch = str(opts.trigger_from) if opts.trigger_from else None

    def start_spam():
        if session.spam_stop is not None and not session.spam_stop.is_set():
            return
        session.spam_stop = threading.Event()
        t = threading.Thread(target=spam_loop,
                             args=(session, opts, queue, session.spam_stop),
                             daemon=True)
        t.start()
        log(session, 'SPAM started (every %d-%d ms)' % (
            int(opts.spam_delay * 1000),
            int(getattr(opts, 'spam_delay_max', 0.5) * 1000)))

    def stop_spam():
        if session.spam_stop is not None and not session.spam_stop.is_set():
            session.spam_stop.set()
            log(session, 'SPAM stopped')

    def on_message(user_id, q, message_bytes):
        try:
            data = json.loads(message_bytes.decode('utf-8', 'replace'))
        except ValueError:
            return
        if not isinstance(data, dict):
            return
        sender = data.get('userId')
        text = data.get('message', '') or ''
        if opts.debug_frames:
            log(session, 'recv from %s: %r' % (sender, text))
        if str(sender) == str(session.cid):
            return
        if watch and str(sender) != watch:
            return
        if opts.spam:
            if stop_re and stop_re.search(text):
                stop_spam()
                return
            if trigger_re.search(text):
                start_spam()
            return
        if not trigger_re.search(text):
            return
        payload = json.dumps({
            'userId': session.cid,
            'chatId': session.chat_id,
            'message': opts.message,
            'to': 0,
        }).encode('utf-8')
        try:
            session.imq.send_chat(queue, payload)
            session.sent += 1
            log(session, 'heard %r from %s -> said %r'
                % (text, sender, opts.message))
        except Exception as e:
            log(session, 'failed to respond: %s' % e)

    return on_message


def chat_queue_name(chat_id):
    queue = '/chat/%s' % chat_id
    try:
        queue = '/chat/%d' % int(chat_id)
    except (TypeError, ValueError):
        pass
    return queue


def pick_word(opts):
    words = getattr(opts, 'spam_words', None)
    if words:
        return random.choice(words)
    return opts.message


class JoinPool(object):
    """First account in the room invites the rest so they skip 'full' / expired."""

    def __init__(self):
        self.lock = threading.Lock()
        self.host = None
        self.chat_id = None
        self.in_room = []
        self.host_ready = threading.Event()
        self.join_lock = threading.Lock()

    def claim_host(self, session):
        with self.lock:
            if self.host is None:
                self.host = session
                return True
            return False

    def mark_joined(self, session):
        with self.lock:
            if session not in self.in_room:
                self.in_room.append(session)
            self.chat_id = session.chat_id
            if self.host is None:
                self.host = session
        self.host_ready.set()

    def mark_left(self, session):
        with self.lock:
            if session in self.in_room:
                self.in_room.remove(session)
            if self.host is session:
                self.host = self.in_room[0] if self.in_room else None
            if not self.in_room:
                self.chat_id = None
                self.host_ready.clear()

    def mark_join_failed(self, session):
        with self.lock:
            if self.host is session and session not in self.in_room:
                self.host = None

    def inviter(self, exclude):
        with self.lock:
            for session in self.in_room:
                if session is exclude:
                    continue
                if session.imq and not session.imq.closed_by_server:
                    return session
            return None


def resolve_invite(session, opts, stop_event):
    """Wait until a host is in the room, then join that same live chatId.

    chat.php has no invite method; the real client accepts an invite by calling
    getOrMakeChat with the existing chat id (same as --chat-id --register).
    """
    pool = getattr(opts, 'pool', None)
    if not pool or getattr(opts, 'no_invite', False):
        return None
    while not (stop_event and stop_event.is_set()):
        if pool.claim_host(session):
            log(session, 'joining as host (public room)')
            return None
        if pool.host_ready.wait(timeout=5):
            inviter = pool.inviter(session)
            if inviter and inviter.chat_id:
                log(session, 'joining via invite from %s (chat %s)'
                    % (inviter.username, inviter.chat_id))
                return (inviter.chat_id, inviter.cid)
        # host may have failed; loop and try to claim
    raise BackendError('stopped before join')


def join_room(session, info, opts, stats_lock, stop_event=None):
    """Register in the chat, connect IMQ, subscribe, announce seat. Returns queue."""
    invite = None
    pool = getattr(opts, 'pool', None)
    try:
        invite = resolve_invite(session, opts, stop_event)
        if invite and pool:
            join_lock = pool.join_lock
        else:
            join_lock = threading.Lock()
        with join_lock:
            last_err = None
            attempts = 3 if invite else 1
            for attempt in range(attempts):
                try:
                    session.chat_id = get_or_make_chat(
                        session, info, opts, invite=invite)
                    last_err = None
                    break
                except BackendError as e:
                    last_err = e
                    err = str(e)
                    retryable = invite and attempt + 1 < attempts and (
                        '1012' in err or 'Invite expired' in err)
                    if retryable:
                        log(session, '%s — retry %d/%d same chat'
                            % (err, attempt + 2, attempts))
                        time.sleep(random.uniform(0.3, 0.51))
                        continue
                    raise
            if last_err:
                raise last_err
            log(session, 'chatId %s' % (session.chat_id,))

            imq_host = opts.imq_host or info.get('imq_gateway_secure_host')
            if not imq_host:
                raise BackendError('no IMQ host: pass --imq-host or use a login '
                                   'response with imq_gateway_secure_host')
            cookie = info.get('imq_cookie', '')
            token = info.get('imq_auth_token', '')
            if isinstance(cookie, str):
                cookie = cookie.encode('utf-8')
            if isinstance(token, str):
                token = token.encode('utf-8')

            t0 = time.time()
            session.imq = ImqClient(imq_host, opts.imq_port, not opts.imq_plain,
                                    opts.insecure)
            session.imq.debug_frames = opts.debug_frames
            session.imq.connect(str(session.cid), cookie, token)
            session.imq_ms = (time.time() - t0) * 1000
            log(session, 'IMQ authenticated (%d ms)' % session.imq_ms)

            user_queue = '/user/%s' % session.cid
            session.imq.subscribe(user_queue)
            log(session, 'subscribed %s' % user_queue)

            queue = chat_queue_name(session.chat_id)
            session.imq.subscribe(queue)
            log(session, 'subscribed %s' % queue)

            with stats_lock:
                opts.stats['joined'] += 1

            if session.seat not in (None, ''):
                seat_payload = json.dumps({
                    'userId': session.cid,
                    'chatId': session.chat_id,
                    'message': '*seat %s' % session.seat,
                    'to': 0,
                }).encode('utf-8')
                session.imq.send_chat(queue, seat_payload)
                log(session, 'sent seat announcement (seat %s)' % session.seat)
            if pool:
                pool.mark_joined(session)
            return queue
    except Exception:
        if pool:
            pool.mark_join_failed(session)
        raise


def leave_room(session, opts, stats_lock):
    pool = getattr(opts, 'pool', None)
    if session.spam_stop is not None:
        session.spam_stop.set()
    if not session.imq:
        if pool:
            pool.mark_left(session)
        return
    session.echoes += session.imq.echoes
    session.pongs += session.imq.pongs
    session.imq.close()
    session.imq = None
    if pool:
        pool.mark_left(session)
    with stats_lock:
        opts.stats['left'] += 1
    log(session, 'left room')


def send_words(session, opts, queue, count, stop_event, stats_lock):
    for i in range(count):
        if stop_event.is_set() or not session.imq or session.imq.closed_by_server:
            break
        word = pick_word(opts)
        payload = json.dumps({
            'userId': session.cid,
            'chatId': session.chat_id,
            'message': word,
            'to': 0,
        }).encode('utf-8')
        session.imq.send_chat(queue, payload)
        session.sent += 1
        with stats_lock:
            opts.stats['sent'] += 1
        log(session, 'sent %d/%d: %s' % (i + 1, count, word))
        if i + 1 < count:
            time.sleep(random.uniform(0.3, 0.51))


def run_churn(session, info, opts, stop_event, stats_lock):
    """Join -> send N words -> leave, then repeat until stop."""
    while not stop_event.is_set():
        try:
            queue = join_room(session, info, opts, stats_lock, stop_event)
            send_words(session, opts, queue, opts.repeat, stop_event, stats_lock)
            if opts.hold > 0 and session.imq and not stop_event.is_set():
                session.imq.run_until(stop_event, time.time() + opts.hold)
        except (BackendError, socket.error, ssl.SSLError) as e:
            session.error = str(e)
            log(session, 'ERROR: %s' % e)
            with stats_lock:
                opts.stats['errors'] += 1
        leave_room(session, opts, stats_lock)
        session.cycles += 1
        with stats_lock:
            opts.stats['cycles'] += 1
        log(session, 'cycle %d done' % session.cycles)
        if stop_event.wait(opts.churn_delay):
            break


def run_account(session, opts, stop_event, stats_lock):
    if opts.listen_invite:
        run_listen_invite(session, opts, stop_event, stats_lock)
        return
    try:
        t0 = time.time()
        info = login(session, opts)
        session.info = info
        session.login_ms = (time.time() - t0) * 1000
        session.cid = detect_key(info, CID_KEYS, opts.cid_key, 'customer id')
        log(session, 'logged in as cid %s (%d ms)' % (session.cid, session.login_ms))

        if opts.churn and not opts.trigger:
            run_churn(session, info, opts, stop_event, stats_lock)
            return

        queue = join_room(session, info, opts, stats_lock, stop_event)

        if opts.trigger:
            if opts.spam and not getattr(opts, 'spam_words', None):
                opts.spam_words = load_wordlist(opts.wordlist)
            session.imq.on_message = make_trigger_handler(session, opts, queue)
            log(session, 'listening for %r from %s' % (
                opts.trigger, opts.trigger_from or 'anyone'))
        send_words(session, opts, queue,
                   0 if opts.trigger else opts.repeat,
                   stop_event, stats_lock)

        deadline = (time.time() + opts.hold) if opts.hold > 0 else None
        session.imq.run_until(stop_event, deadline)

    except (BackendError, socket.error, ssl.SSLError) as e:
        session.error = str(e)
        log(session, 'ERROR: %s' % e)
        with stats_lock:
            opts.stats['errors'] += 1
    finally:
        if session.spam_stop is not None:
            session.spam_stop.set()
        if session.imq:
            session.echoes += session.imq.echoes
            session.pongs += session.imq.pongs
            session.imq.close()
            session.imq = None


_log_hook = None


def log(session, text):
    line = '[%s] %s' % (session.username, text)
    print(line, flush=True)
    hook = _log_hook
    if hook:
        try:
            hook(line)
        except Exception:
            pass


# --- main --------------------------------------------------------------------

def main(argv=None):
    here = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Fill a chat room with test accounts (own backend only).')
    parser.add_argument('--accounts', default=os.path.join(here, 'accounts.txt'),
                        help='user:password per line')
    parser.add_argument('--room',
                        help='public room instance id to join')
    parser.add_argument('--find-room', metavar='KEYWORDS',
                        help='search public rooms by name, print their '
                             'room instance ids, and exit')
    parser.add_argument('--probe-chat', metavar='CHAT_ID',
                        help='call chat.getParticipants for a chat id, print '
                             'the raw response, and exit')
    parser.add_argument('--chat-id',
                        help='join this chat queue directly, skipping '
                             'getOrMakeChat (the chat id shown in the '
                             'client Session panel)')
    parser.add_argument('--register', action='store_true',
                        help='with --chat-id: call getOrMakeChat first to register')
    parser.add_argument('--count', type=int, default=0,
                        help='max accounts to use (default: all)')
    parser.add_argument('--listen-invite', action='store_true',
                        help='listen mode: log in, wait for a chat invitation '
                             'on the user queue, auto-accept and join')
    parser.add_argument('--message', default='hola')
    parser.add_argument('--spam', action='store_true',
                        help='spam mode: trigger word starts random-word spam, '
                             '--stop-trigger stops it')
    parser.add_argument('--stop-trigger', default='st',
                        help='word that stops the spam (spam mode)')
    parser.add_argument('--spam-delay', type=float, default=0.3,
                        help='min seconds between spam messages (default 0.3)')
    parser.add_argument('--spam-delay-max', type=float, default=0.5,
                        help='max seconds between spam messages (default 0.5)')
    parser.add_argument('--wordlist',
                        default=os.path.join(here, 'wordlist.txt'),
                        help='word list file for --spam (one word per line)')
    parser.add_argument('--trigger',
                        help='listen mode: say --message when this word '
                             'appears in chat (instead of sending on join)')
    parser.add_argument('--trigger-from',
                        help='only react to this sender cid (e.g. your main '
                             'account)')
    parser.add_argument('--repeat', type=int, default=10,
                        help='messages (words) per account per visit (default 10)')
    parser.add_argument('--delay', type=float, default=0,
                        help='seconds between repeated messages')
    parser.add_argument('--ramp', type=float, default=0,
                        help='seconds between account starts')
    parser.add_argument('--hold', type=float, default=0,
                        help='seconds to keep sessions after sending '
                             '(default: until Ctrl+C; with --churn, '
                             'seconds in room before leaving)')
    parser.add_argument('--churn', action='store_true',
                        help='loop: join room, send --repeat words, leave, '
                             'repeat until Stop/Ctrl+C')
    parser.add_argument('--churn-delay', type=float, default=1.0,
                        help='seconds to wait after leaving before rejoining '
                             '(default 1)')
    parser.add_argument('--gui', action='store_true',
                        help='open a window with a Go button')
    parser.add_argument('--no-invite', action='store_true',
                        help='do not have agents invite each other; each '
                             'joins the public room on its own')

    parser.add_argument('--host', help='shortcut: same host for both endpoints')
    parser.add_argument('--secure-host', default='secure.imvu.com')
    parser.add_argument('--chat-host', default='chat.imvu.com')
    parser.add_argument('--service-host', default='client-dynamic.imvu.com',
                        help='host for the room list API')
    parser.add_argument('--service-scheme', default='http',
                        choices=('http', 'https'))
    parser.add_argument('--secure-scheme', default='https', choices=('http', 'https'))
    parser.add_argument('--chat-scheme', default='http', choices=('http', 'https'))
    parser.add_argument('--client-endpoint', default='/api/xmlrpc/client.php')
    parser.add_argument('--chat-endpoint', default='/api/xmlrpc/chat.php')
    parser.add_argument('--imq-host', help='default: imq_gateway_secure_host '
                                           'from the login response')
    parser.add_argument('--imq-port', type=int, default=443)
    parser.add_argument('--imq-plain', action='store_true',
                        help='IMQ over plain TCP instead of TLS')
    parser.add_argument('--insecure', action='store_true',
                        help='do not verify TLS certificates')

    parser.add_argument('--client-version', default='554.0')
    parser.add_argument('--cid-key', help='customer id key in login response')
    parser.add_argument('--chatid-key', help='chat id key in getOrMakeChat response')
    parser.add_argument('--print-userinfo', action='store_true',
                        help='log the login response keys (key mapping debug)')
    parser.add_argument('--debug-frames', action='store_true',
                        help='log every IMQ frame received while subscribing')

    opts = parser.parse_args(argv)
    if opts.host:
        opts.secure_host = opts.chat_host = opts.service_host = opts.host
    if not opts.room and not opts.chat_id and not opts.find_room \
            and not opts.probe_chat and not opts.listen_invite:
        opts.gui = True
    if opts.gui and not opts.find_room and not opts.probe_chat:
        return run_gui(opts)

    return run_load(opts)


def prepare_run(opts):
    opts.stats = {'joined': 0, 'sent': 0, 'left': 0, 'cycles': 0, 'errors': 0}
    stats_lock = threading.Lock()
    opts.stats_lock = stats_lock
    stop_event = threading.Event()
    opts.stop_event = stop_event
    opts.pool = JoinPool()
    opts.ready_cids = []
    if opts.churn:
        opts.register = True
        if not getattr(opts, 'spam_words', None):
            try:
                opts.spam_words = load_wordlist(opts.wordlist)
            except Exception:
                opts.spam_words = None
    return stats_lock, stop_event


def run_load(opts):
    stats_lock, stop_event = prepare_run(opts)

    accounts = load_accounts(opts.accounts, opts.count)
    opts.total_accounts = len(accounts)
    if not accounts:
        print('no accounts in %s' % opts.accounts)
        return 1

    if opts.probe_chat:
        session = accounts[0]
        info = login(session, opts)
        session.cid = detect_key(info, CID_KEYS, opts.cid_key, 'customer id')
        result = probe_chat(session, info, opts, opts.probe_chat)
        print('getParticipants %s ->' % opts.probe_chat)
        print(json.dumps(result, indent=2, default=str))
        return 0

    if opts.find_room:
        session = accounts[0]
        info = login(session, opts)
        session.cid = detect_key(info, CID_KEYS, opts.cid_key, 'customer id')
        rooms = find_rooms(session, info, opts, opts.find_room)
        if not rooms:
            print('no rooms matching %r' % opts.find_room)
            return 1
        print('rooms matching %r:' % opts.find_room)
        for room in rooms:
            print('  %s  %s  (owner %s, %s in room)'
                  % (room['roomInstanceId'], room['name'],
                     room['owner'], room['participants']))
        print('\njoin one with: --room <roomInstanceId>')
        return 0

    if not opts.room and not opts.chat_id and not opts.listen_invite:
        print('pass --room <roomInstanceId> or --chat-id <chatId>')
        return 1

    if opts.listen_invite:
        target = 'listen-invite (waiting for invitations)'
    else:
        target = opts.room or ('chat ' + str(opts.chat_id))
    mode = 'churn join/leave + %d words' % opts.repeat if opts.churn else (
        'message %r x%d' % (opts.message, opts.repeat))
    print('roomload: %d accounts -> %s, %s'
          % (len(accounts), target, mode), flush=True)

    threads = []
    started = time.time()
    try:
        for session in accounts:
            thread = threading.Thread(
                target=run_account,
                args=(session, opts, stop_event, stats_lock),
                daemon=True,
            )
            thread.start()
            threads.append(thread)
            if opts.ramp:
                time.sleep(opts.ramp)
        while any(t.is_alive() for t in threads):
            time.sleep(random.uniform(0.3, 0.51))
            if stop_event.is_set():
                break
        if stop_event.is_set():
            for t in threads:
                t.join(timeout=10)
    except KeyboardInterrupt:
        print('\nstopping...', flush=True)
        stop_event.set()
        for t in threads:
            t.join(timeout=10)

    elapsed = time.time() - started
    ok = [s for s in accounts if s.error is None]
    failed = [s for s in accounts if s.error is not None]
    echoes = sum(s.echoes for s in accounts)
    pongs = sum(s.pongs for s in accounts)

    print('\n===== summary =====')
    print('elapsed:           %.1f s' % elapsed)
    print('accounts:          %d' % len(accounts))
    print('joined room queue: %d' % opts.stats['joined'])
    print('left room:         %d' % opts.stats['left'])
    print('cycles:            %d' % opts.stats['cycles'])
    print('messages sent:     %d' % opts.stats['sent'])
    print('message echoes:    %d' % echoes)
    print('pongs:             %d' % pongs)
    print('errors:            %d' % opts.stats['errors'])
    for s in failed:
        print('  FAIL %s: %s' % (s.username, s.error))
    if ok:
        logins = [s.login_ms for s in ok if s.login_ms]
        imqs = [s.imq_ms for s in ok if s.imq_ms]
        if logins:
            print('login ms min/avg/max: %.0f/%.0f/%.0f'
                  % (min(logins), sum(logins) / len(logins), max(logins)))
        if imqs:
            print('imq auth ms min/avg/max: %.0f/%.0f/%.0f'
                  % (min(imqs), sum(imqs) / len(imqs), max(imqs)))
    return 0 if not failed else 2


def run_gui(opts):
    try:
        import tkinter as tk
        from tkinter import scrolledtext, messagebox
    except ImportError:
        print('tkinter is not available; run with --room/--chat-id --churn')
        return 1

    root = tk.Tk()
    root.title('roomload')
    root.geometry('720x520')

    running = {'thread': None}

    frm = tk.Frame(root, padx=10, pady=8)
    frm.pack(fill='x')

    tk.Label(frm, text='Room instance id').grid(row=0, column=0, sticky='w')
    room_var = tk.StringVar(value=opts.room or '')
    tk.Entry(frm, textvariable=room_var, width=40).grid(row=0, column=1, sticky='we', padx=6)

    tk.Label(frm, text='Chat id (optional)').grid(row=1, column=0, sticky='w')
    chat_var = tk.StringVar(value=opts.chat_id or '')
    tk.Entry(frm, textvariable=chat_var, width=40).grid(row=1, column=1, sticky='we', padx=6)

    tk.Label(frm, text='Accounts (0 = all)').grid(row=2, column=0, sticky='w')
    count_var = tk.StringVar(value=str(opts.count or 0))
    tk.Entry(frm, textvariable=count_var, width=10).grid(row=2, column=1, sticky='w', padx=6)

    tk.Label(frm, text='Words per visit').grid(row=3, column=0, sticky='w')
    words_var = tk.StringVar(value=str(opts.repeat or 10))
    tk.Entry(frm, textvariable=words_var, width=10).grid(row=3, column=1, sticky='w', padx=6)

    insecure_var = tk.BooleanVar(value=bool(opts.insecure))
    tk.Checkbutton(frm, text='Skip TLS verify', variable=insecure_var).grid(
        row=4, column=1, sticky='w', padx=6)

    btns = tk.Frame(root, padx=10)
    btns.pack(fill='x')
    go_btn = tk.Button(btns, text='Go', width=10)
    stop_btn = tk.Button(btns, text='Stop', width=10, state='disabled')
    go_btn.pack(side='left', padx=(0, 6), pady=4)
    stop_btn.pack(side='left', pady=4)
    status_var = tk.StringVar(value='join/leave loop + 10 words per connected user')
    tk.Label(btns, textvariable=status_var).pack(side='left', padx=12)

    log_box = scrolledtext.ScrolledText(root, height=22, wrap='word', state='disabled')
    log_box.pack(fill='both', expand=True, padx=10, pady=(4, 10))

    pending_lines = []
    pending_lock = threading.Lock()

    def append_log(line):
        with pending_lock:
            pending_lines.append(line)

    def flush_log():
        with pending_lock:
            lines = pending_lines[:]
            del pending_lines[:]
        if lines:
            log_box.configure(state='normal')
            for line in lines:
                log_box.insert('end', line + '\n')
            log_box.see('end')
            log_box.configure(state='disabled')
        root.after(200, flush_log)

    def on_stop():
        if getattr(opts, 'stop_event', None):
            opts.stop_event.set()
        status_var.set('stopping...')
        stop_btn.configure(state='disabled')

    def worker():
        try:
            run_load(opts)
        finally:
            def done():
                global _log_hook
                _log_hook = None
                running['thread'] = None
                try:
                    go_btn.configure(state='normal')
                    stop_btn.configure(state='disabled')
                    status_var.set('stopped')
                except Exception:
                    pass
            try:
                root.after(0, done)
            except Exception:
                pass

    def on_go():
        global _log_hook
        if running['thread']:
            return
        room = room_var.get().strip()
        chat_id = chat_var.get().strip()
        if not room and not chat_id:
            messagebox.showerror('roomload', 'Enter a room instance id or a chat id')
            return
        try:
            count = int(count_var.get().strip() or '0')
            words = int(words_var.get().strip() or '10')
        except ValueError:
            messagebox.showerror('roomload', 'Count and words must be integers')
            return
        opts.room = room or None
        opts.chat_id = chat_id or None
        opts.count = count
        opts.repeat = words if words > 0 else 10
        opts.insecure = insecure_var.get()
        opts.churn = True
        opts.trigger = None
        if not opts.ramp:
            opts.ramp = 1
        _log_hook = append_log
        go_btn.configure(state='disabled')
        stop_btn.configure(state='normal')
        status_var.set('running: join -> %d words -> leave -> repeat' % opts.repeat)
        t = threading.Thread(target=worker, daemon=True)
        running['thread'] = t
        t.start()

    go_btn.configure(command=on_go)
    stop_btn.configure(command=on_stop)
    root.protocol('WM_DELETE_WINDOW', lambda: (on_stop(), root.destroy()))
    flush_log()
    root.mainloop()
    return 0


def accept_invite(session, opts, chat_id, invite_id, location):
    """Accept a chat invitation. The real client calls chat.acceptInvite; some
    servers lack it, so we also register via getOrMakeChat using the room
    activity taken from the invitation location (works for private rooms)."""
    url = chat_url(opts)
    auth = chat_auth(session, session.info)
    args = {'userId': session.cid, 'inviteId': invite_id, 'chatId': chat_id}
    try:
        return xmlrpc_call(url, 'chat.acceptInvite', (args,), auth=auth,
                           insecure=opts.insecure)
    except BackendError:
        pass
    args2 = {'userId': session.cid, 'version': opts.client_version,
             'publicroom': True, 'private': True}
    if chat_id:
        args2['chatId'] = int(chat_id)
    room = room_from_location(location)
    if room:
        args2['activity'] = 'publicroom-%s' % room
        args2['private'] = False
    return xmlrpc_call(url, 'chat.getOrMakeChat', (args2,), auth=auth,
                       insecure=opts.insecure)


def room_from_location(location):
    """Extract a roomInstanceId from an invitation location dict."""
    if not isinstance(location, dict):
        return None
    for key in ('roomInstanceId', 'room_instance_id', 'roomId', 'room_id',
                'instanceId', 'id'):
        val = location.get(key)
        if val:
            return val
    owner = location.get('ownerId')
    room_id = location.get('customers_room_id') or location.get('roomId')
    if owner and room_id:
        return '%s-%s' % (owner, room_id)
    return None


def make_invite_handler(session, opts):
    def on_message(user_id, q, message_bytes):
        text = message_bytes.decode('utf-8', 'replace')
        if opts.debug_frames:
            log(session, 'user-queue msg on %s: %r' % (q, text[:200]))
        parts = text.split(' ', 1)
        if len(parts) != 2:
            return
        token, payload = parts
        if 'chatinvite' not in token.lower():
            return
        try:
            info = json.loads(payload)
        except ValueError:
            return
        if not isinstance(info, dict):
            return
        chat_id = info.get('chatId')
        invite_id = info.get('inviteId')
        location = info.get('location')
        if not chat_id or not invite_id:
            return
        log(session, 'invited to chat %s by %s, accepting'
            % (chat_id, info.get('inviter')))
        try:
            accept_invite(session, opts, chat_id, invite_id, location)
        except Exception as e:
            log(session, 'accept failed: %s' % e)
            return
        session.chat_id = chat_id
        queue = '/chat/%s' % chat_id
        try:
            session.imq.subscribe(queue)
        except Exception as e:
            log(session, 'subscribe after accept failed: %s' % e)
            return
        with opts.stats_lock:
            opts.stats['joined'] += 1
        log(session, 'accepted and joined %s' % queue)
        if opts.trigger:
            if opts.spam and not getattr(opts, 'spam_words', None):
                opts.spam_words = load_wordlist(opts.wordlist)
            session.imq.on_message = make_trigger_handler(session, opts, queue)
            log(session, 'in room; listening for trigger %r' % opts.trigger)
    return on_message


def run_listen_invite(session, opts, stop_event, stats_lock):
    try:
        t0 = time.time()
        info = login(session, opts)
        session.info = info
        session.login_ms = (time.time() - t0) * 1000
        session.cid = detect_key(info, CID_KEYS, opts.cid_key, 'customer id')
        log(session, 'ready as cid %s, waiting for invite' % session.cid)
        with stats_lock:
            opts.stats['ready'] = opts.stats.get('ready', 0) + 1
            opts.ready_cids.append(str(session.cid))
            print('INVITE CIDS: ' + ','.join(opts.ready_cids), flush=True)
            if len(opts.ready_cids) == getattr(opts, 'total_accounts', 0):
                print('ALL READY - PASTE THIS IN THE PANEL: '
                      + ','.join(opts.ready_cids), flush=True)

        imq_host = opts.imq_host or info.get('imq_gateway_secure_host')
        if not imq_host:
            raise BackendError('no IMQ host: pass --imq-host')
        cookie = info.get('imq_cookie', '')
        token = info.get('imq_auth_token', '')
        if isinstance(cookie, str):
            cookie = cookie.encode('utf-8')
        if isinstance(token, str):
            token = token.encode('utf-8')

        session.imq = ImqClient(imq_host, opts.imq_port, not opts.imq_plain,
                                opts.insecure)
        session.imq.debug_frames = opts.debug_frames
        session.imq.connect(str(session.cid), cookie, token)
        user_queue = '/user/%s' % session.cid
        session.imq.subscribe(user_queue)
        session.imq.on_message = make_invite_handler(session, opts)
        log(session, 'listening for invites on %s' % user_queue)
        deadline = (time.time() + opts.hold) if opts.hold > 0 else None
        session.imq.run_until(stop_event, deadline)
    except (BackendError, socket.error, ssl.SSLError) as e:
        session.error = str(e)
        log(session, 'ERROR: %s' % e)
        with stats_lock:
            opts.stats['errors'] += 1
    finally:
        if session.imq:
            session.imq.close()


if __name__ == '__main__':
    sys.exit(main())
