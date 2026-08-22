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
import http.client
import json
import os
import re
import random
import socket
import ssl
import struct
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


DEFAULT_PROXY_API = (
    'https://api.proxyscrape.com/v2/?request=displayproxies'
    '&protocol=socks5&timeout=15000&country=all'
)
PROXY_EXTRA_APIS = (
    'https://api.proxyscrape.com/v2/?request=displayproxies'
    '&protocol=http&timeout=15000&country=all',
    'https://proxylist.geonode.com/api/proxy-list?protocols=socks5'
    '&limit=500&sort_by=lastChecked&sort_type=desc',
    'https://www.proxy-list.download/api/v1/get?type=socks5',
    'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt',
    'https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt',
    'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt',
)
# IMVU drops extra clients from the same public IP after this many.
PROXY_PER_IP = 8
PROXY_RETRY = 6
PROBE_XML = (
    b'<?xml version="1.0"?>\n'
    b'<methodCall><methodName>system.listMethods</methodName>'
    b'<params></params></methodCall>'
)


class ProxySpec(object):
    def __init__(self, host, port, kind='socks5'):
        self.host = host
        self.port = int(port)
        self.kind = kind or 'socks5'

    def key(self):
        return '%s:%s' % (self.host, self.port)


def proxy_ip(session):
    px = getattr(session, 'proxy', None)
    return px.host if px else ''


def parse_proxy_line(line):
    line = (line or '').strip()
    if not line or line.startswith('#'):
        return None
    kind = 'socks5'
    text = line
    if '://' in text:
        scheme, text = text.split('://', 1)
        scheme = scheme.lower()
        if scheme in ('http', 'https'):
            kind = 'http'
        elif 'socks' in scheme:
            kind = 'socks5'
        if '@' in text:
            text = text.split('@', 1)[1]
    text = text.split('/')[0].strip()
    if text.count(':') < 1:
        return None
    host, port = text.rsplit(':', 1)
    host = host.strip().strip('[]')
    try:
        port = int(port)
    except ValueError:
        return None
    if not host or port <= 0:
        return None
    return ProxySpec(host, port, kind)


def parse_proxy_text(text):
    found = []
    seen = set()
    for line in (text or '').splitlines():
        spec = parse_proxy_line(line)
        if spec is None or spec.key() in seen:
            continue
        seen.add(spec.key())
        found.append(spec)
    return found


def _json_proxy_kind(value):
    if isinstance(value, (list, tuple)):
        parts = [str(v).lower() for v in value]
        if any('socks' in p for p in parts):
            return 'socks5'
        if any(p.startswith('http') for p in parts):
            return 'http'
        return 'socks5'
    text = str(value or '').lower()
    if 'socks' in text:
        return 'socks5'
    if 'http' in text:
        return 'http'
    return 'socks5'


def spec_from_json_row(row):
    if isinstance(row, str):
        return parse_proxy_line(row)
    if not isinstance(row, dict):
        return None
    host = row.get('ip') or row.get('host') or row.get('ipAddress')
    port = row.get('port')
    if host and port:
        kind = _json_proxy_kind(
            row.get('protocols') or row.get('protocol') or row.get('type'))
        try:
            return ProxySpec(str(host), int(port), kind)
        except (TypeError, ValueError):
            return None
    return parse_proxy_line(row.get('proxy') or row.get('addr') or '')


def parse_proxy_json(data):
    rows = []
    if isinstance(data, dict):
        for key in ('data', 'proxies', 'list', 'result'):
            if isinstance(data.get(key), list):
                rows = data[key]
                break
        else:
            rows = [data]
    elif isinstance(data, list):
        rows = data
    found = []
    seen = set()
    for row in rows:
        spec = spec_from_json_row(row)
        if spec is None or spec.key() in seen:
            continue
        seen.add(spec.key())
        found.append(spec)
    return found


def parse_proxy_payload(text):
    text = (text or '').strip()
    if not text:
        return []
    if text[:1] in '{[':
        try:
            return parse_proxy_json(json.loads(text))
        except ValueError:
            pass
    return parse_proxy_text(text)


def load_proxies_file(path):
    if not path or not os.path.isfile(path):
        return []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return parse_proxy_payload(f.read())
    except Exception:
        return []


def fetch_proxy_api(url, timeout=20):
    if not url:
        return []
    req = urllib.request.Request(
        url, headers={'User-Agent': 'Mozilla/5.0 (compatible; roomload)'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return parse_proxy_payload(resp.read().decode('utf-8', 'replace'))
    except Exception as e:
        raise BackendError('proxy api failed: %s' % e)


def proxies_needed(n_accounts):
    n = max(1, int(n_accounts or 1))
    return (n + PROXY_PER_IP - 1) // PROXY_PER_IP


def proxy_api_urls(opts):
    api = (getattr(opts, 'proxy_api', None) or '').strip()
    urls = []
    if api and api != DEFAULT_PROXY_API:
        urls.append(api)
    urls.append(DEFAULT_PROXY_API)
    for extra in PROXY_EXTRA_APIS:
        if extra not in urls:
            urls.append(extra)
    return urls


def collect_proxies_from_apis(opts, quiet=False):
    found = []
    for url in proxy_api_urls(opts):
        try:
            found.extend(fetch_proxy_api(url))
        except Exception as e:
            if not quiet:
                _proxy_note(str(e))
    return found


def _recvn(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise BackendError('proxy closed')
        buf.extend(chunk)
    return bytes(buf)


def socks5_connect(sock, dest_host, dest_port):
    sock.sendall(b'\x05\x01\x00')
    hello = _recvn(sock, 2)
    if len(hello) < 2 or hello[0] != 5 or hello[1] != 0:
        raise BackendError('socks5 auth rejected')
    host_b = dest_host.encode('ascii')
    req = (b'\x05\x01\x00\x03' + bytes([len(host_b)]) + host_b
           + struct.pack('!H', int(dest_port)))
    sock.sendall(req)
    hdr = _recvn(sock, 4)
    if hdr[1] != 0:
        raise BackendError('socks5 connect failed %d' % hdr[1])
    atyp = hdr[3]
    if atyp == 1:
        _recvn(sock, 6)
    elif atyp == 3:
        ln = _recvn(sock, 1)[0]
        _recvn(sock, ln + 2)
    elif atyp == 4:
        _recvn(sock, 18)
    else:
        raise BackendError('socks5 bad atyp %d' % atyp)


def http_connect(sock, dest_host, dest_port):
    msg = ('CONNECT %s:%d HTTP/1.1\r\nHost: %s:%d\r\n\r\n'
           % (dest_host, dest_port, dest_host, dest_port))
    sock.sendall(msg.encode('ascii'))
    buf = b''
    while b'\r\n\r\n' not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise BackendError('proxy CONNECT closed')
        buf += chunk
        if len(buf) > 8192:
            raise BackendError('proxy CONNECT too long')
    status = buf.split(b'\r\n', 1)[0].decode('latin-1', 'replace')
    if ' 200 ' not in status and not status.endswith(' 200'):
        raise BackendError('proxy CONNECT %s' % status)


def open_via_proxy(dest_host, dest_port, proxy, timeout=8.0):
    raw = socket.create_connection((proxy.host, proxy.port), timeout=timeout)
    raw.settimeout(timeout)
    try:
        if proxy.kind == 'http':
            http_connect(raw, dest_host, dest_port)
        else:
            socks5_connect(raw, dest_host, dest_port)
    except Exception:
        try:
            raw.close()
        except Exception:
            pass
        raise
    return raw


def _http_status(chunk):
    line = (chunk or b'').split(b'\r\n', 1)[0]
    parts = line.split()
    if len(parts) >= 2 and parts[1].isdigit():
        return int(parts[1])
    return 0


def probe_proxy_tls(proxy, dest_host, dest_port, timeout=5.0):
    sock = None
    try:
        sock = open_via_proxy(dest_host, dest_port, proxy, timeout)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        tls = ctx.wrap_socket(sock, server_hostname=dest_host)
        sock = tls
        return True
    except Exception:
        return False
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass


def probe_proxy(proxy, dest_host, dest_port, timeout=5.0,
                path='/api/xmlrpc/client.php'):
    """SOCKS/CONNECT + TLS + real POST to IMVU. HEAD-only lets junk through."""
    sock = None
    try:
        sock = open_via_proxy(dest_host, dest_port, proxy, timeout)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        tls = ctx.wrap_socket(sock, server_hostname=dest_host)
        sock = tls
        req = (
            'POST %s HTTP/1.0\r\n'
            'Host: %s\r\n'
            'Content-Type: text/xml\r\n'
            'Content-Length: %d\r\n'
            'User-Agent: IMVU Client\r\n'
            '\r\n'
        ) % (path, dest_host, len(PROBE_XML))
        tls.sendall(req.encode('ascii') + PROBE_XML)
        chunk = b''
        deadline = time.time() + timeout
        while len(chunk) < 48 and time.time() < deadline:
            tls.settimeout(max(0.2, deadline - time.time()))
            part = tls.recv(256)
            if not part:
                break
            chunk += part
            if b'\r\n' in chunk:
                break
        status = _http_status(chunk)
        if status in (502, 503, 504, 407, 0):
            return False
        return 200 <= status < 500
    except Exception:
        return False
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass


def probe_proxy_imvu(proxy, login_host, login_path, imq_host=None,
                     timeout=5.0):
    if not probe_proxy(proxy, login_host, 443, timeout, path=login_path):
        return False
    if imq_host and imq_host != login_host:
        if not probe_proxy_tls(proxy, imq_host, 443, timeout):
            return False
    return True


def probe_proxies(proxies, want, check, workers=20):
    live = []
    lock = threading.Lock()
    idx = [0]

    def worker():
        while True:
            with lock:
                if idx[0] >= len(proxies) or len(live) >= want:
                    return
                spec = proxies[idx[0]]
                idx[0] += 1
            if check(spec):
                with lock:
                    live.append(spec)

    threads = []
    n = min(workers, max(1, len(proxies)))
    for _ in range(n):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    return live


def proxy_transport_error(err):
    text = str(err).lower()
    if 'fault ' in text or 'xml-rpc fault' in text:
        return False
    if 'no working prox' in text or 'none left' in text:
        return False
    needles = (
        'timed out', 'timeout', 'socks', 'reset', 'eof',
        'broken pipe', 'refused', 'unreachable', 'closed',
        'tunnel', '407', '502', '503', '504', 'cannot reach',
        'socks5', 'proxy connect', 'proxy closed', 'proxy dead',
    )
    for needle in needles:
        if needle in text:
            return True
    return False


class ProxyPool(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.live = []
        self.reserve = []
        self.users = {}
        self.bad = set()
        self.seen = set()
        self.fails = {}
        self.assigned = {}
        self.login_host = 'secure.imvu.com'
        self.login_path = '/api/xmlrpc/client.php'
        self.imq_host = None
        self.probe_host = 'secure.imvu.com'
        self.probe_port = 443
        self.per_ip = PROXY_PER_IP
        self.need = 1
        self.want = 4
        self.n_accounts = 0
        self._stop = None
        self._opts = None
        self._hunter = None

    def live_count(self):
        with self.lock:
            return len([p for p in self.live if p.key() not in self.bad])

    def free_slots(self):
        with self.lock:
            n = 0
            for spec in self.live:
                if spec.key() in self.bad:
                    continue
                used = len(self.users.get(spec.key(), set()))
                n += max(0, self.per_ip - used)
            return n

    def status_line(self):
        live = self.live_count()
        return '%d live · %d/ip · %d slots' % (
            live, self.per_ip, live * self.per_ip)

    def _unassign(self, username):
        cur = self.assigned.pop(username, None)
        if cur is None:
            return cur
        bucket = self.users.get(cur.key())
        if bucket and username in bucket:
            bucket.discard(username)
        return cur

    def _pick(self):
        """Least-loaded live proxy with room (IMVU cap = PROXY_PER_IP)."""
        best = None
        best_n = None
        for spec in self.live:
            if spec.key() in self.bad:
                continue
            n = len(self.users.get(spec.key(), set()))
            if n >= self.per_ip:
                continue
            if best is None or n < best_n:
                best = spec
                best_n = n
                if n == 0:
                    break
        return best

    def claim(self, session):
        with self.lock:
            name = session.username
            cur = self.assigned.get(name)
            if cur and cur.key() not in self.bad:
                used = self.users.get(cur.key(), set())
                if name in used or len(used) < self.per_ip:
                    if name not in used:
                        used = self.users.setdefault(cur.key(), set())
                        used.add(name)
                    session.proxy = cur
                    return cur
            if cur:
                self._unassign(name)
            spec = self._pick()
            if spec is None:
                session.proxy = None
                return None
            self.assigned[name] = spec
            self.users.setdefault(spec.key(), set()).add(name)
            session.proxy = spec
            return spec

    def ensure(self, session):
        return self.claim(session)

    def _check(self, spec):
        return probe_proxy_imvu(
            spec, self.login_host, self.login_path,
            imq_host=self.imq_host)

    def _add_live(self, spec):
        with self.lock:
            if spec.key() in self.bad:
                return False
            keys = set(p.key() for p in self.live)
            if spec.key() in keys:
                return False
            self.live.append(spec)
            self.seen.add(spec.key())
            return True

    def promote(self, count=1):
        added = 0
        while added < count:
            with self.lock:
                spec = None
                while self.reserve:
                    cand = self.reserve.pop(0)
                    if cand.key() not in self.bad:
                        spec = cand
                        break
            if spec is None:
                return added
            if self._check(spec) and self._add_live(spec):
                added += 1
                _proxy_note('proxy + %s (%s)' % (spec.host, self.status_line()))
        return added

    def rotate(self, session):
        with self.lock:
            name = session.username
            old = self._unassign(name)
            if old:
                self.bad.add(old.key())
        nxt = self.claim(session)
        if nxt is None or (old and nxt.key() == old.key()):
            self.promote(2)
            nxt = self.claim(session)
            if old and nxt and nxt.key() == old.key():
                nxt = None
        return old, nxt

    def release(self, session):
        with self.lock:
            self._unassign(session.username)
        session.proxy = None

    def set_imq_host(self, host):
        host = (host or '').strip()
        if not host:
            return
        self.imq_host = host

    def start_hunter(self, opts):
        self._opts = opts
        self._stop = getattr(opts, 'stop_event', None)
        if self._hunter and self._hunter.is_alive():
            return
        t = threading.Thread(target=self._hunt_loop, name='proxy-hunt')
        t.daemon = True
        t.start()
        self._hunter = t

    def _merge_new(self, specs):
        added = 0
        with self.lock:
            live_keys = set(p.key() for p in self.live)
            res_keys = set(p.key() for p in self.reserve)
            for spec in specs:
                key = spec.key()
                if key in self.bad or key in live_keys or key in res_keys:
                    continue
                if key in self.seen and key not in live_keys:
                    continue
                self.reserve.append(spec)
                self.seen.add(key)
                res_keys.add(key)
                added += 1
        return added

    def _fetch_more(self, opts):
        try:
            fresh = collect_proxies_from_apis(opts, quiet=True)
        except Exception:
            return 0
        n = self._merge_new(fresh)
        if n:
            _proxy_note('api +%d reserve' % n)
        return n

    def _hunt_loop(self):
        last_fetch = 0
        while True:
            stop = self._stop
            if stop is not None and stop.is_set():
                return
            live = self.live_count()
            slots = live * self.per_ip
            short = live < self.want or slots < self.n_accounts
            if short:
                if time.time() - last_fetch > 40:
                    if self._opts is not None:
                        self._fetch_more(self._opts)
                    last_fetch = time.time()
                self.promote(3)
            if stop is not None:
                if stop.wait(8.0):
                    return
            else:
                time.sleep(8.0)

    def claim_wait(self, session, stop_event=None, timeout=None):
        stop = stop_event or getattr(session, 'stop_event', None)
        deadline = (time.time() + timeout) if timeout else None
        last_log = 0
        while True:
            spec = self.claim(session)
            if spec:
                return spec
            if self.promote(1):
                continue
            spec = self.claim(session)
            if spec:
                return spec
            if session.halt is not None and session.halt.is_set():
                return None
            if stop is not None and stop.is_set():
                return None
            if deadline is not None and time.time() >= deadline:
                return None
            now = time.time()
            if now - last_log >= 15:
                log(session, 'waiting for ip slot (%d/ip)' % self.per_ip)
                last_log = now
            if stop is not None:
                if stop.wait(3.0):
                    return None
            else:
                time.sleep(3.0)


def _proxy_note(text):
    class _P(object):
        username = 'proxy'
    log(_P(), text)


def prepare_proxies(opts):
    opts.proxy_pool = None
    if not getattr(opts, 'use_proxy', False):
        return 0
    here = os.path.dirname(os.path.abspath(__file__))
    path = getattr(opts, 'proxies_file', None) or os.path.join(here, 'proxies.txt')
    found = load_proxies_file(path)
    found.extend(collect_proxies_from_apis(opts))
    seen = set()
    unique = []
    for spec in found:
        if spec.key() in seen:
            continue
        seen.add(spec.key())
        unique.append(spec)
    file_keys = set(p.key() for p in load_proxies_file(path))
    keep = [p for p in unique if p.key() in file_keys]
    rest = [p for p in unique if p.key() not in file_keys]
    random.shuffle(rest)
    unique = keep + rest
    n_accounts = int(getattr(opts, 'total_accounts', 0)
                     or getattr(opts, 'count', 0) or 8)
    need = proxies_needed(n_accounts)
    want = min(16, max(need + 3, need * 2, 3))
    login_host = getattr(opts, 'secure_host', None) or 'secure.imvu.com'
    login_path = getattr(opts, 'client_endpoint', None) or '/api/xmlrpc/client.php'
    imq_host = getattr(opts, 'imq_host', None) or None
    _proxy_note('need %d ips (%d agents · %d/ip) · hunting %d'
                % (need, n_accounts, PROXY_PER_IP, len(unique)))

    def check(spec):
        return probe_proxy_imvu(spec, login_host, login_path, imq_host=imq_host)

    live = []
    leftover = unique[:]
    checked = 0
    max_check = 280
    while leftover and len(live) < want and checked < max_check:
        batch = leftover[:32]
        leftover = leftover[32:]
        checked += len(batch)
        live.extend(probe_proxies(batch, want - len(live), check))
        if len(live) >= need and checked >= 96 and len(live) >= want:
            break
        if len(live) >= need and checked >= 160:
            break
    if not live:
        raise BackendError('no working proxies from api')
    pool = ProxyPool()
    pool.live = live
    pool.reserve = leftover
    pool.seen = set(p.key() for p in unique)
    pool.login_host = login_host
    pool.login_path = login_path
    pool.imq_host = imq_host
    pool.probe_host = login_host
    pool.probe_port = 443
    pool.need = need
    pool.want = want
    pool.n_accounts = n_accounts
    opts.proxy_pool = pool
    pool.start_hunter(opts)
    _proxy_note('proxies %s · %d reserve · hunting'
                % (pool.status_line(), len(leftover)))
    return len(live)


def run_via_proxy(session, opts, fn):
    pool = getattr(opts, 'proxy_pool', None)
    stop = getattr(opts, 'stop_event', None)
    if pool:
        if not pool.claim_wait(session, stop_event=stop):
            raise BackendError('no working proxies')
    tries = PROXY_RETRY if pool else 1
    last = None
    for _ in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if not pool or not proxy_transport_error(e):
                raise
            old, nxt = pool.rotate(session)
            old_ip = old.host if old else '?'
            if nxt is None:
                if pool.claim_wait(session, stop_event=stop):
                    continue
                raise BackendError('proxy dead %s — none left' % old_ip)
            log(session, 'proxy dead %s — retry %s' % (old_ip, nxt.host))
    raise last


def bump_proxy(session, opts, err=None):
    pool = getattr(opts, 'proxy_pool', None)
    if not pool:
        return
    if err is not None and not proxy_transport_error(err):
        pool.ensure(session)
        return
    old, nxt = pool.rotate(session)
    if old and nxt:
        log(session, 'proxy dead %s — retry %s' % (old.host, nxt.host))
    elif old and nxt is None:
        log(session, 'proxy dead %s — none left' % old.host)


def http_exchange(sock, method, path, headers, body, timeout):
    sock.settimeout(timeout)
    lines = ['%s %s HTTP/1.1' % (method, path)]
    for key, value in headers:
        lines.append('%s: %s' % (key, value))
    if body:
        lines.append('Content-Length: %d' % len(body))
    lines.append('Connection: close')
    lines.append('')
    raw = '\r\n'.join(lines).encode('utf-8') + b'\r\n'
    if body:
        raw += body
    sock.sendall(raw)
    resp = http.client.HTTPResponse(sock)
    resp.begin()
    data = resp.read()
    status = resp.status
    try:
        resp.close()
    except Exception:
        pass
    return status, data


def urlopen_bytes(req, timeout=30, context=None, proxy=None):
    if proxy is None:
        with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
            return resp.getcode(), resp.read()
    parsed = urllib.parse.urlparse(req.full_url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == 'https' else 80)
    path = parsed.path or '/'
    if parsed.query:
        path += '?' + parsed.query
    raw = open_via_proxy(host, port, proxy, timeout)
    try:
        if parsed.scheme == 'https':
            if context is None:
                context = ssl.create_default_context()
            sock = context.wrap_socket(raw, server_hostname=host)
        else:
            sock = raw
        hdrs = [('Host', parsed.netloc)]
        for key, value in req.header_items():
            if key.lower() != 'host':
                hdrs.append((key, value))
        return http_exchange(sock, req.get_method(), path, hdrs, req.data, timeout)
    finally:
        try:
            raw.close()
        except Exception:
            pass


def xmlrpc_call(url, method, params, auth=None, insecure=False, timeout=30,
                proxy=None, session=None):
    if proxy is None and session is not None:
        proxy = getattr(session, 'proxy', None)
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
        status, data = urlopen_bytes(req, timeout=timeout, context=context,
                                     proxy=proxy)
        if status >= 400:
            raise BackendError('HTTP %s from %s: %s' % (status, url, data[:200]))
    except BackendError:
        raise
    except urllib.error.HTTPError as e:
        raise BackendError('HTTP %s from %s: %s' % (e.code, url, e.read()[:200]))
    except urllib.error.URLError as e:
        raise BackendError('cannot reach %s: %s' % (url, e.reason))
    except (socket.error, ssl.SSLError, OSError) as e:
        raise BackendError('cannot reach %s: %s' % (url, e))
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
    def __init__(self, host, port, use_tls, insecure, timeout=15.0, proxy=None):
        self.host = host
        self.port = port
        self.use_tls = use_tls
        self.insecure = insecure
        self.timeout = timeout
        self.proxy = proxy
        self.sock = None
        self.buf = bytearray()
        self.op_id = 0
        self.echoes = 0
        self.pongs = 0
        self.closed_by_server = False
        self.debug_frames = False
        self.on_message = None
        self.on_close = None
        self._close_fired = False
        self.send_lock = threading.Lock()

    def _next_op(self):
        self.op_id += 1
        return self.op_id

    def open(self):
        if self.proxy:
            raw = open_via_proxy(self.host, self.port, self.proxy, self.timeout)
        else:
            raw = socket.create_connection((self.host, self.port),
                                           timeout=self.timeout)
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

    def subscribe(self, queue, retries=5, wait=12.0):
        """Subscribe and keep pinging so a slow ACK does not drop the socket."""
        old = self.sock.gettimeout() if self.sock else None
        if self.sock:
            self.sock.settimeout(2.0)
        last_err = None
        try:
            for attempt in range(retries):
                if self.closed_by_server:
                    raise BackendError('IMQ connection closed by peer')
                op = self._next_op()
                subscription = p_str(1, queue) + p_uint(2, op)
                msg = p_bytes(2, subscription)  # queues_with_results
                self._send(C2G_SUBSCRIBE, msg)
                started = time.monotonic()
                last_ping = started
                try:
                    while time.monotonic() - started < wait:
                        if self.closed_by_server:
                            raise BackendError('IMQ connection closed by peer')
                        now = time.monotonic()
                        if now - last_ping >= PING_INTERVAL:
                            self.ping()
                            last_ping = now
                        try:
                            mtype, fields = self.read_frame()
                        except socket.timeout:
                            continue
                        if self.debug_frames:
                            print('    [frame] type %d fields %s' % (mtype, fields))
                        if mtype == G2C_RESULT:
                            ops = field_values(fields, 1)
                            status = field_values(fields, 2)[0]
                            if ops and ops[0] == op:
                                if status != 0:
                                    raise BackendError(
                                        'subscribe %s failed, status %d'
                                        % (queue, status))
                                return
                        elif mtype == G2C_JOINED_QUEUE:
                            queues = field_values(fields, 2)
                            if queues and queues[0].decode('utf-8', 'replace') == queue:
                                return
                        else:
                            self._handle_async(mtype, fields)
                    last_err = BackendError(
                        'subscribe %s timed out' % queue)
                except BackendError as e:
                    if 'closed by peer' in str(e).lower():
                        raise
                    if 'failed, status' in str(e):
                        raise
                    last_err = e
            raise last_err or BackendError(
                'subscribe %s timed out after %d tries' % (queue, retries))
        finally:
            if self.sock and old is not None:
                try:
                    self.sock.settimeout(old)
                except Exception:
                    pass
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
            self._fire_close()

    def _fire_close(self):
        self.closed_by_server = True
        if self._close_fired:
            return
        self._close_fired = True
        cb = self.on_close
        if cb:
            try:
                cb()
            except Exception:
                pass

    def run_until(self, stop_event, deadline=None, extra_stop=None):
        """Drain frames; ping every PING interval on a fixed schedule (like
        the real client) so the server does not drop us in a busy room."""
        self.sock.settimeout(2.0)
        last_ping = time.monotonic()
        while not stop_event.is_set() and not self.closed_by_server:
            if extra_stop is not None and extra_stop.is_set():
                return
            if deadline is not None and time.time() >= deadline:
                return
            try:
                mtype, fields = self.read_frame()
                self._handle_async(mtype, fields)
            except socket.timeout:
                pass
            except (BackendError, socket.error, ssl.SSLError, OSError):
                self.closed_by_server = True
                return
            now = time.monotonic()
            if now - last_ping >= PING_INTERVAL:
                self.ping()
                last_ping = now

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
        self.phase = 'idle'
        self.halt = threading.Event()
        self.spam_thread = None
        self.joined_at = 0
        self._left_noted = False
        self.proxy = None


def probe_chat(session, info, opts, chat_id, timeout=30):
    """Call chat.getParticipants to inspect what the backend knows about a
    chat id (looking for the room instance id / activity)."""
    url = '%s://%s%s' % (opts.chat_scheme, opts.chat_host, opts.chat_endpoint)
    cid = session.cid
    auth = (cid, info.get('clientSessionId', ''), info.get('securityKey', ''))
    args = {'userId': cid, 'chatId': int(chat_id)}
    return xmlrpc_call(url, 'chat.getParticipants', (args,), auth=auth,
                       insecure=opts.insecure, timeout=timeout, session=session)


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
        _status, raw_b = urlopen_bytes(req, timeout=30, context=context,
                                       proxy=getattr(session, 'proxy', None))
        raw = raw_b.decode('utf-8', 'replace')
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
    def once():
        info = xmlrpc_call(url, 'test.avatarInfoForLogin2', (params,),
                           insecure=opts.insecure, session=session)
        if not isinstance(info, dict):
            raise BackendError('unexpected login response: %r' % (info,))
        if opts.print_userinfo:
            log(session, 'userInfo keys: %s' % sorted(info))
        return info
    info = run_via_proxy(session, opts, once)
    pool = getattr(opts, 'proxy_pool', None)
    if pool is not None:
        pool.set_imq_host((info or {}).get('imq_gateway_secure_host'))
    return info


def imq_connect_args(opts, info):
    host = opts.imq_host or (info or {}).get('imq_gateway_secure_host')
    if not host:
        raise BackendError('no IMQ host: pass --imq-host')
    cookie = (info or {}).get('imq_cookie', '')
    token = (info or {}).get('imq_auth_token', '')
    if isinstance(cookie, str):
        cookie = cookie.encode('utf-8')
    if isinstance(token, str):
        token = token.encode('utf-8')
    return host, cookie, token


def connect_user_imq(session, opts, info=None):
    """Open IMQ with current login bits and subscribe /user/<cid>."""
    info = info if info is not None else session.info
    host, cookie, token = imq_connect_args(opts, info)

    def once():
        if session.imq:
            try:
                session.imq.close()
            except Exception:
                pass
            session.imq = None
        session.imq = ImqClient(host, opts.imq_port, not opts.imq_plain,
                                opts.insecure, proxy=session.proxy)
        session.imq.debug_frames = opts.debug_frames
        t0 = time.time()
        session.imq.connect(str(session.cid), cookie, token)
        session.imq_ms = (time.time() - t0) * 1000
        user_queue = '/user/%s' % session.cid
        session.imq.subscribe(user_queue)
        if session.proxy:
            log(session, 'IMQ ok (%d ms) via %s' % (session.imq_ms,
                                                    session.proxy.host))
        else:
            log(session, 'IMQ ok (%d ms)  sub %s' % (session.imq_ms, user_queue),
                verbose=True)
        return session.imq

    return run_via_proxy(session, opts, once)


def chat_url(opts):
    return '%s://%s%s' % (opts.chat_scheme, opts.chat_host, opts.chat_endpoint)


def session_ping(session, opts):
    """XML-RPC session keepalive on the same host as login (not the room-list host)."""
    url = '%s://%s%s' % (opts.secure_scheme, opts.secure_host, opts.client_endpoint)
    auth = chat_auth(session, session.info)
    args = {'userId': session.cid, 'error_logs': []}
    return xmlrpc_call(url, 'ping', (args,), auth=auth, insecure=opts.insecure,
                       session=session)


def session_ping_loop(session, opts, stop_event):
    while not stop_event.is_set():
        if stop_event.wait(110):
            return
        try:
            session_ping(session, opts)
            if opts.debug_frames:
                log(session, 'session ping ok')
        except Exception as e:
            text = str(e)
            if '-32601' in text or 'not specified' in text.lower():
                continue
            log(session, 'session ping failed: %s' % text.split('\n')[0][:80])


def start_session_ping(session, opts, stop_event):
    t = threading.Thread(target=session_ping_loop,
                         args=(session, opts, stop_event), daemon=True)
    t.start()
    return t


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
                         insecure=opts.insecure, session=session)
    if not isinstance(result, dict):
        raise BackendError('unexpected getOrMakeChat response: %r' % (result,))
    if result.get('response') == 'declined':
        raise BackendError('join declined: %s %s'
                           % (result.get('reason'), result.get('explanation')))
    session.seat = result.get('seat')
    if opts.chat_id and not invite and not any(k in result for k in CHATID_KEYS):
        return opts.chat_id
    return detect_key(result, CHATID_KEYS, opts.chatid_key, 'chat id')


# Romanian joiners only. No first/second person (eu, tu, îmi, mă, te…).
# Seed if unions.txt is missing; edit the file or the TUI after that.
DEFAULT_UNIONS = (
    'și', 'de', 'la', 'pe', 'cu', 'din', 'în', 'un', 'o', 'că',
    'sau', 'dar', 'ca', 'pentru', 'după', 'fără', 'până', 'când',
    'unde', 'cum', 'dacă', 'decât', 'între', 'spre', 'prin', 'despre',
    'sub', 'peste', 'lângă', 'către', 'ori', 'nici', 'doar', 'mai',
    'foarte', 'așa', 'cât', 'ce', 'care', 'tot', 'alt', 'plus',
)
_SKIP_UNIONS = set((
    'eu', 'mie', 'meu', 'mea', 'mine', 'imi', 'îmi', 'ma', 'mă',
    'tu', 'tau', 'tău', 'ta', 'tine', 'tie', 'ție', 'iti', 'îți',
    'te', 'me', 'mi', 'se', 'yo', 'su', 'como',
))


def parse_wordlist_text(text):
    """Each non-empty line is one word or token."""
    lines = []
    for raw in str(text or '').splitlines():
        line = raw.strip().strip('\r')
        if line and not line.startswith('#'):
            lines.append(line)
    return lines


def load_wordlist(path):
    with open(path, 'r', encoding='utf-8') as f:
        lines = parse_wordlist_text(f.read())
    if not lines:
        raise BackendError('wordlist %s is empty' % path)
    return lines


def save_wordlist(path, text):
    lines = parse_wordlist_text(text)
    if not lines:
        raise BackendError('wordlist is empty')
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    return lines


def unions_path(wordlist_path):
    folder = os.path.dirname(os.path.abspath(wordlist_path or ''))
    return os.path.join(folder or '.', 'unions.txt')


def clean_unions(words):
    out = []
    seen = set()
    for raw in words or []:
        word = (raw or '').strip()
        if not word:
            continue
        key = word.lower()
        if key in _SKIP_UNIONS or key in seen:
            continue
        seen.add(key)
        out.append(word)
    return out or list(DEFAULT_UNIONS)


def load_unions(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            words = clean_unions(parse_wordlist_text(f.read()))
        if words:
            return words
    except Exception:
        pass
    return list(DEFAULT_UNIONS)


def save_unions(path, text):
    words = clean_unions(parse_wordlist_text(text))
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(words) + '\n')
    return words


def ensure_unions_file(path):
    if os.path.isfile(path):
        return
    try:
        save_unions(path, '\n'.join(DEFAULT_UNIONS))
    except Exception:
        pass


def next_union(opts, unions):
    """Walk the union list in order so every joiner gets used."""
    idx = int(getattr(opts, 'union_index', 0) or 0)
    word = unions[idx % len(unions)]
    opts.union_index = idx + 1
    return word


def build_spam_phrase(opts):
    """Keyword + Romanian union + keyword: every phrase uses at least one union."""
    words = getattr(opts, 'spam_words', None) or [getattr(opts, 'message', 'hola')]
    unions = clean_unions(getattr(opts, 'union_words', None))
    if not unions:
        path = getattr(opts, 'unions', None) or unions_path(
            getattr(opts, 'wordlist', None))
        unions = load_unions(path)
        opts.union_words = unions
    n = random.randint(2, 4)
    parts = [random.choice(words)]
    for _ in range(n - 1):
        parts.append(next_union(opts, unions))
        parts.append(random.choice(words))
    return ' '.join(parts)


def _spam_bank(session, opts, items):
    if not items:
        items = [getattr(opts, 'message', 'hola')]
    idx = getattr(session, 'spam_index', None)
    if idx is None:
        idx = random.randrange(len(items)) if len(items) > 1 else 0
    item = items[idx % len(items)]
    session.spam_index = idx + 1
    return item


def next_spam_word(session, opts):
    tokens = getattr(opts, 'spam_tokens', None)
    if tokens is None:
        tokens = []
        for line in getattr(opts, 'spam_words', None) or []:
            tokens.extend(str(line).split())
        opts.spam_tokens = tokens
    return _spam_bank(session, opts, tokens)


def next_spam_raw(session, opts):
    return _spam_bank(
        session, opts,
        getattr(opts, 'spam_words', None) or [getattr(opts, 'message', 'hola')])


def next_spam_line(session, opts):
    style = getattr(opts, 'spam_style', 'phrase') or 'phrase'
    if style == 'word':
        return next_spam_word(session, opts)
    if style == 'raw':
        return next_spam_raw(session, opts)
    return build_spam_phrase(opts)


def session_halted(session, stop_event=None):
    if stop_event is not None and stop_event.is_set():
        return True
    halt = getattr(session, 'halt', None)
    return halt is not None and halt.is_set()


def halt_session(session, opts):
    if session.halt is None:
        session.halt = threading.Event()
    session.halt.set()
    drop_imq(session, opts)
    session.phase = 'down'
    log(session, 'disconnected')


def imq_alive(session):
    imq = getattr(session, 'imq', None)
    return bool(imq and not imq.closed_by_server and imq.sock is not None)


def spam_loop_alive(session):
    stop = getattr(session, 'spam_stop', None)
    if stop is None or stop.is_set():
        return False
    thread = getattr(session, 'spam_thread', None)
    if thread is not None and not thread.is_alive():
        return False
    return True


def still_in_chat(session):
    if session.halt is not None and session.halt.is_set():
        return False
    if getattr(session, '_left_noted', False):
        return False
    if not session.chat_id:
        return False
    if (session.phase or 'idle') not in ('in-room', 'spam'):
        return False
    return imq_alive(session)


def can_accept_invite(session):
    if session.halt is not None and session.halt.is_set():
        return False
    if not session.cid or still_in_chat(session):
        return False
    if getattr(session, '_joining', False):
        return False
    return imq_alive(session)


def session_offline(session):
    """Has a cid but IMQ is dead — cannot receive invites or chat."""
    if session.halt is not None and session.halt.is_set():
        return False
    if (session.phase or '') == 'login':
        return False
    if not session.cid:
        return False
    return not imq_alive(session)


def _phase_if_not_in_room(session):
    return 'idle' if imq_alive(session) else 'down'


def effective_phase(session):
    """What the TUI should show right now, even if phase was not updated yet."""
    if session.halt is not None and session.halt.is_set():
        return 'down'
    phase = session.phase or 'idle'
    if phase == 'login':
        return 'login'
    if phase in ('in-room', 'spam') and still_in_chat(session):
        if phase == 'spam' and not spam_loop_alive(session):
            return 'in-room'
        return phase
    return _phase_if_not_in_room(session)


def wait_for_invite(session, opts, reason=None):
    """Idle + listen for a new invite so the agent can be pulled back in."""
    if session.halt is not None and session.halt.is_set():
        return
    if session.spam_stop is not None:
        session.spam_stop.set()
        session.spam_stop = None
    pool = getattr(opts, 'pool', None)
    if pool:
        pool.mark_left(session)
    session._joining = False
    session._left_noted = True
    session.chat_id = None
    if reason:
        log(session, reason)
    if not imq_alive(session) or not session.cid:
        session.phase = 'down'
        session.error = session.error or 'disconnected'
        imq = session.imq
        if imq:
            imq.closed_by_server = True
        log(session, 'offline  —  will relogin')
        return
    session.phase = 'idle'
    session.error = None
    bind_imq_session(session, opts, None)
    log(session, 'waiting for invite')


def note_left_room(session, opts, reason='left'):
    """Bot is no longer in the chat. Flip status immediately."""
    if session.halt is not None and session.halt.is_set():
        return
    was_in = session.phase in ('in-room', 'spam')
    if getattr(session, '_left_noted', False) and not was_in:
        return
    if was_in:
        lock = getattr(opts, 'stats_lock', None)
        if lock is not None:
            with lock:
                opts.stats['left'] = opts.stats.get('left', 0) + 1
    if getattr(opts, 'churn', False):
        if session.spam_stop is not None:
            session.spam_stop.set()
            session.spam_stop = None
        pool = getattr(opts, 'pool', None)
        if pool:
            pool.mark_left(session)
        session._left_noted = True
        session.chat_id = None
        session.phase = _phase_if_not_in_room(session)
        if was_in:
            log(session, reason)
        return
    wait_for_invite(session, opts, reason if was_in else None)


_LEAVE_WORDS = (
    'leave', 'left', 'kick', 'kicked', 'eject', 'ejected',
    'remove', 'removed', 'ban', 'banned', 'disconnect',
    'disconnected', 'part', 'exit', 'quit', 'chatleave',
    'leavechat', 'boot', 'booted', 'expel', 'expelled',
    'evict', 'evicted', 'uninvite', 'sessionend', 'endchat',
    'closechat', 'removedfrom', 'youwere',
)


def _token_norm(value):
    return re.sub(r'[^a-z]', '', str(value).lower())


def payload_says_self_left(session, queue, message_bytes):
    cid = str(session.cid) if session.cid else ''
    text = message_bytes.decode('utf-8', 'replace')
    if queue.startswith('/user/'):
        parts = text.split(' ', 1)
        token = _token_norm(parts[0])
        if any(word in token for word in _LEAVE_WORDS):
            if len(parts) == 2:
                try:
                    info = json.loads(parts[1])
                except ValueError:
                    info = None
                if isinstance(info, dict):
                    other = (info.get('userId') or info.get('cid')
                             or info.get('fromUserId'))
                    if other and cid and str(other) != cid:
                        return False
            return True
    try:
        data = json.loads(text)
    except ValueError:
        return False
    if not isinstance(data, dict):
        return False
    who = data.get('userId') or data.get('cid') or data.get('fromUserId')
    for key in ('leaving', 'isLeaving', 'left', 'kicked', 'removed',
                'isKicked'):
        if data.get(key) in (True, 1, '1', 'true', 'True'):
            return (not who) or (not cid) or str(who) == cid
    kind = ' '.join(str(data.get(key, '')) for key in (
        'type', 'event', 'action', 'command', 'kind', 'name', 'msgType'))
    kind_n = _token_norm(kind)
    if kind_n and any(word in kind_n for word in _LEAVE_WORDS):
        if who and cid and str(who) != cid:
            return False
        return True
    reason = _token_norm(
        str(data.get('reason') or '') + str(data.get('status') or ''))
    if reason and any(word in reason for word in _LEAVE_WORDS):
        return (not who) or (not cid) or str(who) == cid
    msg = str(data.get('message') or '')
    if msg.startswith('*') and any(
            word in _token_norm(msg)
            for word in ('leave', 'quit', 'exit', 'kick', 'boot', 'eject')):
        return (not who) or (not cid) or str(who) == cid
    return False


def extract_participant_cids(result):
    """CIDs still in the chat, or None if the payload cannot be read."""
    if not isinstance(result, dict):
        return None
    found = set()

    def take(value):
        if value in (None, '', 0, '0'):
            return
        try:
            if int(value) == 0:
                return
        except (TypeError, ValueError):
            pass
        found.add(str(value))

    def from_entry(entry):
        if isinstance(entry, dict):
            for key in ('userId', 'user_id', 'cid', 'customer_id',
                        'customerId'):
                if key in entry:
                    take(entry.get(key))
                    return
            if len(entry) == 1:
                key, value = next(iter(entry.items()))
                take(key)
                if isinstance(value, dict):
                    from_entry(value)
        else:
            take(entry)

    blob = None
    for key in ('participants', 'users', 'userIds', 'members'):
        if key in result:
            blob = result.get(key)
            break
    if blob is None:
        return None
    if isinstance(blob, dict):
        for key, value in blob.items():
            take(key)
            from_entry(value)
    elif isinstance(blob, (list, tuple)):
        for item in blob:
            from_entry(item)
    else:
        take(blob)
    return found


def mark_joined_room(session):
    session._left_noted = False
    session.joined_at = time.time()
    session.phase = 'in-room'


def bind_imq_session(session, opts, chat_queue=None):
    imq = session.imq
    if not imq:
        return
    imq.on_message = make_imq_handler(session, opts, chat_queue)
    imq.on_close = lambda: note_left_room(session, opts, 'left')
    # XML-RPC session keepalive (the real client pings every 110s); without it
    # the server considers the session dead and drops us from the room.
    if not getattr(session, '_session_ping_started', False):
        session._session_ping_started = True
        start_session_ping(session, opts, opts.stop_event)


def relogin_session(session, opts):
    lock = getattr(session, '_relogin_lock', None)
    if lock is None:
        lock = threading.Lock()
        session._relogin_lock = lock
    if not lock.acquire(False):
        return False
    session.phase = 'login'
    log(session, 'relogin')
    try:
        info = login(session, opts)
        session.info = info
        session.cid = detect_key(info, CID_KEYS, opts.cid_key, 'customer id')
        session.error = None
        session.phase = 'login' if not imq_alive(session) else 'idle'
        log(session, 'online')
        return True
    except Exception as e:
        session.error = str(e)
        session.phase = 'down'
        log(session, 'relogin failed: %s' % e)
        return False
    finally:
        lock.release()


def accept_invite_or_relogin(session, opts, chat_id, invite_id, location):
    """Accept; on failure login again and retry once. Then idle."""
    try:
        accept_invite(session, opts, chat_id, invite_id, location)
        return True
    except Exception as e:
        log(session, 'accept failed: %s' % e)
    if not relogin_session(session, opts):
        wait_for_invite(session, opts)
        return False
    log(session, 'retry accept')
    try:
        accept_invite(session, opts, chat_id, invite_id, location)
        return True
    except Exception as e:
        wait_for_invite(session, opts, 'accept failed after relogin: %s' % e)
        return False


def try_handle_invite(session, opts, queue, message_bytes):
    if still_in_chat(session):
        return False
    if getattr(session, '_joining', False):
        return False
    text = message_bytes.decode('utf-8', 'replace')
    if opts.debug_frames:
        log(session, 'user-queue msg on %s: %r' % (queue, text[:200]))
    parts = text.split(' ', 1)
    if len(parts) != 2:
        return False
    token, payload = parts
    if 'chatinvite' not in token.lower():
        return False
    try:
        info = json.loads(payload)
    except ValueError:
        return False
    if not isinstance(info, dict):
        return False
    chat_id = info.get('chatId')
    invite_id = info.get('inviteId')
    location = info.get('location')
    if not chat_id or not invite_id:
        return False
    log(session, 'invite from %s, joining' % info.get('inviter'))
    session._joining = True
    lock = invite_join_lock(opts)
    lock.acquire()
    try:
        if not accept_invite_or_relogin(
                session, opts, chat_id, invite_id, location):
            return True
        session.chat_id = chat_id
        chat_queue = chat_queue_name(chat_id)
        try:
            subscribe_after_accept(session, chat_queue, tries=8, gap=1.0)
        except Exception as e:
            if imq_alive(session):
                log(session, 'subscribe after accept failed: %s' % e)
                session.chat_id = None
                session.phase = 'idle'
                bind_imq_session(session, opts, None)
                log(session, 'waiting for invite')
            else:
                wait_for_invite(
                    session, opts,
                    'subscribe after accept failed: %s' % e)
            return True
        with opts.stats_lock:
            opts.stats['joined'] += 1
        log(session, 'in room')
        mark_joined_room(session)
        arm_trigger(session, opts, chat_queue)
        time.sleep(0.25)
        return True
    finally:
        session._joining = False
        lock.release()


def invite_session_dead(err):
    text = str(err).lower()
    needles = (
        'session', 'auth', 'login', 'disconnect', 'closed',
        'not connected', 'timed out', 'timeout', 'broken pipe',
        'reset', 'eof', '401', '403', 'invalid key', 'security',
    )
    for needle in needles:
        if needle in text:
            return True
    return False


def mark_invite_dead(session, opts, reason):
    session.error = reason
    session.chat_id = None
    session._left_noted = True
    session._joining = False
    session.phase = 'down'
    log(session, reason)
    imq = session.imq
    if imq:
        imq.closed_by_server = True


def looks_like_cid(value):
    if value is None:
        return True
    text = str(value).strip()
    return (not text) or text.isdigit()


def cid_name_store(opts):
    store = getattr(opts, 'cid_names', None)
    if store is None:
        store = {}
        opts.cid_names = store
        opts.cid_names_lock = threading.Lock()
        opts.cid_name_pending = set()
    return store


def remember_cid_name(opts, cid, name):
    cid = '' if cid is None else str(cid)
    name = (name or '').strip()
    if not cid or looks_like_cid(name):
        return
    store = cid_name_store(opts)
    lock = getattr(opts, 'cid_names_lock', None)
    if lock:
        with lock:
            store[cid] = name
    else:
        store[cid] = name
    for session in getattr(opts, 'sessions', None) or []:
        chat_lock = getattr(session, 'chat_lock', None)
        lines = getattr(session, 'chat_log', None)
        if not lines:
            continue
        if chat_lock is None:
            for item in lines:
                if str(item.get('sender')) == cid:
                    item['name'] = name
            continue
        with chat_lock:
            for item in lines:
                if str(item.get('sender')) == cid:
                    item['name'] = name


def name_for_cid(opts, cid, fallback=''):
    cid = '' if cid is None else str(cid)
    if not cid:
        return fallback or '?'
    for session in getattr(opts, 'sessions', None) or []:
        if session.cid is not None and str(session.cid) == cid:
            remember_cid_name(opts, cid, session.username)
            return session.username
    store = cid_name_store(opts)
    cached = store.get(cid)
    if cached:
        return cached
    if fallback and not looks_like_cid(fallback):
        remember_cid_name(opts, cid, fallback)
        return fallback
    return fallback or cid


def fetch_avatar_name(session, opts, cid):
    """Same endpoint the client avatar card uses: /api/avatarcard.php."""
    cid = str(cid)
    info = getattr(session, 'info', None) or {}
    viewer = session.cid
    if not viewer or not info.get('securityKey'):
        return None
    query = urllib.parse.urlencode({'cid': cid, 'viewer_cid': viewer})
    url = '%s://%s/api/avatarcard.php?%s' % (
        opts.service_scheme, opts.service_host, query)
    key = info.get('securityKey', '')
    headers = {
        'User-Agent': 'IMVU Client',
        'X-imvu-userid': str(viewer),
        'X-imvu-csid': str(info.get('clientSessionId', '')),
        'X-imvu-auth': hashlib.md5(
            str(viewer).encode('utf-8') + str(key).encode('utf-8')
            + query.encode('utf-8')).hexdigest(),
    }
    req = urllib.request.Request(url, headers=headers)
    context = insecure_context(opts)
    try:
        _status, raw_b = urlopen_bytes(req, timeout=15, context=context,
                                       proxy=getattr(session, 'proxy', None))
        raw = raw_b.decode('utf-8', 'replace')
        data = json.loads(raw)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    name = (data.get('avatarName') or data.get('avname')
            or data.get('avatar_name') or data.get('name'))
    if looks_like_cid(name):
        return None
    if data.get('isGuest') or data.get('guest') or data.get('is_guest'):
        if not str(name).lower().startswith('guest_'):
            name = 'Guest_%s' % name
    return name


def request_avatar_name(session, opts, cid):
    cid = '' if cid is None else str(cid)
    if not cid:
        return
    if not looks_like_cid(name_for_cid(opts, cid)):
        return
    store = cid_name_store(opts)
    pending = opts.cid_name_pending
    lock = opts.cid_names_lock
    with lock:
        if cid in store or cid in pending:
            return
        pending.add(cid)

    def worker():
        try:
            name = fetch_avatar_name(session, opts, cid)
            if name:
                remember_cid_name(opts, cid, name)
        finally:
            with lock:
                pending.discard(cid)

    threading.Thread(target=worker, daemon=True).start()


def harvest_participant_names(result, opts):
    if not isinstance(result, dict):
        return

    def walk(node):
        if isinstance(node, dict):
            cid = (node.get('userId') or node.get('user_id')
                   or node.get('cid') or node.get('customer_id')
                   or node.get('customerId'))
            name = None
            for key in ('avatarName', 'avatarname', 'avname', 'avatar_name',
                        'who', 'name', 'userName', 'username',
                        'customers_name'):
                value = node.get(key)
                if value and not looks_like_cid(value):
                    name = value
                    break
            if cid and name:
                remember_cid_name(opts, cid, name)
            for key, value in node.items():
                if str(key).isdigit() and isinstance(value, dict):
                    nested = dict(value)
                    nested.setdefault('cid', key)
                    walk(nested)
                else:
                    walk(value)
        elif isinstance(node, (list, tuple)):
            for item in node:
                walk(item)

    walk(result)


def parse_chat_payload(message_bytes):
    try:
        data = json.loads(message_bytes.decode('utf-8', 'replace'))
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    return data


def record_chat_message(session, sender, name, text, to=0):
    text = (text or '').strip()
    if not text:
        return
    now = time.time()
    sender = '' if sender is None else str(sender)
    if getattr(session, 'chat_lock', None) is None:
        session.chat_lock = threading.Lock()
        session.chat_log = []
        session.chat_seq = 0
    with session.chat_lock:
        log_lines = session.chat_log
        if log_lines:
            last = log_lines[-1]
            if (last.get('sender') == sender
                    and last.get('text') == text
                    and now - last.get('ts', 0) < 0.4):
                return
        session.chat_seq += 1
        log_lines.append({
            'seq': session.chat_seq,
            'ts': now,
            'sender': sender,
            'name': name or sender or '?',
            'text': text,
            'to': to or 0,
        })
        del log_lines[:-400]


def record_incoming_chat(session, opts, queue, message_bytes):
    if not queue.startswith('/chat/'):
        return
    data = parse_chat_payload(message_bytes)
    if not data:
        return
    text = str(data.get('message') or '').strip()
    if not text or text.startswith('*'):
        return
    sender = data.get('userId') or data.get('cid')
    name = (data.get('avatarName') or data.get('avatarname')
            or data.get('who') or data.get('name')
            or data.get('userName') or data.get('username'))
    resolved = name_for_cid(opts, sender, name)
    record_chat_message(session, sender, resolved, text, data.get('to') or 0)
    if looks_like_cid(resolved):
        request_avatar_name(session, opts, sender)
    dest = data.get('to') or 0
    if dest and looks_like_cid(name_for_cid(opts, dest)):
        request_avatar_name(session, opts, dest)


def send_manual_chat(session, opts, text, to=0):
    text = (text or '').strip()
    if not text:
        return 'empty'
    if session_halted(session) or not imq_alive(session) or not session.chat_id:
        return 'not in room'
    try:
        to_cid = int(to or 0)
    except (TypeError, ValueError):
        to_cid = 0
    queue = chat_queue_name(session.chat_id)
    payload = json.dumps({
        'userId': session.cid,
        'chatId': session.chat_id,
        'message': text,
        'to': to_cid,
    }).encode('utf-8')
    try:
        session.imq.send_chat(queue, payload)
        session.sent += 1
        with opts.stats_lock:
            opts.stats['sent'] += 1
    except Exception as e:
        return str(e)
    record_chat_message(session, session.cid, session.username, text, to_cid)
    return None


def make_imq_handler(session, opts, chat_queue=None):
    trigger = (make_trigger_handler(session, opts, chat_queue)
               if chat_queue else None)

    def on_message(user_id, q, message_bytes):
        record_incoming_chat(session, opts, q, message_bytes)
        if payload_says_self_left(session, q, message_bytes):
            note_left_room(session, opts, 'left')
            return
        if try_handle_invite(session, opts, q, message_bytes):
            return
        if trigger:
            trigger(user_id, q, message_bytes)

    return on_message


def reconcile_session(session, opts):
    """If IMQ or the spam thread died, do not keep showing in-room/spam."""
    if session.halt is not None and session.halt.is_set():
        return
    if (session.phase or '') == 'login':
        return
    if getattr(session, '_left_noted', False) or not session.chat_id:
        if session.phase in ('in-room', 'spam', 'idle', 'down'):
            session.phase = _phase_if_not_in_room(session)
        return
    phase = session.phase
    if phase not in ('in-room', 'spam'):
        if phase in ('idle', 'down'):
            session.phase = _phase_if_not_in_room(session)
        return
    if not imq_alive(session):
        note_left_room(session, opts, 'left')
        return
    if phase == 'spam' and not spam_loop_alive(session):
        session.phase = 'in-room'


def room_full_error(err):
    text = str(err).lower()
    needles = (
        'full', 'too many', 'maximum', 'capacity', 'no seat',
        'no room', 'occupancy', 'over the limit', 'room is full',
    )
    for needle in needles:
        if needle in text:
            return True
    return False


def accept_method_missing(err):
    text = str(err).lower()
    needles = (
        'unknown method', 'no such method', 'invalid method',
        'not implemented', 'unimplemented',
    )
    for needle in needles:
        if needle in text:
            return True
    return False


def invite_join_lock(opts):
    lock = getattr(opts, 'invite_join_lock', None)
    if lock is None:
        lock = threading.Lock()
        opts.invite_join_lock = lock
    return lock


def not_in_chat_error(err):
    text = str(err).lower()
    needles = (
        'not in', 'not a participant', 'no such chat', 'unknown chat',
        'invalid chat', 'declined', 'kicked', 'removed', 'not found',
        'left', '1012', '1006', '1008', '1010',
    )
    for needle in needles:
        if needle in text:
            return True
    return False


def session_missing_from_chat(session, opts):
    """True if this bot is no longer in the chat. None if we cannot tell."""
    if not session.chat_id or not session.info or not session.cid:
        return None
    try:
        result = probe_chat(session, session.info, opts, session.chat_id,
                            timeout=8)
    except Exception as e:
        if not_in_chat_error(e):
            return True
        return None
    harvest_participant_names(result, opts)
    if isinstance(result, dict) and result.get('response') == 'declined':
        return True
    present = extract_participant_cids(result)
    if present is None:
        return None
    return str(session.cid) not in present


def presence_watch(opts, stop_event):
    while not stop_event.wait(1.2):
        sessions = getattr(opts, 'sessions', None) or []
        for session in sessions:
            if stop_event.is_set():
                return
            if session.halt is not None and session.halt.is_set():
                continue
            reconcile_session(session, opts)
            if not still_in_chat(session):
                continue
            joined_at = getattr(session, 'joined_at', 0) or 0
            if joined_at and (time.time() - joined_at) < 3:
                continue
            missing = session_missing_from_chat(session, opts)
            if missing:
                note_left_room(session, opts, 'kicked')


def spam_wait(opts):
    lo = getattr(opts, 'spam_delay', 0.4)
    hi = getattr(opts, 'spam_delay_max', lo)
    if hi < lo:
        hi = lo
    return random.uniform(lo, hi)


class SpamBeat(object):
    """One clock for every bot so they send on the same tick, not in a queue."""

    def __init__(self, interval):
        self.interval = max(0.05, float(interval or 0.4))
        self.cond = threading.Condition()
        self.tick = 0
        self.stop = threading.Event()

    def run(self):
        while not self.stop.is_set():
            if self.stop.wait(self.interval):
                break
            with self.cond:
                self.tick += 1
                self.cond.notify_all()

    def wait_tick(self, extra_stop=None):
        with self.cond:
            n = self.tick
            while self.tick == n:
                if self.stop.is_set():
                    return False
                if extra_stop is not None and extra_stop.is_set():
                    return False
                self.cond.wait(0.05)
            return True


def ensure_spam_beat(opts):
    beat = getattr(opts, 'spam_beat', None)
    if beat is not None and not beat.stop.is_set():
        delay = max(0.05, float(getattr(opts, 'spam_delay', 0.4) or 0.4))
        beat.interval = delay
        return beat
    beat = SpamBeat(getattr(opts, 'spam_delay', 0.4))
    opts.spam_beat = beat
    threading.Thread(target=beat.run, daemon=True).start()
    return beat


def start_spam_loop(session, opts, queue):
    if session.spam_stop is not None and not session.spam_stop.is_set():
        return
    if opts.spam:
        attach_word_banks(opts)
    if getattr(opts, 'spam_rate_t0', None) is None:
        mark_spam_rate_start(opts)
    session.spam_stop = threading.Event()
    t = threading.Thread(target=spam_loop,
                         args=(session, opts, queue, session.spam_stop),
                         daemon=True)
    session.spam_thread = t
    t.start()
    session.phase = 'spam'
    log(session, 'spam loop')


def mark_spam_rate_start(opts):
    stats = getattr(opts, 'stats', None) or {}
    opts.spam_rate_t0 = time.time()
    opts.spam_rate_sent0 = stats.get('sent', 0)
    opts.spam_rate_frozen = None


def mark_spam_rate_stop(opts):
    if getattr(opts, 'spam_rate_frozen', None) is not None:
        return
    opts.spam_rate_frozen = spam_mps(opts)


def spam_mps(opts):
    frozen = getattr(opts, 'spam_rate_frozen', None)
    if frozen is not None:
        return frozen
    t0 = getattr(opts, 'spam_rate_t0', None)
    if not t0:
        return 0.0
    stats = getattr(opts, 'stats', None) or {}
    sent = max(0, stats.get('sent', 0) - getattr(opts, 'spam_rate_sent0', 0))
    dt = time.time() - t0
    if dt < 0.2:
        return 0.0
    return sent / dt


def start_spam_all(opts):
    mark_spam_rate_start(opts)
    if opts.spam:
        attach_word_banks(opts)
    for session in getattr(opts, 'sessions', None) or []:
        if session_halted(session) or not still_in_chat(session):
            continue
        if not session.imq or not session.chat_id:
            continue
        start_spam_loop(session, opts, chat_queue_name(session.chat_id))


def stop_spam_all(opts):
    mark_spam_rate_stop(opts)
    for session in getattr(opts, 'sessions', None) or []:
        if session.spam_stop is not None and not session.spam_stop.is_set():
            session.spam_stop.set()
            if session.phase == 'spam' and still_in_chat(session):
                session.phase = 'in-room'


def spam_loop(session, opts, queue, stop):
    try:
        while not stop.is_set() and not session_halted(session):
            if stop.wait(spam_wait(opts)):
                return
            imq = session.imq
            if imq is None or imq.closed_by_server:
                note_left_room(session, opts, 'left')
                return
            if not session.chat_id:
                note_left_room(session, opts, 'left')
                return
            line = next_spam_line(session, opts)
            payload = json.dumps({
                'userId': session.cid,
                'chatId': session.chat_id,
                'message': line,
                'to': 0,
            }).encode('utf-8')
            try:
                imq.send_chat(queue, payload)
                session.sent += 1
                with opts.stats_lock:
                    opts.stats['sent'] += 1
            except Exception:
                note_left_room(session, opts, 'left')
                return
    finally:
        if getattr(session, '_left_noted', False) or not session.chat_id:
            if session.phase in ('in-room', 'spam'):
                session.phase = _phase_if_not_in_room(session)
            return
        if session.phase == 'spam':
            if not imq_alive(session):
                note_left_room(session, opts, 'left')
            else:
                session.phase = 'in-room'


def make_trigger_handler(session, opts, queue):
    trigger_re = (re.compile(r'\b' + re.escape(opts.trigger) + r'\b',
                             re.IGNORECASE)
                  if opts.trigger else None)
    stop_re = (re.compile(r'\b' + re.escape(opts.stop_trigger) + r'\b',
                          re.IGNORECASE)
               if opts.stop_trigger else None)
    watch = str(opts.trigger_from) if opts.trigger_from else None

    def start_spam():
        now = time.monotonic()
        if now - getattr(opts, '_last_go', 0) < 0.35:
            start_spam_loop(session, opts, queue)
            return
        opts._last_go = now
        log(session, 'got go')
        start_spam_all(opts)

    def stop_spam():
        now = time.monotonic()
        if now - getattr(opts, '_last_stop', 0) < 0.35:
            if session.spam_stop is not None and not session.spam_stop.is_set():
                session.spam_stop.set()
                if session.phase == 'spam' and still_in_chat(session):
                    session.phase = 'in-room'
            return
        opts._last_stop = now
        log(session, 'got stop')
        stop_spam_all(opts)

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
            if trigger_re and trigger_re.search(text):
                start_spam()
            return
        if not trigger_re or not trigger_re.search(text):
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


def pick_word(opts, session=None):
    if getattr(opts, 'spam_words', None) and session is not None:
        return next_spam_line(session, opts)
    words = getattr(opts, 'spam_words', None)
    if words:
        return words[0]
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
    while not session_halted(session, stop_event):
        if pool.claim_host(session):
            log(session, 'entering room')
            return None
        if pool.host_ready.wait(timeout=5):
            inviter = pool.inviter(session)
            if inviter and inviter.chat_id:
                log(session, 'entering via %s' % inviter.username)
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
                        log(session, '%s — retry %d/%d'
                            % (err, attempt + 2, attempts), verbose=True)
                        time.sleep(random.uniform(0.3, 0.51))
                        continue
                    raise
            if last_err:
                raise last_err
            log(session, 'chatId %s' % (session.chat_id,), verbose=True)

            connect_user_imq(session, opts, info)

            queue = chat_queue_name(session.chat_id)
            session.imq.subscribe(queue)
            log(session, 'sub %s' % queue, verbose=True)

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
                log(session, 'seat %s' % session.seat, verbose=True)
            if pool:
                pool.mark_joined(session)
            mark_joined_room(session)
            bind_imq_session(session, opts, queue)
            log(session, 'in room')
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
    session.phase = 'down'
    log(session, 'left')


def send_words(session, opts, queue, count, stop_event, stats_lock):
    for i in range(count):
        if (session_halted(session, stop_event)
                or not session.imq or session.imq.closed_by_server):
            break
        word = pick_word(opts, session)
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
        log(session, 'sent %d/%d: %s' % (i + 1, count, word), verbose=True)
        if i + 1 < count:
            time.sleep(random.uniform(0.3, 0.51))


def run_churn(session, info, opts, stop_event, stats_lock):
    """Join -> send N words -> leave, then repeat until stop."""
    while not session_halted(session, stop_event):
        try:
            queue = join_room(session, info, opts, stats_lock, stop_event)
            if session_halted(session, stop_event):
                break
            arm_trigger(session, opts, queue)
            send_words(session, opts, queue, opts.repeat, stop_event, stats_lock)
            if opts.hold > 0 and session.imq and not session_halted(session, stop_event):
                session.imq.run_until(stop_event, time.time() + opts.hold,
                                      extra_stop=session.halt)
        except (BackendError, socket.error, ssl.SSLError) as e:
            session.error = str(e)
            log(session, 'ERROR: %s' % e)
            with stats_lock:
                opts.stats['errors'] += 1
        leave_room(session, opts, stats_lock)
        session.cycles += 1
        with stats_lock:
            opts.stats['cycles'] += 1
        log(session, 'cycle %d' % session.cycles, verbose=True)
        if stop_event.wait(opts.churn_delay):
            break


def drop_imq(session, opts, count_leave=False):
    """Tear down IMQ without treating it as an intentional leave."""
    if session.spam_stop is not None:
        session.spam_stop.set()
        session.spam_stop = None
    pool = getattr(opts, 'pool', None)
    if session.imq:
        session.echoes += session.imq.echoes
        session.pongs += session.imq.pongs
        try:
            session.imq.close()
        except Exception:
            pass
        session.imq = None
    if pool:
        pool.mark_left(session)
    if count_leave:
        with opts.stats_lock:
            opts.stats['left'] += 1
    session.phase = 'down'


def arm_trigger(session, opts, queue):
    if opts.spam:
        attach_word_banks(opts)
    bind_imq_session(session, opts, queue)
    if opts.trigger or opts.stop_trigger:
        log(session, 'listening', verbose=True)
    if (opts.spam and not opts.churn
            and (getattr(opts, 'spam_auto', False) or not opts.trigger)):
        start_spam_loop(session, opts, queue)


def run_account(session, opts, stop_event, stats_lock):
    if opts.listen_invite:
        run_listen_invite(session, opts, stop_event, stats_lock)
        return
    try:
        session.phase = 'login'
        t0 = time.time()
        info = login(session, opts)
        session.info = info
        session.login_ms = (time.time() - t0) * 1000
        session.cid = detect_key(info, CID_KEYS, opts.cid_key, 'customer id')
        if session.proxy:
            log(session, 'online via %s' % session.proxy.host)
        else:
            log(session, 'online')

        if opts.churn:
            run_churn(session, info, opts, stop_event, stats_lock)
            return

        while not session_halted(session, stop_event):
            try:
                queue = join_room(session, info, opts, stats_lock, stop_event)
                if session_halted(session, stop_event):
                    break
                session.error = None
                arm_trigger(session, opts, queue)
                looping = (session.spam_stop is not None
                           and not session.spam_stop.is_set())
                send_words(session, opts, queue,
                           0 if (looping or opts.trigger) else opts.repeat,
                           stop_event, stats_lock)
                deadline = (time.time() + opts.hold) if opts.hold > 0 else None
                if session.imq:
                    session.imq.run_until(stop_event, deadline,
                                          extra_stop=session.halt)
                reconcile_session(session, opts)
            except (BackendError, socket.error, ssl.SSLError) as e:
                if session_halted(session, stop_event):
                    break
                session.error = str(e)
                session.phase = 'down'
                log(session, 'ERROR: %s' % e)
                bump_proxy(session, opts, e)
                with stats_lock:
                    opts.stats['errors'] += 1
            if session_halted(session, stop_event) or opts.hold > 0:
                break
            dropped = (session.imq is None or session.imq.closed_by_server
                       or session.error)
            if not dropped:
                break
            log(session, 'rejoining')
            drop_imq(session, opts)
            err = session.error or ''
            if 'no working prox' in err.lower() or 'none left' in err.lower():
                if stop_event.wait(3.0):
                    break
                continue
            if session.error:
                bump_proxy(session, opts, session.error)
            if not relogin_session(session, opts):
                if stop_event.wait(2.0):
                    break
                continue
            info = session.info
            if stop_event.wait(0.4):
                break
    finally:
        drop_imq(session, opts, count_leave=False)
        px = getattr(opts, 'proxy_pool', None)
        if px:
            px.release(session)
_log_verbose = False


def log(session, text, verbose=False):
    if verbose and not _log_verbose:
        return
    line = '[%s] %s' % (session.username, text)
    hook = _log_hook
    if hook:
        try:
            hook(line)
        except Exception:
            pass
        if not _log_verbose:
            return
    print(line, flush=True)


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
    parser.add_argument('--spam-delay', type=float, default=0.4,
                        help='min seconds between spam messages (default 0.4)')
    parser.add_argument('--spam-delay-max', type=float, default=0.4,
                        help='max seconds between spam messages (default 0.4)')
    parser.add_argument('--wordlist',
                        default=os.path.join(here, 'wordlist.txt'),
                        help='keywords file: one word per line, mixed into phrases')
    parser.add_argument('--unions',
                        default=os.path.join(here, 'unions.txt'),
                        help='joining words (tu, como, de...) used between keywords')
    parser.add_argument('--spam-style', choices=('phrase', 'word', 'raw'),
                        default='phrase',
                        help='phrase: mix with unions; word: one token; '
                             'raw: each wordlist line as-is, no unions')
    parser.add_argument('--spam-auto', action='store_true',
                        help='start the wordlist loop immediately (no go word)')
    parser.add_argument('--trigger',
                        help='word that starts the loop; omit with --spam-auto '
                             'to send the wordlist on its own')
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
                        help='open a desktop window')
    parser.add_argument('--menu', action='store_true',
                        help='open the terminal menu (default if no --room)')
    parser.add_argument('--verbose', action='store_true',
                        help='print every join/subscribe/send line')
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
    parser.add_argument('--proxy', action='store_true',
                        help='send each account through a proxy (spread load)')
    parser.add_argument('--proxy-api', default=DEFAULT_PROXY_API,
                        help='URL that returns ip:port lines (SOCKS5 preferred)')

    parser.add_argument('--client-version', default='554.0')
    parser.add_argument('--cid-key', help='customer id key in login response')
    parser.add_argument('--chatid-key', help='chat id key in getOrMakeChat response')
    parser.add_argument('--print-userinfo', action='store_true',
                        help='log the login response keys (key mapping debug)')
    parser.add_argument('--debug-frames', action='store_true',
                        help='log every IMQ frame received while subscribing')

    opts = parser.parse_args(argv)
    opts.use_proxy = bool(getattr(opts, 'proxy', False))
    if opts.host:
        opts.secure_host = opts.chat_host = opts.service_host = opts.host
    global _log_verbose
    _log_verbose = bool(opts.verbose)
    no_target = (not opts.room and not opts.chat_id and not opts.find_room
                 and not opts.probe_chat and not opts.listen_invite)
    if opts.gui and not opts.find_room and not opts.probe_chat:
        return run_gui(opts)
    if (opts.menu or no_target) and not opts.find_room and not opts.probe_chat:
        from tui import run_tui
        return run_tui(opts)

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
    if getattr(opts, 'spam', False):
        attach_word_banks(opts)
    return stats_lock, stop_event


def attach_word_banks(opts):
    if not getattr(opts, 'spam_words', None):
        try:
            opts.spam_words = load_wordlist(opts.wordlist)
        except Exception:
            opts.spam_words = [getattr(opts, 'message', 'hola')]
    upath = getattr(opts, 'unions', None) or unions_path(
        getattr(opts, 'wordlist', None))
    opts.unions = upath
    ensure_unions_file(upath)
    if not getattr(opts, 'union_words', None):
        opts.union_words = load_unions(upath)


def run_load(opts):
    stats_lock, stop_event = prepare_run(opts)

    accounts = load_accounts(opts.accounts, opts.count)
    opts.total_accounts = len(accounts)
    opts.sessions = accounts
    if not accounts:
        print('no accounts in %s' % opts.accounts)
        return 1

    try:
        prepare_proxies(opts)
    except BackendError as e:
        if not getattr(opts, 'tui', False):
            print(e)
        else:
            _proxy_note(str(e))
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

    if not getattr(opts, 'tui', False):
        if opts.listen_invite:
            target = 'listen-invite (waiting for invitations)'
        else:
            target = opts.room or ('chat ' + str(opts.chat_id))
        mode = 'churn join/leave + %d words' % opts.repeat if opts.churn else (
            'message %r x%d' % (opts.message, opts.repeat))
        print('roomload: %d accounts -> %s, %s'
              % (len(accounts), target, mode), flush=True)

    threads = []
    workers = {}
    started = time.time()
    watch = threading.Thread(target=presence_watch,
                             args=(opts, stop_event), daemon=True)
    watch.start()

    def spawn_account(session):
        thread = threading.Thread(
            target=run_account,
            args=(session, opts, stop_event, stats_lock),
            daemon=True,
        )
        session.worker = thread
        workers[session.username] = thread
        threads.append(thread)
        thread.start()
        return thread

    try:
        for session in accounts:
            spawn_account(session)
            if opts.ramp:
                time.sleep(opts.ramp)
        while not stop_event.is_set():
            time.sleep(random.uniform(0.3, 0.51))
            any_alive = False
            for session in accounts:
                if session.halt.is_set():
                    continue
                worker = workers.get(session.username)
                if worker is not None and worker.is_alive():
                    any_alive = True
            if not any_alive:
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

    if getattr(opts, 'tui', False):
        return 0 if not failed else 2

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
        result = xmlrpc_call(url, 'chat.acceptInvite', (args,), auth=auth,
                             insecure=opts.insecure, session=session)
    except BackendError as e:
        if not accept_method_missing(e):
            log(session, 'acceptInvite failed: %s' % e, verbose=True)
        result = None
    else:
        if isinstance(result, dict) and result.get('response') == 'declined':
            reason = '%s %s' % (result.get('reason'), result.get('explanation'))
            log(session, 'accept declined: %s — trying getOrMakeChat' % reason.strip(),
                verbose=True)
            result = None
        else:
            return result
    args2 = {'userId': session.cid, 'version': opts.client_version,
             'publicroom': True, 'private': True}
    if chat_id:
        args2['chatId'] = int(chat_id)
    room = room_from_location(location)
    if room:
        args2['activity'] = 'publicroom-%s' % room
        args2['private'] = False
    result = xmlrpc_call(url, 'chat.getOrMakeChat', (args2,), auth=auth,
                         insecure=opts.insecure, session=session)
    if isinstance(result, dict) and result.get('response') == 'declined':
        reason = '%s %s' % (result.get('reason'), result.get('explanation'))
        log(session, 'getOrMakeChat declined: %s — joining via invite anyway'
            % reason.strip())
        return {'chatId': chat_id}
    return result


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


def subscribe_after_accept(session, queue, tries=4, gap=1.0):
    """Subscribe to the chat queue; retry so a busy or full room can catch up."""
    last_err = None
    for i in range(tries):
        imq = session.imq
        if imq is None or imq.closed_by_server:
            raise last_err or BackendError('IMQ dropped during subscribe')
        try:
            imq.subscribe(queue, retries=2, wait=10.0)
            return
        except Exception as e:
            last_err = e
            if not imq_alive(session):
                break
            if i + 1 >= tries:
                break
            log(session, 'subscribe failed, retry %d/%d in %.0fs'
                % (i + 2, tries, gap))
            time.sleep(gap)
            if imq_alive(session):
                try:
                    session.imq.ping()
                except Exception:
                    pass
    raise last_err


def make_invite_handler(session, opts):
    return make_imq_handler(session, opts, None)


def run_listen_invite(session, opts, stop_event, stats_lock):
    try:
        session.phase = 'login'
        t0 = time.time()
        info = login(session, opts)
        session.info = info
        session.login_ms = (time.time() - t0) * 1000
        session.cid = detect_key(info, CID_KEYS, opts.cid_key, 'customer id')
        if session.proxy:
            log(session, 'waiting for invite via %s' % session.proxy.host)
        else:
            log(session, 'waiting for invite')
        with stats_lock:
            opts.stats['ready'] = opts.stats.get('ready', 0) + 1
            opts.ready_cids.append(str(session.cid))
            if not getattr(opts, 'tui', False):
                print('INVITE CIDS: ' + ','.join(opts.ready_cids), flush=True)

        while not session_halted(session, stop_event):
            try:
                connect_user_imq(session, opts)
                session.error = None
                if session.phase not in ('in-room', 'spam'):
                    session.phase = 'idle'
                bind_imq_session(session, opts, (
                    chat_queue_name(session.chat_id)
                    if session.chat_id else None))
                deadline = ((time.time() + opts.hold)
                            if opts.hold > 0 else None)
                session.imq.run_until(stop_event, deadline,
                                      extra_stop=session.halt)
            except (BackendError, socket.error, ssl.SSLError) as e:
                if session_halted(session, stop_event):
                    break
                session.error = str(e)
                session.phase = 'down'
                log(session, 'ERROR: %s' % e)
                bump_proxy(session, opts, e)
                with stats_lock:
                    opts.stats['errors'] += 1
            if session_halted(session, stop_event) or opts.hold > 0:
                break
            dropped = (session.imq is None or session.imq.closed_by_server
                       or session.error)
            if not dropped:
                break
            log(session, 'reconnecting')
            drop_imq(session, opts)
            err = session.error or ''
            if 'no working prox' in err.lower() or 'none left' in err.lower():
                if stop_event.wait(3.0):
                    break
                continue
            bump_proxy(session, opts, session.error or 'closed')
            if not relogin_session(session, opts):
                if stop_event.wait(2.0):
                    break
                continue
            if stop_event.wait(0.4):
                break
    finally:
        drop_imq(session, opts, count_leave=False)
        px = getattr(opts, 'proxy_pool', None)
        if px:
            px.release(session)


if __name__ == '__main__':
    sys.exit(main())
