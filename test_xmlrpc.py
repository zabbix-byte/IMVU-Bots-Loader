# Offline test of the XML-RPC layer against a local mock server (Python 3).
import hashlib
import json
import sys
import threading
import xmlrpc.client
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, r'c:\sdsd\dev\roomload')
import roomload as r

CID = 361688157
CSID = 'csid-abc'
KEY = 'security-key-123'

seen = {}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        seen['path'] = self.path
        seen['headers'] = dict(self.headers)
        seen['body'] = body
        params, method = xmlrpc.client.loads(body)
        seen['method'] = method
        seen['params'] = params
        seen['headers_lc'] = {k.lower(): v for k, v in seen['headers'].items()}

        if method == 'test.avatarInfoForLogin2':
            result = {
                'customer_id': CID,
                'avatarName': 'ztrunk',
                'securityKey': KEY,
                'clientSessionId': CSID,
                'imq_cookie': 'cookie-bytes',
                'imq_auth_token': 'token-xyz',
                'imq_gateway_secure_host': 'imq.example.test',
            }
        elif method == 'chat.getOrMakeChat':
            result = {'chatId': 777, 'seat': ''}
        else:
            result = {}

        out = xmlrpc.client.dumps((result,), methodresponse=True)
        self.send_response(200)
        self.send_header('Content-Type', 'text/xml')
        self.send_header('Content-Length', str(len(out)))
        self.end_headers()
        self.wfile.write(out.encode('utf-8'))

    def log_message(self, *args):
        pass


def check(name, ok):
    print('%s: %s' % (name, 'OK' if ok else 'FAIL'))
    if not ok:
        sys.exit(1)


server = HTTPServer(('127.0.0.1', 0), Handler)
port = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()

# --- login (no auth headers) ---
url = 'http://127.0.0.1:%d/api/xmlrpc/client.php' % port
info = r.xmlrpc_call(url, 'test.avatarInfoForLogin2', ({
    'avatarname': 'ztrunk',
    'client_version': '554.0',
    'system_info': {},
    'client_type': 'imvu',
    'client_experiments': [],
    'password': 'secret',
},))
check('login response', info['customer_id'] == CID
      and info['imq_auth_token'] == 'token-xyz')
check('login method', seen['method'] == 'test.avatarInfoForLogin2')
check('login params', seen['params'][0]['avatarname'] == 'ztrunk'
      and seen['params'][0]['password'] == 'secret')
check('login has no auth header', 'x-imvu-auth' not in seen['headers_lc'])

# --- authenticated call ---
url = 'http://127.0.0.1:%d/api/xmlrpc/chat.php' % port
result = r.xmlrpc_call(url, 'chat.getOrMakeChat',
                       ({'userId': CID, 'version': '554.0', 'publicroom': '12345'},),
                       auth=(CID, CSID, KEY))
check('getOrMakeChat response', result['chatId'] == 777)
check('auth userid header', seen['headers_lc'].get('x-imvu-userid') == str(CID))
check('auth csid header', seen['headers_lc'].get('x-imvu-csid') == CSID)
expected = hashlib.md5(str(CID).encode() + KEY.encode() + seen['body']).hexdigest()
check('auth md5 header', seen['headers_lc'].get('x-imvu-auth') == expected)

# --- fault handling ---
class FaultHandler(Handler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        out = xmlrpc.client.dumps(xmlrpc.client.Fault(11, 'bad login'),
                                  methodresponse=True)
        self.send_response(200)
        self.send_header('Content-Type', 'text/xml')
        self.send_header('Content-Length', str(len(out)))
        self.end_headers()
        self.wfile.write(out.encode('utf-8'))

server2 = HTTPServer(('127.0.0.1', 0), FaultHandler)
threading.Thread(target=server2.serve_forever, daemon=True).start()
try:
    r.xmlrpc_call('http://127.0.0.1:%d/x' % server2.server_address[1],
                  'test.avatarInfoForLogin2', ({},))
    check('fault raises', False)
except r.BackendError as e:
    check('fault raises', 'fault 11' in str(e))

# --- key detection ---
check('cid detect', r.detect_key({'customer_id': 5}, r.CID_KEYS, None, 'cid') == 5)
check('cid override', r.detect_key({'weird': 9}, r.CID_KEYS, 'weird', 'cid') == 9)
try:
    r.detect_key({'nope': 1}, r.CID_KEYS, None, 'cid')
    check('cid missing raises', False)
except r.BackendError:
    check('cid missing raises', True)

# --- get_or_make_chat (room activity path, like JoinRoomSession) ---
from types import SimpleNamespace

fake = SimpleNamespace(cid=CID, seat=None)
opts = SimpleNamespace(chat_id=None, room='12345', chatid_key=None,
                       client_version='554.0', chat_scheme='http',
                       chat_host='127.0.0.1:%d' % port,
                       chat_endpoint='/api/xmlrpc/chat.php', insecure=False)
info = {'clientSessionId': CSID, 'securityKey': KEY}
chat_id = r.get_or_make_chat(fake, info, opts)
check('get_or_make_chat room path', chat_id == 777)
sent = seen['params'][0]
check('room activity args',
      sent.get('activity') == 'publicroom-12345'
      and sent.get('chatId') == 0
      and sent.get('publicroom') is True
      and sent.get('private') is False)

# --- get_or_make_chat with --chat-id ---
fake2 = SimpleNamespace(cid=CID, seat=None)
opts2 = SimpleNamespace(chat_id='878029506', room=None, chatid_key=None,
                        client_version='554.0', chat_scheme='http',
                        chat_host='127.0.0.1:%d' % port,
                        chat_endpoint='/api/xmlrpc/chat.php', insecure=False)
chat_id = r.get_or_make_chat(fake2, info, opts2)
check('get_or_make_chat plain chat-id<|sep|> API', chat_id == '878029506')

# --- get_or_make_chat with --chat-id --register (does call the API) ---
opts3 = SimpleNamespace(chat_id='878029506', room=None, chatid_key=None,
                        register=True, client_version='554.0',
                        chat_scheme='http', chat_host='127.0.0.1:%d' % port,
                        chat_endpoint='/api/xmlrpc/chat.php', insecure=False)
chat_id = r.get_or_make_chat(fake2, info, opts3)
check('get_or_make_chat chat-id register path', chat_id == 777)
sent = seen['params'][0]
check('chat-id register args', sent.get('chatId') == 878029506
      and 'activity' not in sent)

# --- get_or_make_chat via agent invite ---
fake3 = SimpleNamespace(cid=CID, seat=None)
chat_id = r.get_or_make_chat(fake3, info, opts, invite=(777, 999))
check('get_or_make_chat invite path', chat_id == 777)
sent = seen['params'][0]
check('invite args',
      sent.get('chatId') == 777
      and sent.get('activity') == 'publicroom-12345'
      and 'fromUserId' not in sent
      and 'invite' not in sent)

server.shutdown()
server2.shutdown()
print('all xmlrpc tests passed')
