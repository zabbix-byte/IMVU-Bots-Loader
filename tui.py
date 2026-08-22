"""Textual TUI for roomload — neon lobby, no kilometric flags."""
import json
import os
import sys
import threading
import time

import roomload as r

CONFIG_NAME = '.roomload.json'
GITHUB = 'https://github.com/zabbix-byte'
DISCLAIMER = (
    'Educational use only. Not recommended for a real production environment.'
)
CREDIT = 'Created by ztrunk  ·  github.com/zabbix-byte'

MODES = (
    ('Join the room you typed', 'room'),
    ('Wait for your invite', 'invite'),
    ('In and out of the room', 'churn'),
)

SPAM_STYLES = (
    ('phrase — mix with unions', 'phrase'),
    ('word — one word each message', 'word'),
    ('raw — wordlist lines, no unions', 'raw'),
)


def norm_spam_style(style):
    if style in ('word', 'raw', 'phrase'):
        return style
    return 'phrase'

MODE_ALIASES = {'spam': 'room', 'listen': 'invite'}


def norm_mode(mode):
    mode = MODE_ALIASES.get(mode, mode)
    if mode in ('room', 'invite', 'churn'):
        return mode
    return 'room'

BANNER = (
    '[bold cyan]╦═╗╔═╗╔═╗╔╦╗╦  ╔═╗╔═╗╔╦╗[/]\n'
    '[bold magenta]╠╦╝║ ║║ ║║║║║  ║ ║╠═╣ ║║[/]\n'
    '[bold cyan]╩╚═╚═╝╚═╝╩ ╩╩═╝╚═╝╩ ╩═╩╝[/]'
)

def _crowd_art(width):
    """3-line stick crew. Fills the whole strip, never taller than 3."""
    if width < 5:
        return ''
    unit = ('  o  ', ' /|\\ ', ' / \\ ')
    n = max(1, width // 5)
    lines = []
    for i in range(3):
        lines.append((unit[i] * n)[:width].ljust(width))
    return (
        '[bold cyan]%s[/]\n'
        '[bold magenta]%s[/]\n'
        '[bold cyan]%s[/]'
        % (lines[0], lines[1], lines[2])
    )


def _config_path():
    here = os.path.dirname(os.path.abspath(r.__file__))
    return os.path.join(here, CONFIG_NAME)


def load_config():
    defaults = {
        'room': '',
        'count': 4,
        'mode': 'room',
        'trigger': 'go',
        'stop_trigger': 'st',
        'trigger_from': '',
        'insecure': True,
        'message': 'hola',
        'spam_delay': 0.2,
        'spam_delay_max': 0.4,
        'spam_auto': False,
        'spam_style': 'phrase',
        'proxy': False,
        'proxy_api': r.DEFAULT_PROXY_API,
    }
    path = _config_path()
    if not os.path.isfile(path):
        return defaults
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        defaults.update(data)
    except Exception:
        pass
    return defaults


def save_config(cfg):
    try:
        with open(_config_path(), 'w', encoding='utf-8') as f:
            json.dump(cfg, f, indent=2)
    except Exception:
        pass


def _wordlist_path(opts):
    return getattr(opts, 'wordlist', None) or os.path.join(
        os.path.dirname(os.path.abspath(r.__file__)), 'wordlist.txt')


def _read_wordlist_text(opts):
    path = _wordlist_path(opts)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception:
        return ''


def _unions_path(opts):
    return getattr(opts, 'unions', None) or r.unions_path(_wordlist_path(opts))


def _read_unions_text(opts):
    path = _unions_path(opts)
    r.ensure_unions_file(path)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception:
        return '\n'.join(r.DEFAULT_UNIONS) + '\n'


def account_total(opts):
    try:
        return len(r.load_accounts(opts.accounts, 0))
    except Exception:
        return 0


def _proxy_api_shown(cfg):
    val = (cfg.get('proxy_api') or '').strip()
    if not val or val == r.DEFAULT_PROXY_API:
        return ''
    return val


def apply_cfg(opts, cfg):
    opts.room = (cfg.get('room') or '').strip() or None
    opts.chat_id = None
    opts.count = int(cfg.get('count') or 0)
    opts.insecure = bool(cfg.get('insecure', True))
    opts.use_proxy = bool(cfg.get('proxy'))
    opts.proxy_api = (cfg.get('proxy_api') or r.DEFAULT_PROXY_API).strip()
    opts.message = cfg.get('message') or 'hola'
    opts.trigger = (cfg.get('trigger') or '').strip() or None
    opts.stop_trigger = (cfg.get('stop_trigger') or '').strip() or None
    opts.trigger_from = (cfg.get('trigger_from') or '').strip() or None
    mode = norm_mode(cfg.get('mode'))
    opts.listen_invite = mode == 'invite'
    opts.spam = True
    opts.churn = mode == 'churn'
    opts.spam_auto = bool(cfg.get('spam_auto'))
    if not opts.trigger:
        opts.spam_auto = True
    opts.spam_style = norm_spam_style(cfg.get('spam_style'))
    lo, hi = delay_range(cfg)
    opts.spam_delay = lo
    opts.spam_delay_max = hi
    opts.wordlist = _wordlist_path(opts)
    opts.unions = _unions_path(opts)
    r.ensure_unions_file(opts.unions)
    try:
        opts.union_words = r.load_unions(opts.unions)
    except Exception:
        opts.union_words = list(r.DEFAULT_UNIONS)
    opts.hold = 0
    opts.tui = True
    if not opts.ramp:
        opts.ramp = 0.35


def delay_range(cfg):
    try:
        lo = float(cfg.get('spam_delay') if cfg.get('spam_delay') not in (None, '') else 0.2)
    except (TypeError, ValueError):
        lo = 0.2
    try:
        raw = cfg.get('spam_delay_max')
        hi = float(raw if raw not in (None, '') else lo)
    except (TypeError, ValueError):
        hi = lo
    if lo < 0:
        lo = 0
    if hi < lo:
        hi = lo
    return lo, hi


def apply_live_cfg(opts, cfg):
    """Hot-apply form fields without tearing down a live run."""
    opts.trigger = (cfg.get('trigger') or '').strip() or None
    opts.stop_trigger = (cfg.get('stop_trigger') or '').strip() or None
    opts.trigger_from = (cfg.get('trigger_from') or '').strip() or None
    opts.insecure = bool(cfg.get('insecure', True))
    opts.spam_auto = bool(cfg.get('spam_auto'))
    if not opts.trigger:
        opts.spam_auto = True
    opts.spam_style = norm_spam_style(cfg.get('spam_style'))
    lo, hi = delay_range(cfg)
    opts.spam_delay = lo
    opts.spam_delay_max = hi
    opts.spam_tokens = None
    if getattr(opts, 'spam', False):
        r.attach_word_banks(opts)
    for session in getattr(opts, 'sessions', None) or []:
        if getattr(session, '_joining', False) or not session.imq:
            continue
        if r.still_in_chat(session) and session.chat_id:
            r.bind_imq_session(session, opts, r.chat_queue_name(session.chat_id))
        elif r.imq_alive(session):
            r.bind_imq_session(session, opts, None)


def _phase_mark(phase):
    if phase == 'spam':
        return '[bold magenta]●[/] {0}'
    if phase == 'in-room':
        return '[bold green]●[/] {0}'
    if phase == 'login':
        return '[bold yellow]●[/] {0}'
    if phase == 'down':
        return '[bold red]●[/] {0}'
    return '[dim]○[/] {0}'


def _session_phase(session):
    return r.effective_phase(session)


def _agent_label(session):
    phase = _session_phase(session)
    extra = {
        'spam': 'spam',
        'in-room': 'in room',
        'login': 'login',
        'down': 'offline',
        'idle': 'waiting',
    }.get(phase, phase)
    if phase == 'down' and session.error:
        err = str(session.error).strip()
        if err and err.lower() not in ('disconnected', 'down', 'offline'):
            extra = 'offline  %s' % err[:16]
    tone = 'red' if phase == 'down' else 'dim'
    pip = proxy_ip(session)
    if pip:
        extra = '%s  %s' % (extra, pip)
    return _phase_mark(phase).format(
        '%s  [%s]%s[/]' % (session.username, tone, extra))


def proxy_ip(session):
    px = getattr(session, 'proxy', None)
    return px.host if px else ''


def _wordlist_line_count(opts):
    words = getattr(opts, 'spam_words', None)
    if words:
        return len(words)
    return len(r.parse_wordlist_text(_read_wordlist_text(opts)))


def _union_count(opts):
    unions = getattr(opts, 'union_words', None)
    if unions:
        return len(unions)
    return len(r.parse_wordlist_text(_read_unions_text(opts)))


# Clicking a Windows console with Quick Edit on freezes the process
# until you press a key. Textual inputs then look "stuck".
_ENABLE_EXTENDED_FLAGS = 0x0080
_ENABLE_QUICK_EDIT_MODE = 0x0040
_ENABLE_MOUSE_INPUT = 0x0010


def disable_quick_edit():
    if sys.platform != 'win32':
        return None
    try:
        import ctypes
        from ctypes import wintypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-10)
        mode = wintypes.DWORD()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return None
        old = mode.value
        new = (old | _ENABLE_EXTENDED_FLAGS | _ENABLE_MOUSE_INPUT)
        new &= ~_ENABLE_QUICK_EDIT_MODE
        kernel32.SetConsoleMode(handle, new)
        return old
    except Exception:
        return None


def restore_console_mode(old):
    if old is None or sys.platform != 'win32':
        return
    try:
        import ctypes
        ctypes.windll.kernel32.SetConsoleMode(
            ctypes.windll.kernel32.GetStdHandle(-10), old)
    except Exception:
        pass


def run_tui(opts):
    try:
        from textual.app import App
        from textual.binding import Binding
        from textual.containers import Horizontal, Vertical, VerticalScroll
        from textual.screen import ModalScreen
        from textual.widgets import (
            Button, Checkbox, DirectoryTree, Input, Label, Log, Select,
            Static, TextArea,
        )
    except ImportError:
        print('TUI needs Textual. From this folder run:')
        print('  python -m venv .venv')
        print('  .venv\\Scripts\\pip install -r requirements.txt')
        print('  .venv\\Scripts\\python roomload.py')
        return 1

    class Brand(Horizontal):
        def compose(self):
            yield Static(BANNER, id='banner')
            yield Static('', id='brand-gap')

        def on_mount(self):
            self._paint_fill()

        def on_resize(self):
            self._paint_fill()

        def _paint_fill(self):
            try:
                gap = self.query_one('#brand-gap', Static)
            except Exception:
                return
            banner = self.query_one('#banner', Static)
            used = banner.size.width + 2
            inner = max(0, self.size.width - used)
            gap.update(_crowd_art(inner))

    class FilePickScreen(ModalScreen):
        BINDINGS = [
            Binding('escape', 'cancel', 'close', show=True),
        ]
        CSS = """
        FilePickScreen {
            align: center middle;
        }
        #pick-box {
            width: 86;
            max-width: 96%;
            height: 80%;
            background: #0b0f19;
            border: solid #2de2e6;
            padding: 1 2;
        }
        #pick-title { height: 1; color: #2de2e6; text-style: bold; }
        #pick-hint { height: 1; color: #6b7a94; margin-bottom: 1; }
        #pick-path { margin-bottom: 1; }
        #pick-tree {
            height: 1fr;
            background: #121826;
            border: none;
        }
        #pick-bar {
            height: 1;
            align: left middle;
            margin-top: 1;
        }
        #pick-err { color: #ff2a6d; width: 1fr; content-align: left middle; }
        #pick-load { background: #2de2e6; color: #0b0f19; }
        #pick-load:hover { background: #5aedf0; }
        #pick-cancel { background: #3a3144; color: #ff9ec0; }
        """

        def __init__(self, start_dir):
            super(FilePickScreen, self).__init__()
            self.start_dir = start_dir

        def compose(self):
            with Vertical(id='pick-box'):
                yield Static('import file', id='pick-title')
                yield Static(
                    'click a file or paste a path   ·   esc closes',
                    id='pick-hint')
                yield Input(placeholder='C:\\path\\list.txt', id='pick-path',
                            compact=True)
                yield DirectoryTree(self.start_dir, id='pick-tree')
                with Horizontal(id='pick-bar'):
                    yield Button('  load  ', id='pick-load', compact=True,
                                 flat=True)
                    yield Button('  cancel  ', id='pick-cancel', compact=True,
                                 flat=True)
                    yield Label('', id='pick-err')

        def on_directory_tree_file_selected(self, event):
            self.query_one('#pick-path', Input).value = str(event.path)

        def on_button_pressed(self, event):
            if event.button.id == 'pick-load':
                self.action_load()
            elif event.button.id == 'pick-cancel':
                self.action_cancel()

        def action_cancel(self):
            self.dismiss(None)

        def action_load(self):
            path = self.query_one('#pick-path', Input).value.strip()
            if not path:
                self.query_one('#pick-err', Label).update(
                    'pick a file or paste a path')
                return
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    text = f.read()
            except Exception as e:
                self.query_one('#pick-err', Label).update(str(e))
                return
            if not r.parse_wordlist_text(text):
                self.query_one('#pick-err', Label).update('that file is empty')
                return
            self.dismiss(text)

    class WordlistScreen(ModalScreen):
        BINDINGS = [
            Binding('escape', 'cancel', 'close', show=True),
        ]
        CSS = """
        WordlistScreen {
            align: center middle;
        }
        #wl-box {
            width: 86;
            max-width: 96%;
            height: 80%;
            background: #0b0f19;
            border: solid #2de2e6;
            padding: 1 2;
        }
        #wl-title { height: 1; color: #2de2e6; text-style: bold; }
        #wl-hint { height: 1; color: #6b7a94; margin-bottom: 1; }
        #kw-cols { height: 1fr; }
        #kw-left { width: 2fr; height: 1fr; margin-right: 1; }
        #kw-right { width: 1fr; height: 1fr; }
        #kw-words-lbl, #kw-unions-lbl { height: 1; color: #6b7a94; }
        #wl-editor, #un-editor {
            height: 1fr;
            background: #121826;
            border: none;
        }
        #wl-bar {
            height: 1;
            align: left middle;
            margin-top: 1;
        }
        #wl-err { color: #8b97ad; width: 1fr; content-align: left middle; }
        #wl-save { background: #2de2e6; color: #0b0f19; }
        #wl-save:hover { background: #5aedf0; }
        #wl-import { background: #1a2740; color: #2de2e6; }
        #wl-import:hover { background: #243656; }
        #wl-cancel { background: #3a3144; color: #ff9ec0; }
        """

        def __init__(self, opts):
            super(WordlistScreen, self).__init__()
            self.opts = opts

        def compose(self):
            with Vertical(id='wl-box'):
                yield Static('keywords', id='wl-title')
                yield Static(
                    'builds phrases like: prost de la olog   ·   esc closes',
                    id='wl-hint')
                with Horizontal(id='kw-cols'):
                    with Vertical(id='kw-left'):
                        yield Static('words  —  one per line', id='kw-words-lbl')
                        yield TextArea(
                            _read_wordlist_text(self.opts),
                            id='wl-editor', compact=True,
                            show_line_numbers=True, soft_wrap=False)
                    with Vertical(id='kw-right'):
                        yield Static('unions  —  și, de, la, cu…',
                                     id='kw-unions-lbl')
                        yield TextArea(
                            _read_unions_text(self.opts),
                            id='un-editor', compact=True,
                            show_line_numbers=True, soft_wrap=False)
                with Horizontal(id='wl-bar'):
                    yield Button('  save  ', id='wl-save', compact=True,
                                 flat=True)
                    yield Button('  import words  ', id='wl-import',
                                 compact=True, flat=True)
                    yield Button('  import unions  ', id='un-import',
                                 compact=True, flat=True)
                    yield Button('  cancel  ', id='wl-cancel', compact=True,
                                 flat=True)
                    yield Label('', id='wl-err')

        def on_button_pressed(self, event):
            if event.button.id == 'wl-save':
                self.action_save()
            elif event.button.id == 'wl-import':
                self.action_import('words')
            elif event.button.id == 'un-import':
                self.action_import('unions')
            elif event.button.id == 'wl-cancel':
                self.action_cancel()

        def action_import(self, target='words'):
            self._import_target = target
            start = os.path.dirname(_wordlist_path(self.opts))
            if not start or not os.path.isdir(start):
                start = os.getcwd()
            self.app.push_screen(FilePickScreen(start), self._imported)

        def _imported(self, text):
            if not text:
                return
            n = len(r.parse_wordlist_text(text))
            if getattr(self, '_import_target', 'words') == 'unions':
                self.query_one('#un-editor', TextArea).text = text
                self.query_one('#wl-err', Label).update(
                    'imported %d unions' % n)
            else:
                self.query_one('#wl-editor', TextArea).text = text
                self.query_one('#wl-err', Label).update(
                    'imported %d words' % n)

        def action_cancel(self):
            self.dismiss(False)

        def action_save(self):
            text = self.query_one('#wl-editor', TextArea).text
            unions = self.query_one('#un-editor', TextArea).text
            try:
                lines = r.save_wordlist(_wordlist_path(self.opts), text)
                joins = r.save_unions(_unions_path(self.opts), unions)
            except Exception as e:
                self.query_one('#wl-err', Label).update(str(e))
                return
            self.opts.spam_words = lines
            self.opts.union_words = joins
            self.dismiss(True)

    class AgentRow(Horizontal):
        def __init__(self, session, picked):
            super(AgentRow, self).__init__(classes='agent-row')
            self.username = session.username
            self.session = session
            self.picked = picked

        def compose(self):
            yield Checkbox('', value=self.picked, classes='agent-pick',
                           compact=True)
            yield Static(_agent_label(self.session), classes='agent-name')
            yield Button('chat', classes='agent-chat', compact=True, flat=True)

    class ChatScreen(ModalScreen):
        BINDINGS = [
            Binding('escape', 'cancel', 'close', show=True),
        ]
        CSS = """
        ChatScreen {
            align: center middle;
        }
        #chat-box {
            width: 86;
            max-width: 96%;
            height: 80%;
            background: #0b0f19;
            border: solid #2de2e6;
            padding: 1 2;
        }
        #chat-title { height: 1; color: #2de2e6; text-style: bold; }
        #chat-hint { height: 1; color: #6b7a94; margin-bottom: 1; }
        #chat-log {
            height: 1fr;
            border: none;
            background: #101623;
            padding: 0 1;
        }
        #chat-send-row, #chat-bar {
            height: 1;
            align: left middle;
            margin-top: 1;
        }
        #chat-in { width: 1fr; background: #121826; margin-right: 1; }
        #chat-send { background: #2de2e6; color: #0b0f19; min-width: 10; }
        #chat-send:hover { background: #5aedf0; }
        #chat-close { background: #3a3144; color: #ff9ec0; }
        #chat-err { color: #ff2a6d; width: 1fr; content-align: left middle; }
        """

        def __init__(self, opts, session):
            super(ChatScreen, self).__init__()
            self.opts = opts
            self.session = session
            self._seen = 0
            self._names_sig = None

        def compose(self):
            who = self.session.username
            cid = self.session.cid or '?'
            with Vertical(id='chat-box'):
                yield Static('chat  —  %s' % who, id='chat-title')
                yield Static(
                    'cid %s  ·  you speak only as this bot  ·  esc closes'
                    % cid, id='chat-hint')
                yield Log(id='chat-log', highlight=False)
                with Horizontal(id='chat-send-row'):
                    yield Input('', placeholder='message as %s' % who,
                                id='chat-in', compact=True)
                    yield Button('  send  ', id='chat-send', compact=True,
                                 flat=True)
                with Horizontal(id='chat-bar'):
                    yield Button('  close  ', id='chat-close', compact=True,
                                 flat=True)
                    yield Label('', id='chat-err')

        def on_mount(self):
            self._pump()
            self.set_interval(0.15, self._pump)
            self.query_one('#chat-in', Input).focus()

        def _names_stamp(self):
            store = getattr(self.opts, 'cid_names', None) or {}
            return tuple(sorted((str(k), str(v)) for k, v in store.items()))

        def _format_item(self, item):
            name = r.name_for_cid(
                self.opts, item.get('sender'), item.get('name'))
            text = item.get('text') or ''
            to = item.get('to') or 0
            if to:
                dest = r.name_for_cid(self.opts, to, str(to))
                return '[%s -> %s] %s' % (name, dest, text)
            return '[%s] %s' % (name, text)

        def _pump(self):
            stamp = self._names_stamp()
            if stamp != self._names_sig:
                self._names_sig = stamp
                if self._seen:
                    self.query_one('#chat-log', Log).clear()
                    self._seen = 0
            session = self.session
            lock = getattr(session, 'chat_lock', None)
            lines = getattr(session, 'chat_log', None) or []
            if lock is None:
                extra = [item for item in lines if item.get('seq', 0) > self._seen]
            else:
                with lock:
                    extra = [item for item in lines
                             if item.get('seq', 0) > self._seen]
            if not extra:
                return
            logw = self.query_one('#chat-log', Log)
            for item in extra:
                self._seen = max(self._seen, item.get('seq', 0))
                logw.write_line(self._format_item(item))

        def on_button_pressed(self, event):
            if event.button.id == 'chat-send':
                self.action_send()
            elif event.button.id == 'chat-close':
                self.action_cancel()

        def on_input_submitted(self, event):
            if event.input.id == 'chat-in':
                self.action_send()

        def action_cancel(self):
            self.dismiss(None)

        def action_send(self):
            box = self.query_one('#chat-in', Input)
            text = box.value
            err = r.send_manual_chat(self.session, self.opts, text)
            if err:
                self.query_one('#chat-err', Label).update(err)
                return
            box.value = ''
            self.query_one('#chat-err', Label).update('')
            self._pump()

    class RoomloadApp(App):
        TITLE = 'ROOMLOAD'
        ENABLE_COMMAND_PALETTE = False
        CSS = """
        Screen {
            background: #0b0f19;
            color: #d5def0;
        }
        #brand {
            height: 4;
            width: 100%;
            padding: 0 3;
            border-bottom: solid #1a2336;
        }
        #banner {
            height: 3;
            width: auto;
        }
        #brand-gap {
            width: 1fr;
            height: 3;
            overflow: hidden;
        }
        #body {
            height: 1fr;
            padding: 2 3 1 3;
        }
        #form {
            width: 46;
            min-width: 40;
            height: 1fr;
            margin-right: 2;
        }
        #side {
            width: 1fr;
            height: 1fr;
        }
        #side-title, #cids-label, #idle-label {
            height: 1;
            color: #5c6b84;
            margin-bottom: 1;
        }
        #cids-row, #idle-row {
            height: 1;
            margin-bottom: 1;
        }
        #cids, #idle-cids {
            width: 1fr;
            background: #121826;
            margin-right: 1;
        }
        #copy-cids, #copy-idle {
            background: #1a2740;
            color: #2de2e6;
            min-width: 10;
        }
        #copy-cids:hover, #copy-idle:hover { background: #243656; }
        .field-row {
            height: 1;
            margin-bottom: 1;
        }
        .lbl {
            width: 11;
            color: #6b7a94;
            content-align: left middle;
        }
        Input, Select, Checkbox, TextArea {
            background: #121826;
            border: none;
            padding: 0 1;
        }
        Input:focus, Select:focus, TextArea:focus {
            background: #172033;
        }
        #count, #delay, #delay-max { width: 7; }
        .delay-sep {
            width: 2;
            color: #6b7a94;
            content-align: center middle;
        }
        #trigger, #stop_trigger { width: 10; }
        #auto { width: auto; color: #6b7a94; }
        #wordlist {
            background: #1a2740;
            color: #2de2e6;
            min-width: 10;
            margin-right: 1;
        }
        #wordlist:hover { background: #243656; }
        #wl-count { width: 1fr; color: #6b7a94; content-align: left middle; }
        #agents-label { height: 1; color: #5c6b84; margin-top: 1; }
        #agents {
            height: 1fr;
            margin-top: 1;
            background: #121826;
            border: none;
        }
        .agent-row { height: 1; }
        .agent-pick { width: 4; }
        .agent-name { width: 1fr; color: #d5def0; content-align: left middle; }
        .agent-chat {
            background: #1a2740;
            color: #2de2e6;
            min-width: 8;
            margin-right: 0;
        }
        .agent-chat:hover { background: #243656; }
        #live-actions {
            height: 1;
            align: left middle;
            margin-top: 1;
        }
        #spam-go {
            background: #c026d3;
            color: #0b0f19;
        }
        #spam-go:hover { background: #e040fb; }
        #spam-stop {
            background: #3a3144;
            color: #ff9ec0;
        }
        #spam-stop:hover { background: #4a3d55; }
        #drop-row {
            height: 1;
            align: left middle;
            margin-top: 1;
        }
        #drop, #drop-all {
            background: #3a3144;
            color: #ff9ec0;
            margin-top: 0;
        }
        #drop-all:hover { background: #4a3d55; }
        #actions {
            height: 1;
            align: left middle;
            margin-top: 0;
            margin-bottom: 1;
        }
        Button {
            height: 1;
            min-width: 12;
            width: auto;
            border: none;
            padding: 0 2;
            margin-right: 2;
            text-style: bold;
        }
        #start {
            background: #2de2e6;
            color: #0b0f19;
        }
        #start:hover { background: #5aedf0; }
        #stop {
            background: #ff2a6d;
            color: #0b0f19;
        }
        #stop:hover { background: #ff5a8c; }
        #flash { color: #ff2a6d; width: 1fr; content-align: left middle; }
        #insecure, #use-proxy {
            width: auto;
            color: #8b97ad;
            margin-right: 2;
        }
        #proxy-api { width: 1fr; }
        #proxy-stat {
            width: auto;
            color: #5c6b84;
            content-align: left middle;
            margin-left: 1;
        }
        #stats { height: 1; color: #5c6b84; margin-top: 2; }
        #bots { height: auto; color: #8b97ad; }
        #log {
            height: 1fr;
            border: none;
            background: #101623;
            padding: 0 1;
        }
        #foot {
            dock: bottom;
            height: 1;
            color: #4a5568;
            background: #0b0f19;
            padding: 0 3;
        }
        .hidden { display: none; }
        """
        BINDINGS = [
            Binding('q', 'quit_app', 'quit', show=True),
            Binding('s', 'stop_run', 'stop', show=True),
            Binding('f5', 'start_run', 'start', show=True),
            Binding('g', 'spam_go', 'spam', show=True),
            Binding('t', 'spam_stop', 'stop spam', show=True),
        ]

        def __init__(self, opts):
            super(RoomloadApp, self).__init__()
            self.opts = opts
            self.cfg = load_config()
            if opts.room:
                self.cfg['room'] = opts.room
            if opts.count:
                self.cfg['count'] = opts.count
            if opts.trigger:
                self.cfg['trigger'] = opts.trigger
            if opts.trigger_from:
                self.cfg['trigger_from'] = opts.trigger_from
            if opts.insecure:
                self.cfg['insecure'] = True
            self._thread = None
            self._started = 0
            self._pending = []
            self._lock = threading.Lock()
            self._agent_sig = None
            self._cid_sig = None
            self._idle_sig = None

        def compose(self):
            yield Brand(id='brand')
            with Horizontal(id='body'):
                with Vertical(id='form'):
                    with Horizontal(classes='field-row'):
                        yield Label('room', classes='lbl')
                        yield Input(self.cfg.get('room') or '',
                                    placeholder='ownerId-roomId', id='room',
                                    compact=True)
                    with Horizontal(classes='field-row'):
                        yield Label('accounts', classes='lbl')
                        yield Input(str(self.cfg.get('count') or 4),
                                    placeholder='all', id='count', compact=True)
                    with Horizontal(classes='field-row'):
                        yield Label('how', classes='lbl')
                        yield Select(MODES, value=norm_mode(self.cfg.get('mode')),
                                     allow_blank=False, id='mode', compact=True)
                    with Horizontal(classes='field-row'):
                        yield Label('go', classes='lbl')
                        yield Input(self.cfg.get('trigger') or '',
                                    placeholder='off', id='trigger',
                                    compact=True)
                        yield Label('stop', classes='lbl')
                        yield Input(self.cfg.get('stop_trigger') or '',
                                    placeholder='off', id='stop_trigger',
                                    compact=True)
                    with Horizontal(classes='field-row'):
                        yield Label('from cid', classes='lbl')
                        yield Input(self.cfg.get('trigger_from') or '',
                                    placeholder='empty = any cid',
                                    id='trigger_from', compact=True)
                    with Horizontal(classes='field-row'):
                        yield Label('delay', classes='lbl')
                        yield Input(str(self.cfg.get('spam_delay') or 0.2),
                                    placeholder='min', id='delay', compact=True)
                        yield Static('–', classes='delay-sep')
                        yield Input(str(self.cfg.get('spam_delay_max')
                                        or self.cfg.get('spam_delay')
                                        or 0.4),
                                    placeholder='max', id='delay-max',
                                    compact=True)
                    with Horizontal(classes='field-row'):
                        yield Label('send', classes='lbl')
                        yield Select(
                            SPAM_STYLES,
                            value=norm_spam_style(self.cfg.get('spam_style')),
                            allow_blank=False, id='spam_style', compact=True)
                    with Horizontal(classes='field-row'):
                        yield Label('', classes='lbl')
                        yield Checkbox(
                            'start loop now',
                            value=bool(self.cfg.get('spam_auto')),
                            id='auto', compact=True)
                    with Horizontal(classes='field-row'):
                        yield Label('words', classes='lbl')
                        yield Button('  edit  ', id='wordlist', compact=True,
                                     flat=True)
                        yield Static('', id='wl-count')
                    with Horizontal(id='actions', classes='field-row'):
                        yield Label('run', classes='lbl')
                        yield Button('  start  ', id='start', compact=True,
                                     flat=True)
                        yield Button('  stop  ', id='stop', compact=True,
                                     flat=True, disabled=True)
                    with Horizontal(classes='field-row'):
                        yield Label('net', classes='lbl')
                        yield Checkbox(
                            'skip TLS',
                            value=bool(self.cfg.get('insecure', True)),
                            id='insecure', compact=True)
                        yield Checkbox(
                            'proxies',
                            value=bool(self.cfg.get('proxy')),
                            id='use-proxy', compact=True)
                    with Horizontal(classes='field-row'):
                        yield Label('list', classes='lbl')
                        yield Input(
                            _proxy_api_shown(self.cfg),
                            placeholder='empty = hunt socks5 via api',
                            id='proxy-api', compact=True)
                        yield Static('', id='proxy-stat')
                    yield Label('', id='flash')
                    yield Static('', id='stats')
                    yield Static('agents', id='agents-label')
                    yield VerticalScroll(id='agents')
                    with Horizontal(id='live-actions'):
                        yield Button('  spam  ', id='spam-go', compact=True,
                                     flat=True, disabled=True)
                        yield Button('  stop spam  ', id='spam-stop',
                                     compact=True, flat=True, disabled=True)
                    with Horizontal(id='drop-row'):
                        yield Button('  disconnect  ', id='drop', compact=True,
                                     flat=True, disabled=True)
                        yield Button('  disconnect all  ', id='drop-all',
                                     compact=True, flat=True, disabled=True)
                with Vertical(id='side'):
                    yield Static('cids', id='cids-label')
                    with Horizontal(id='cids-row'):
                        yield Input('', placeholder='id,id,id', id='cids',
                                    compact=True)
                        yield Button('  copy  ', id='copy-cids', compact=True,
                                     flat=True)
                    yield Static('waiting  —  re-invite these', id='idle-label')
                    with Horizontal(id='idle-row'):
                        yield Input('', placeholder='id,id,id', id='idle-cids',
                                    compact=True)
                        yield Button('  copy  ', id='copy-idle', compact=True,
                                     flat=True)
                    yield Static('activity', id='side-title')
                    yield Log(id='log', highlight=False)
            yield Static(
                CREDIT + '   ·   ' + DISCLAIMER,
                id='foot',
            )

        def on_mount(self):
            self._refresh_wl_count()
            self.set_interval(0.1, self._tick)

        def _refresh_wl_count(self):
            n = _wordlist_line_count(self.opts)
            u = _union_count(self.opts)
            self.query_one('#wl-count', Static).update(
                '%d words · %d unions' % (n, u))

        def _read_form(self):
            self.cfg['room'] = self.query_one('#room', Input).value.strip()
            raw = self.query_one('#count', Input).value.strip() or '0'
            try:
                self.cfg['count'] = max(0, int(raw))
            except ValueError:
                return 'count must be a number'
            self.cfg['mode'] = norm_mode(self.query_one('#mode', Select).value)
            self.cfg['trigger'] = self.query_one('#trigger', Input).value.strip()
            self.cfg['stop_trigger'] = (
                self.query_one('#stop_trigger', Input).value.strip())
            self.cfg['trigger_from'] = (
                self.query_one('#trigger_from', Input).value.strip())
            self.cfg['insecure'] = self.query_one('#insecure', Checkbox).value
            self.cfg['proxy'] = self.query_one('#use-proxy', Checkbox).value
            self.cfg['proxy_api'] = (
                self.query_one('#proxy-api', Input).value.strip()
                or r.DEFAULT_PROXY_API)
            self.cfg['spam_auto'] = self.query_one('#auto', Checkbox).value
            self.cfg['spam_style'] = norm_spam_style(
                self.query_one('#spam_style', Select).value)
            try:
                lines = r.load_wordlist(_wordlist_path(self.opts))
            except Exception as e:
                return str(e)
            self.opts.spam_words = lines
            self.opts.spam_tokens = None
            upath = _unions_path(self.opts)
            r.ensure_unions_file(upath)
            self.opts.unions = upath
            self.opts.union_words = r.load_unions(upath)
            raw_lo = self.query_one('#delay', Input).value.strip() or '0.2'
            raw_hi = self.query_one('#delay-max', Input).value.strip() or raw_lo
            try:
                lo = float(raw_lo)
                hi = float(raw_hi)
            except ValueError:
                return 'delay must be two numbers (seconds)'
            if lo < 0 or hi < 0:
                return 'delay cannot be negative'
            if hi < lo:
                lo, hi = hi, lo
            self.cfg['spam_delay'] = lo
            self.cfg['spam_delay_max'] = hi
            save_config(self.cfg)
            if self._thread and self._thread.is_alive():
                apply_live_cfg(self.opts, self.cfg)
            return ''

        def _set_live(self, live):
            self.query_one('#start', Button).disabled = live
            self.query_one('#stop', Button).disabled = not live
            self.query_one('#drop', Button).disabled = not live
            self.query_one('#drop-all', Button).disabled = not live
            self.query_one('#spam-go', Button).disabled = not live
            self.query_one('#spam-stop', Button).disabled = not live

        def _flash(self, text):
            self.query_one('#flash', Label).update(text)

        def action_start_run(self):
            err = self._read_form()
            if err:
                self._flash(err)
                return
            apply_cfg(self.opts, self.cfg)
            if not self.opts.listen_invite and not self.opts.room:
                self._flash('type the room they should enter')
                return
            if account_total(self.opts) == 0:
                self._flash('no accounts in accounts.txt')
                return
            if self.opts.use_proxy:
                self._flash('hunting proxies via api…')
            else:
                self._flash('')
            with self._lock:
                self._pending = []
            self.query_one('#log', Log).clear()
            self._started = time.time()
            self._agent_sig = None
            self._cid_sig = None
            self._idle_sig = None
            self._set_live(True)

            def hook(line):
                with self._lock:
                    self._pending.append(line)
                    del self._pending[:-80]

            r._log_hook = hook

            def worker():
                try:
                    r.run_load(self.opts)
                finally:
                    r._log_hook = None

            self._thread = threading.Thread(target=worker, daemon=True)
            self._thread.start()

        def action_stop_run(self):
            ev = getattr(self.opts, 'stop_event', None)
            if ev:
                ev.set()

        def action_quit_app(self):
            self.action_stop_run()
            self.exit(0)

        def action_edit_wordlist(self):
            self.push_screen(WordlistScreen(self.opts), self._wordlist_closed)

        def _wordlist_closed(self, saved):
            self._refresh_wl_count()
            if saved:
                self._flash('')
                if self._thread and self._thread.is_alive():
                    self._read_form()

        def _picked_agents(self):
            names = []
            try:
                rows = self.query(AgentRow)
            except Exception:
                return names
            for row in rows:
                try:
                    if row.query_one('.agent-pick', Checkbox).value:
                        names.append(row.username)
                except Exception:
                    continue
            return names

        def _in_room(self):
            return [s for s in (getattr(self.opts, 'sessions', None) or [])
                    if r.still_in_chat(s)]

        def action_spam_go(self):
            if not self._thread or not self._thread.is_alive():
                self._flash('start the run first')
                return
            err = self._read_form()
            if err:
                self._flash(err)
                return
            ready = self._in_room()
            if not ready:
                self._flash('no agents in room')
                return
            r.start_spam_all(self.opts)
            self._flash('spam %d' % len(ready))

        def action_spam_stop(self):
            if not self._thread or not self._thread.is_alive():
                self._flash('start the run first')
                return
            err = self._read_form()
            if err:
                self._flash(err)
                return
            r.stop_spam_all(self.opts)
            self._flash('spam stopped')

        def action_drop_selected(self):
            sessions = getattr(self.opts, 'sessions', None) or []
            names = set(self._picked_agents())
            if not names:
                self._flash('select one or more agents')
                return
            n = 0
            for session in sessions:
                if session.username in names and not session.halt.is_set():
                    r.wait_for_invite(session, self.opts, 'disconnected')
                    n += 1
            self._flash('disconnected %d' % n)

        def action_disconnect_all(self):
            sessions = getattr(self.opts, 'sessions', None) or []
            n = 0
            for session in sessions:
                if session.halt.is_set():
                    continue
                r.wait_for_invite(session, self.opts, 'disconnected')
                n += 1
            if not n:
                self._flash('no agents to disconnect')
                return
            self._flash('disconnected all %d  —  waiting' % n)

        def action_open_chat(self, username):
            sessions = getattr(self.opts, 'sessions', None) or []
            for session in sessions:
                if session.username == username:
                    self.push_screen(ChatScreen(self.opts, session))
                    return
            self._flash('agent gone')

        def on_select_changed(self, event):
            if (event.select.id in ('spam_style',)
                    and self._thread and self._thread.is_alive()):
                self._read_form()

        def on_checkbox_changed(self, event):
            if (event.checkbox.id in ('auto', 'insecure', 'use-proxy')
                    and self._thread and self._thread.is_alive()):
                self._read_form()

        def on_input_submitted(self, event):
            if (event.input.id in ('delay', 'delay-max', 'trigger',
                                   'stop_trigger', 'trigger_from')
                    and self._thread and self._thread.is_alive()):
                err = self._read_form()
                if err:
                    self._flash(err)

        def on_button_pressed(self, event):
            if event.button.id == 'start':
                self.action_start_run()
            elif event.button.id == 'stop':
                self.action_stop_run()
            elif event.button.id == 'drop':
                self.action_drop_selected()
            elif event.button.id == 'drop-all':
                self.action_disconnect_all()
            elif event.button.id == 'spam-go':
                self.action_spam_go()
            elif event.button.id == 'spam-stop':
                self.action_spam_stop()
            elif event.button.id == 'wordlist':
                self.action_edit_wordlist()
            elif event.button.id == 'copy-cids':
                self.action_copy_cids()
            elif event.button.id == 'copy-idle':
                self.action_copy_idle()
            elif 'agent-chat' in event.button.classes:
                row = event.button.parent
                username = getattr(row, 'username', None)
                if username:
                    self.action_open_chat(username)

        def _copy_text(self, text, empty_msg):
            text = (text or '').strip()
            if not text:
                self._flash(empty_msg)
                return
            self.copy_to_clipboard(text)
            if sys.platform == 'win32':
                try:
                    import subprocess
                    proc = subprocess.Popen(
                        ['clip'], stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    proc.communicate(text.encode('ascii'))
                except Exception:
                    pass
            self._flash('copied %d cids' % (text.count(',') + 1))

        def action_copy_cids(self):
            self._copy_text(self.query_one('#cids', Input).value, 'no cids yet')

        def action_copy_idle(self):
            self._copy_text(
                self.query_one('#idle-cids', Input).value, 'no waiting cids')

        def _tick(self):
            with self._lock:
                extra = self._pending[:]
                del self._pending[:]
            logw = self.query_one('#log', Log)
            for line in extra:
                logw.write_line(line)
            thread = self._thread
            if not thread:
                return
            if not thread.is_alive():
                self._set_live(False)
            pool = getattr(self.opts, 'proxy_pool', None)
            if getattr(self.opts, 'use_proxy', False) and pool:
                pstat = pool.status_line()
            elif getattr(self.opts, 'use_proxy', False):
                pstat = '…'
            else:
                pstat = ''
            if pstat != getattr(self, '_proxy_stat_sig', None):
                self._proxy_stat_sig = pstat
                self.query_one('#proxy-stat', Static).update(pstat)
            stats = getattr(self.opts, 'stats', None) or {}
            sessions = getattr(self.opts, 'sessions', None) or []
            for session in sessions:
                r.reconcile_session(session, self.opts)
            joined = sum(1 for s in sessions
                         if _session_phase(s) in ('in-room', 'spam'))
            waiting = sum(1 for s in sessions if _session_phase(s) == 'idle')
            offline = sum(1 for s in sessions if _session_phase(s) == 'down')
            spam = sum(1 for s in sessions if _session_phase(s) == 'spam')
            elapsed = int(time.time() - self._started) if self._started else 0
            mps = r.spam_mps(self.opts)
            self.query_one('#stats', Static).update(
                '[dim]%ds[/]  in %d  wait %d  off %d  spam %d  sent %d  [bold]%s/s[/]'
                % (elapsed, joined, waiting, offline, spam,
                   stats.get('sent', 0),
                   ('%.1f' % mps) if mps else '0.0')
            )
            cids = []
            idle = []
            for session in sessions:
                if not session.cid or session.halt.is_set():
                    continue
                cid = str(session.cid)
                cids.append(cid)
                if r.can_accept_invite(session):
                    idle.append(cid)
            cid_text = ','.join(cids)
            idle_text = ','.join(idle)
            if cid_text != self._cid_sig:
                self._cid_sig = cid_text
                box = self.query_one('#cids', Input)
                if self.focused is not box:
                    box.value = cid_text
            if idle_text != self._idle_sig:
                self._idle_sig = idle_text
                self.query_one('#idle-cids', Input).value = idle_text
            live = [s for s in sessions if not s.halt.is_set()]
            sig = tuple(
                (s.username, _session_phase(s), bool(s.chat_id),
                 r.imq_alive(s), r.spam_loop_alive(s),
                 bool(getattr(s, '_left_noted', False)),
                 (s.error or '')[:24], proxy_ip(s))
                for s in live
            )
            if sig == self._agent_sig:
                return
            self._agent_sig = sig
            chosen = set(self._picked_agents())
            agents = self.query_one('#agents', VerticalScroll)
            agents.remove_children()
            for session in live:
                agents.mount(AgentRow(session, session.username in chosen))

    old_mode = disable_quick_edit()
    app = RoomloadApp(opts)
    try:
        app.run()
    finally:
        restore_console_mode(old_mode)
    ev = getattr(opts, 'stop_event', None)
    if ev:
        ev.set()
    return 0


if __name__ == '__main__':
    sys.exit(r.main(['--menu']))
