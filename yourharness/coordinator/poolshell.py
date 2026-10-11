"""The pool shell: YourHarness OS's own terminal.

It isn't tied to any computer. It keeps your current folder inside /pool and your
variables, and sends every command to the least-busy computer (see execs.py). After
each command it says where it ran. Files the command made are saved to /pool, so the
next command finds them on whichever computer it lands on.

Line editing happens here, so it works the same from a Mac, a PC, an iPad or a phone.
Commands run without keyboard input; for something interactive (an editor, htop) open a
direct terminal on one computer.
"""

from __future__ import annotations

import base64
import re
import secrets
import shlex
from typing import TYPE_CHECKING

from .jobkinds import UserError
from .pool_store import clean_path

if TYPE_CHECKING:
    from .scheduler import Device, Scheduler

C = {"reset": "\x1b[0m", "dim": "\x1b[2m", "bold": "\x1b[1m", "green": "\x1b[1;32m", "blue": "\x1b[1;34m", "yellow": "\x1b[1;33m", "red": "\x1b[31m", "cyan": "\x1b[36m"}
NEWLINE = re.compile(rb"(?<!\r)\n")
HELP = f"""{C['bold']}YourHarness OS shell{C['reset']}
  Files you make live in {C['cyan']}/pool{C['reset']} and follow you to whichever computer runs the next command.
  Each command runs on the least-busy computer and the line after it says where.

  {C['bold']}on{C['reset']} <computer|linux|mac> <command>   run on a specific computer
  {C['bold']}par{C['reset']} <command with {{}}> ::: a b c       run once per item, spread over the computers
  {C['bold']}runners{C['reset']}                              the computers that can run commands, and how busy they are
  {C['bold']}ps{C['reset']}                                   commands running or finished recently, and where
  {C['bold']}cd{C['reset']}, {C['bold']}pwd{C['reset']}, {C['bold']}export{C['reset']} NAME=value, {C['bold']}clear{C['reset']}, {C['bold']}exit{C['reset']}, {C['bold']}help{C['reset']}
  Ctrl-C stops the running command. Tab completes names in /pool.
"""


def enc(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


class Session:
    def __init__(self, sid: str, browser: "Device", cols: int):
        self.sid, self.browser, self.cols = sid, browser, cols
        self.cwd = ""
        self.env: dict[str, str] = {}
        self.buf: list[str] = []
        self.cur = 0
        self.history: list[str] = []
        self.hpos = 0
        self.draft = ""
        self.esc = ""
        self.running: set[str] = set()  # ids of commands in flight
        self.waiting_note = False
        self.par: dict | None = None
        self.prompted = False  # the prompt is already on screen


class PoolShell:
    def __init__(self, sched: "Scheduler"):
        self.s = sched
        self.sessions: dict[str, Session] = {}

    # ------------------------------------------------------------------ plumbing
    def _out(self, sess: Session, text: str | bytes) -> None:
        if isinstance(text, bytes):
            sess.browser.send({"type": "os.out", "sid": sess.sid, "data": base64.b64encode(NEWLINE.sub(b"\r\n", text)).decode("ascii")})
        else:
            sess.browser.send({"type": "os.out", "sid": sess.sid, "data": enc(text)})

    def _prompt(self, sess: Session) -> str:
        where = "/pool" + (f"/{sess.cwd}" if sess.cwd else "")
        return f"{C['green']}pool{C['reset']}:{C['blue']}{where}{C['reset']}$ "

    def _redraw(self, sess: Session) -> None:
        line = "".join(sess.buf)
        back = len(sess.buf) - sess.cur
        self._out(sess, "\r" + self._prompt(sess) + line + "\x1b[K" + (f"\x1b[{back}D" if back else ""))

    def _new_prompt(self, sess: Session) -> None:
        sess.buf, sess.cur = [], 0
        sess.prompted = True
        self._out(sess, self._prompt(sess))

    # ------------------------------------------------------------------ browser messages
    def from_browser(self, browser: "Device", msg: dict) -> None:
        kind = msg.get("type")
        if kind == "os.open":
            sid = secrets.token_hex(8)
            sess = Session(sid, browser, int(msg.get("cols") or 80))
            self.sessions[sid] = sess
            browser.send({"type": "os.opened", "sid": sid, "ref": msg.get("ref")})
            runners = self.s.execs.runners()
            linux = [d for d in runners if d.exec.get("linux")]
            self._out(sess, f"{C['bold']}YourHarness OS{C['reset']}  {C['dim']}one shell for every computer in the pool. Type {C['reset']}help{C['dim']} for commands.{C['reset']}\r\n")
            if not runners:
                self._out(sess, f"{C['yellow']}No computer can run commands yet.{C['reset']} Open the YourHarness app on a Mac or Windows PC.\r\n")
            else:
                names = ", ".join(d.name for d in (linux or runners))
                note = "" if linux else f" {C['dim']}(no Linux computer is connected, so commands run on macOS for now){C['reset']}"
                self._out(sess, f"{C['dim']}Commands run on: {names}{C['reset']}{note}\r\n")
            self._new_prompt(sess)
            return
        sess = self.sessions.get(str(msg.get("sid")))
        if sess is None or sess.browser.id != browser.id:
            return
        if kind == "os.in" and isinstance(msg.get("data"), str) and len(msg["data"]) <= 65536:
            self._feed(sess, msg["data"])
        elif kind == "os.resize":
            sess.cols = int(msg.get("cols") or sess.cols)
        elif kind == "os.close":
            self._close(sess)

    def _close(self, sess: Session) -> None:
        for pid in list(sess.running):
            self.s.execs.cancel(pid)
        self.sessions.pop(sess.sid, None)

    def device_gone(self, d: "Device") -> None:
        for sess in [x for x in self.sessions.values() if x.browser.id == d.id]:
            self._close(sess)

    # ------------------------------------------------------------------ line editing
    def _feed(self, sess: Session, data: str) -> None:
        for ch in data:
            if sess.esc:
                sess.esc += ch
                if self._escape(sess):
                    sess.esc = ""
                continue
            if ch == "\x1b":
                sess.esc = ch
                continue
            if sess.running:
                if ch == "\x03":
                    for pid in list(sess.running):
                        self.s.execs.cancel(pid)
                    self._out(sess, f"^C\r\n")
                continue
            if ch in "\r\n":
                self._out(sess, "\r\n")
                line = "".join(sess.buf).strip()
                sess.buf, sess.cur = [], 0
                sess.prompted = False
                if line:
                    if not sess.history or sess.history[-1] != line:
                        sess.history.append(line)
                        del sess.history[:-200]
                    sess.hpos = len(sess.history)
                    self._run_line(sess, line)
                    if sess.running or sess.prompted:
                        continue
                self._new_prompt(sess)
            elif ch in "\x7f\x08":
                if sess.cur:
                    del sess.buf[sess.cur - 1]
                    sess.cur -= 1
                    self._redraw(sess)
            elif ch == "\x03":
                self._out(sess, "^C\r\n")
                self._new_prompt(sess)
            elif ch == "\x04":
                if not sess.buf:
                    self._out(sess, f"{C['dim']}logout{C['reset']}\r\n")
                    sess.browser.send({"type": "os.exit", "sid": sess.sid})
                    self._close(sess)
                    return
            elif ch == "\x0c":
                self._out(sess, "\x1b[2J\x1b[H")
                self._redraw(sess)
            elif ch == "\x15":
                del sess.buf[:sess.cur]
                sess.cur = 0
                self._redraw(sess)
            elif ch == "\x0b":
                del sess.buf[sess.cur:]
                self._redraw(sess)
            elif ch == "\x17":
                k = sess.cur
                while k and sess.buf[k - 1] == " ":
                    k -= 1
                while k and sess.buf[k - 1] != " ":
                    k -= 1
                del sess.buf[k:sess.cur]
                sess.cur = k
                self._redraw(sess)
            elif ch == "\x01":
                sess.cur = 0
                self._redraw(sess)
            elif ch == "\x05":
                sess.cur = len(sess.buf)
                self._redraw(sess)
            elif ch == "\t":
                self._complete(sess)
            elif ch >= " ":
                sess.buf.insert(sess.cur, ch)
                sess.cur += 1
                if sess.cur == len(sess.buf):
                    self._out(sess, ch)
                else:
                    self._redraw(sess)

    def _escape(self, sess: Session) -> bool:
        """Handle an escape sequence once it's complete. Returns True when it is."""
        seq = sess.esc
        if len(seq) < 2:
            return False
        if seq[1] not in "[O":
            return True  # an Alt key or something we don't use
        if len(seq) < 3:
            return False
        last = seq[-1]
        if seq[1] == "[" and not (last.isalpha() or last == "~"):
            return False
        if sess.running:
            return True
        if last == "A" and sess.history:
            if sess.hpos == len(sess.history):
                sess.draft = "".join(sess.buf)
            sess.hpos = max(0, sess.hpos - 1)
            sess.buf = list(sess.history[sess.hpos])
            sess.cur = len(sess.buf)
            self._redraw(sess)
        elif last == "B" and sess.history:
            sess.hpos = min(len(sess.history), sess.hpos + 1)
            sess.buf = list(sess.draft if sess.hpos == len(sess.history) else sess.history[sess.hpos])
            sess.cur = len(sess.buf)
            self._redraw(sess)
        elif last == "C" and sess.cur < len(sess.buf):
            sess.cur += 1
            self._out(sess, "\x1b[C")
        elif last == "D" and sess.cur:
            sess.cur -= 1
            self._out(sess, "\x1b[D")
        elif last in "H" or seq.endswith(("1~", "7~")):
            sess.cur = 0
            self._redraw(sess)
        elif last == "F" or seq.endswith(("4~", "8~")):
            sess.cur = len(sess.buf)
            self._redraw(sess)
        elif seq.endswith("3~") and sess.cur < len(sess.buf):
            del sess.buf[sess.cur]
            self._redraw(sess)
        return True

    # ------------------------------------------------------------------ tab completion in /pool
    def _complete(self, sess: Session) -> None:
        text = "".join(sess.buf[:sess.cur])
        m = re.search(r"((?:\\ |[^ ])*)$", text)
        word = m.group(1) if m else ""
        raw = word.replace("\\ ", " ")
        folder, _, prefix = raw.rpartition("/")
        try:
            target = clean_path(folder[len("/pool"):]) if raw.startswith("/pool") else clean_path("/".join(x for x in (sess.cwd, folder) if x))
            entries = self.s.pool_store.list_dir(target)
        except UserError:
            return
        hits = [e for e in entries if e["name"].startswith(prefix) and (prefix or not e["name"].startswith("."))]
        if not hits:
            return
        names = [e["name"] + ("/" if e["dir"] else "") for e in hits]
        common = names[0]
        for n in names[1:]:
            while not n.startswith(common):
                common = common[:-1]
        add = common[len(prefix):]
        if len(hits) == 1 and not hits[0]["dir"]:
            add += " "
        if add:
            ins = add.replace(" ", "\\ ") if add != " " else " "
            sess.buf[sess.cur:sess.cur] = list(ins)
            sess.cur += len(ins)
            self._redraw(sess)
        elif len(hits) > 1:
            self._out(sess, "\r\n" + "  ".join(names) + "\r\n")
            self._redraw(sess)

    # ------------------------------------------------------------------ running lines
    def _run_line(self, sess: Session, line: str) -> None:
        try:
            first = line.split(None, 1)[0]
            rest = line[len(first):].strip()
            handler = {
                "help": self._help, "clear": self._clear, "pwd": self._pwd, "cd": self._cd, "export": self._export,
                "runners": self._runners, "ps": self._ps, "exit": self._exit, "logout": self._exit,
            }.get(first)
            # `cd x && ls` or `pwd; ls` are whole command lines for the runner, which reports where it ended up
            compound = re.search(r"[;&|<>`$(){}\n]", rest) is not None
            if handler and not (compound and first in ("cd", "pwd", "export")):
                handler(sess, rest)
            elif first == "on":
                self._on(sess, rest)
            elif first == "par":
                self._par(sess, rest)
            else:
                self._exec(sess, line, None)
        except UserError as e:
            self._out(sess, f"{C['red']}{e}{C['reset']}\r\n")
        except ValueError as e:  # shlex: an unclosed quote
            self._out(sess, f"{C['red']}Can't read that line: {e}.{C['reset']}\r\n")

    def _help(self, sess, rest):
        self._out(sess, HELP.replace("\n", "\r\n"))

    def _clear(self, sess, rest):
        self._out(sess, "\x1b[2J\x1b[H")

    def _pwd(self, sess, rest):
        self._out(sess, "/pool" + (f"/{sess.cwd}" if sess.cwd else "") + "\r\n")

    def _exit(self, sess, rest):
        self._out(sess, f"{C['dim']}logout{C['reset']}\r\n")
        sess.browser.send({"type": "os.exit", "sid": sess.sid})
        self._close(sess)

    def _cd(self, sess, rest):
        arg = shlex.split(rest)[0] if rest else ""
        if arg in ("", "~", "/pool", "/"):
            sess.cwd = ""
            return
        base = "" if arg.startswith("/pool") or arg.startswith("/") else sess.cwd
        arg = arg[len("/pool"):] if arg.startswith("/pool") else arg
        parts = []
        for part in "/".join(x for x in (base, arg) if x).split("/"):
            if part in ("", "."):
                continue
            if part == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(part)
        target = "/".join(parts)
        if target and not self.s.pool_store.is_dir(target):
            raise UserError(f"cd: no such folder: /pool/{target}")
        sess.cwd = target

    def _export(self, sess, rest):
        for item in shlex.split(rest):
            name, eq, value = item.partition("=")
            if not name.isidentifier() or not eq:
                raise UserError("Use: export NAME=value")
            sess.env[name] = value

    def _runners(self, sess, rest):
        rows = self.s.execs.describe_runners()
        if not rows:
            self._out(sess, "No computer can run commands right now.\r\n")
            return
        for r in rows:
            cpu = f"{r['cpu']:.0f}% CPU" if r["cpu"] is not None else "CPU unknown"
            self._out(sess, f"  {C['bold']}{r['name']}{C['reset']}  {r['os']}  {cpu}  {C['green'] + 'idle' if not r['busy'] else C['yellow'] + 'running a command'}{C['reset']}\r\n")
        if not any(r["linux"] for r in rows):
            self._out(sess, f"{C['dim']}No Linux computer is connected, so commands run on macOS.{C['reset']}\r\n")

    def _ps(self, sess, rest):
        rows = self.s.execs.listing()
        if not rows:
            self._out(sess, "Nothing has run yet.\r\n")
            return
        for p in rows[:15]:
            state = {"queued": "waiting", "running": "running", "done": f"exit {p['code']}", "failed": "failed", "cancelled": "cancelled"}[p["status"]]
            self._out(sess, f"  {p['runner'] or '—':<22} {state:<10} {p['cmd'][:60]}\r\n")

    def _on(self, sess, rest):
        # A computer's name can have spaces ("Darshan's MacBook Air"): match the longest connected name first,
        # then a quoted name, then a single word such as linux, mac or the start of a name.
        target = cmd = ""
        low = rest.lower()
        for name in sorted((r["name"] for r in self.s.execs.describe_runners()), key=len, reverse=True):
            if low.startswith(name.lower() + " "):
                target, cmd = name, rest[len(name):].strip()
                break
        else:
            if rest[:1] in "\"'":
                end = rest.find(rest[0], 1)
                target, cmd = (rest[1:end], rest[end + 1:].strip()) if end > 0 else ("", "")
            else:
                target, _, cmd = rest.partition(" ")
        if not target or not cmd.strip():
            raise UserError("Use: on <computer|linux|mac> <command>   (put quotes around a name that has spaces)")
        if not self.s.execs.eligible(target):
            names = ", ".join(r["name"] for r in self.s.execs.describe_runners()) or "none connected"
            raise UserError(f"No computer matches “{target}”. Computers that can run commands: {names}.")
        self._exec(sess, cmd.strip(), target)

    def _footer(self, p, started_note: bool = True) -> str:
        secs = (p.ended_at or p.started_at or p.queued_at) - (p.started_at or p.queued_at)
        down, up = (p.sync or {}).get("down", {}), (p.sync or {}).get("up", {})
        moved = []
        if down.get("files"):
            moved.append(f"{down['files']} file{'s' if down['files'] != 1 else ''} fetched")
        if up.get("files") or up.get("deleted"):
            bits = []
            if up.get("files"):
                bits.append(f"{up['files']} saved")
            if up.get("deleted"):
                bits.append(f"{up['deleted']} deleted")
            moved.append(", ".join(bits) + " in /pool")
        code = p.code if p.code is not None else "?"
        tint = C["green"] if p.code == 0 else C["red"]
        extra = f" · {' · '.join(moved)}" if moved else ""
        return f"{C['dim']}↳ ran on {C['reset']}{C['bold']}{p.runner_name}{C['reset']}{C['dim']} ({p.runner_os}) · {secs:.1f} s · {C['reset']}{tint}exit {code}{C['reset']}{C['dim']}{extra}{C['reset']}\r\n"

    def _exec(self, sess: Session, cmd: str, target: str | None) -> None:
        def on_start(p):
            if sess.waiting_note:
                self._out(sess, f"{C['dim']}…starting on {p.runner_name}{C['reset']}\r\n")
                sess.waiting_note = False

        def on_out(data: bytes):
            self._out(sess, data)

        def on_done(p):
            sess.running.discard(p.id)
            if p.status == "failed" and not p.runner_name:
                self._out(sess, f"{C['red']}{p.error}{C['reset']}\r\n")
            elif p.status == "failed":
                self._out(sess, f"{C['red']}{p.error}{C['reset']}\r\n")
                if p.runner_name:
                    self._out(sess, self._footer(p))
            elif p.status == "cancelled" and not p.runner_name:
                self._out(sess, f"{C['dim']}cancelled before it started{C['reset']}\r\n")
            else:
                self._out(sess, self._footer(p))
                if p.new_cwd is not None and p.status == "done":
                    sess.cwd = p.new_cwd if (not p.new_cwd or self.s.pool_store.is_dir(p.new_cwd)) else sess.cwd
            if not sess.running and sess.sid in self.sessions:
                sess.par = None
                self._new_prompt(sess)

        proc = self.s.execs.submit(cmd, sess.cwd, dict(sess.env), sess.browser.name, target, on_out=on_out, on_done=on_done, on_start=on_start)
        if proc.status == "queued":
            sess.running.add(proc.id)
            sess.waiting_note = True
            self._out(sess, f"{C['dim']}…waiting for a free computer{C['reset']}\r\n")
        elif proc.status == "running":
            sess.running.add(proc.id)
        # a command that failed at once has already been reported through on_done and showed the prompt

    def _par(self, sess, rest):
        if ":::" not in rest:
            raise UserError("Use: par <command with {}> ::: item1 item2 item3")
        template, _, items_text = rest.partition(":::")
        template, items = template.strip(), shlex.split(items_text)
        if not template or not items:
            raise UserError("Use: par <command with {}> ::: item1 item2 item3")
        if len(items) > 200:
            raise UserError("par runs up to 200 items at a time.")
        if "{}" not in template:
            template += " {}"
        if not self.s.execs.eligible(None):
            raise UserError("No computer can run commands right now.")
        results: list = [None] * len(items)
        remaining = {"n": len(items)}
        self._out(sess, f"{C['dim']}Running {len(items)} command{'s' if len(items) != 1 else ''} across the pool…{C['reset']}\r\n")

        def make(idx: int, item: str):
            chunks: list[bytes] = []

            def on_out(data: bytes):
                chunks.append(data)

            def on_done(p):
                sess.running.discard(p.id)
                results[idx] = (item, p, b"".join(chunks))
                remaining["n"] -= 1
                head = f"{C['bold']}{item}{C['reset']}  {C['dim']}on {p.runner_name or 'no computer'} · "
                head += f"{((p.ended_at or 0) - (p.started_at or p.ended_at or 0)):.1f} s · exit {p.code if p.code is not None else '?'}{C['reset']}\r\n"
                body = NEWLINE.sub(b"\r\n", b"".join(chunks).rstrip(b"\n"))
                self._out(sess, head)
                if body:
                    self._out(sess, body + b"\r\n")
                if p.error and p.status != "done":
                    self._out(sess, f"{C['red']}{p.error}{C['reset']}\r\n")
                if remaining["n"] == 0 and sess.sid in self.sessions:
                    per: dict[str, int] = {}
                    for r in results:
                        per[r[1].runner_name or "none"] = per.get(r[1].runner_name or "none", 0) + 1
                    self._out(sess, f"{C['dim']}↳ {len(items)} commands ran on {', '.join(f'{n} ×{c}' for n, c in sorted(per.items(), key=lambda kv: -kv[1]))}{C['reset']}\r\n")
                    self._new_prompt(sess)

            cmd = template.replace("{}", shlex.quote(item))
            p = self.s.execs.submit(cmd, sess.cwd, dict(sess.env), sess.browser.name, None, on_out=on_out, on_done=on_done)
            if p.status in ("queued", "running"):
                sess.running.add(p.id)

        for idx, item in enumerate(items):
            make(idx, item)
