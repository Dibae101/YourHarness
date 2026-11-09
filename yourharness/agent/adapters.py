"""The AI agents, and plain commands, a computer can run agent tasks with.

Each adapter turns a task into one bash command line, run inside the task's Git
worktree with a cleaned-up environment (``yh_clean``, see taskhost.py), and reads
the agent's output to show progress. The flags are the ones each CLI documents for
running without a person at the keyboard (checked against their --help):

  Claude Code  claude -p … --output-format stream-json --verbose --permission-mode …
  Codex CLI    codex exec --json -C <dir> -s workspace-write -o <last message file> …
  Kiro CLI     kiro-cli chat --no-interactive --trust-tools=… (or --trust-all-tools) …
"""

from __future__ import annotations

import json
import re
import shlex

ADAPTERS = {
    "command": "Command (no AI)",
    "claude": "Claude Code",
    "codex": "Codex CLI",
    "kiro": "Kiro CLI",
}
BINARIES = {"claude": "claude", "codex": "codex", "kiro": "kiro-cli"}

# Finds the tools a computer has. Only --version is called: no agent is started.
# YH_PROBE lists the agent CLIs to look for (HARNESS_AGENT_CLIS can narrow it, e.g. to none in tests).
DETECT_SCRIPT = r'''
for __c in git curl ${YH_PROBE:-}; do
  __p=$(command -v "$__c" 2>/dev/null) || continue
  # on a Mac without the developer tools, /usr/bin/git only opens an install dialog
  if [ "$__p" = /usr/bin/git ] && [ "$(uname -s)" = Darwin ] && ! xcode-select -p >/dev/null 2>&1; then continue; fi
  __v=$("$__c" --version 2>/dev/null) || continue
  __l=$(printf '%s\n' "$__v" | head -n 1)
  printf '::yh-tool::%s::%s\n' "$__c" "${__l:-installed}"
done
'''

ANSI = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07]*\x07")


def q(s) -> str:
    return shlex.quote(str(s))


def parse_detect(output: str) -> dict:
    found = {}
    for line in output.splitlines():
        m = re.match(r"::yh-tool::([\w.-]+)::(.*)$", line.strip())
        if m:
            found[m.group(1)] = m.group(2).strip()[:60]
    agents = [{"id": "command", "version": ""}]
    for aid, binary in BINARIES.items():
        if binary in found:
            agents.append({"id": aid, "version": found[binary]})
    return {"git": found.get("git"), "curl": found.get("curl"), "agents": agents}


def agent_command(adapter: str, spec: dict, prompt_path: str) -> str:
    """The bash command that runs one round of the agent, from inside the worktree."""
    model = (spec.get("model") or "").strip()
    full = spec.get("autonomy") != "edit"
    prompt = f'"$(cat {q(prompt_path)})"'  # expanded by bash on the computer doing the work
    if adapter == "command":
        return "bash -c " + q(spec.get("command") or "true")
    if adapter == "claude":
        parts = ["claude", "-p", prompt, "--output-format", "stream-json", "--verbose", "--no-session-persistence",
                 "--permission-mode", "bypassPermissions" if full else "acceptEdits"]
        if model:
            parts += ["--model", q(model)]
        if spec.get("max_usd"):
            parts += ["--max-budget-usd", q(f"{float(spec['max_usd']):.2f}")]
        return " ".join(parts)
    if adapter == "codex":
        # $YH_OUT is the attempt's output folder, set by the step script on the computer doing the work
        parts = ["codex", "exec", "--json", "--ephemeral", "--skip-git-repo-check", "-C", '"$PWD"', "-o", '"$YH_OUT/last-message.txt"']
        # "edit" keeps Codex's own sandbox: it may change files in this folder and run commands inside it
        parts += ["--dangerously-bypass-approvals-and-sandbox"] if full else ["-s", "workspace-write"]
        if model:
            parts += ["-m", q(model)]
        parts.append(prompt)
        return " ".join(parts)
    if adapter == "kiro":
        parts = ["kiro-cli", "chat", "--no-interactive", "--trust-all-tools" if full else "--trust-tools=fs_read,fs_write"]
        if model:
            parts += ["--model", q(model)]
        parts.append(prompt)
        return " ".join(parts)
    raise ValueError(f"unknown adapter {adapter}")


def _short(value, n: int = 140) -> str:
    if isinstance(value, dict):
        for key in ("command", "file_path", "path", "pattern", "url", "description"):
            if value.get(key):
                value = value[key]
                break
        else:
            value = json.dumps(value)[:n]
    text = " ".join(str(value).split())
    return text if len(text) <= n else text[: n - 1] + "…"


class OutputReader:
    """Turns an agent's raw output into readable log lines, and picks out its final message and cost."""

    def __init__(self, adapter: str):
        self.adapter = adapter
        self.buf = ""
        self.summary = ""
        self.usd: float | None = None
        self.tail = ""
        self._last_unknown = None

    def feed(self, text: str) -> str:
        self.tail = (self.tail + text)[-6000:]
        if self.adapter not in ("claude", "codex"):
            return text
        self.buf += text
        *lines, self.buf = self.buf.split("\n")
        return "".join(self._line(line) for line in lines)

    def flush(self) -> str:
        if self.adapter in ("claude", "codex") and self.buf:
            line, self.buf = self.buf, ""
            return self._line(line)
        return ""

    def final_text(self) -> str:
        if self.summary:
            return self.summary.strip()
        return ANSI.sub("", self.tail).strip()[-2000:]

    def _line(self, line: str) -> str:
        s = line.strip()
        if not s:
            return ""
        try:
            ev = json.loads(s)
        except ValueError:
            return line + "\n"  # not an event: show it as it is
        if not isinstance(ev, dict):
            return ""
        out = self._claude(ev) if self.adapter == "claude" else self._codex(ev)
        if out is None:  # an event type this version doesn't know: note it once so the log isn't silent
            msg = ev.get("msg") if isinstance(ev.get("msg"), dict) else {}
            kind = ev.get("type") or msg.get("type")
            if kind != self._last_unknown:
                self._last_unknown = kind
                return f"· {kind}\n" if kind else ""
            return ""
        return out

    def _claude(self, ev: dict) -> str | None:
        t = ev.get("type")
        if t == "assistant":
            out = []
            for c in (ev.get("message") or {}).get("content") or []:
                if not isinstance(c, dict):
                    continue
                if c.get("type") == "text" and c.get("text"):
                    out.append(c["text"].strip() + "\n")
                elif c.get("type") == "tool_use":
                    out.append(f"→ {c.get('name')}: {_short(c.get('input'))}\n")
            return "".join(out)
        if t == "user":
            return ""  # tool results: long, and the tool line above already says what happened
        if t == "result":
            self.summary = str(ev.get("result") or "")[:4000]
            if isinstance(ev.get("total_cost_usd"), (int, float)):
                self.usd = float(ev["total_cost_usd"])
            cost = f", ${self.usd:.2f}" if self.usd is not None else ""
            if ev.get("is_error"):
                return f"Claude Code stopped ({ev.get('subtype') or 'error'}{cost}).\n"
            return f"Claude Code finished in {ev.get('num_turns', '?')} turns{cost}.\n"
        if t == "system":
            return f"Claude Code started (model {ev.get('model', 'default')}).\n" if ev.get("subtype") == "init" else ""
        return None

    def _codex(self, ev: dict) -> str | None:
        t = ev.get("type")
        item = ev.get("item") if isinstance(ev.get("item"), dict) else None
        if item is not None:
            it = item.get("type") or item.get("item_type")
            if it in ("agent_message", "assistant_message"):
                text = str(item.get("text") or "")
                if text and t != "item.started":
                    self.summary = text[:4000]
                    return text.strip() + "\n"
                return ""
            if it == "command_execution":
                return f"$ {_short(item.get('command'))}\n" if t == "item.started" else ""
            if it == "file_change":
                files = [c.get("path") for c in item.get("changes") or [] if isinstance(c, dict)]
                return f"→ changed {', '.join(f for f in files if f)}\n" if files and t != "item.started" else ""
            if it in ("reasoning", "todo_list", "web_search", "mcp_tool_call"):
                return ""
            if it == "error":
                return f"Error: {item.get('message')}\n"
            return None
        msg = ev.get("msg") if isinstance(ev.get("msg"), dict) else None
        if msg is not None:  # the older event format
            mt = msg.get("type")
            if mt == "agent_message" and msg.get("message"):
                self.summary = str(msg["message"])[:4000]
                return str(msg["message"]).strip() + "\n"
            if mt == "exec_command_begin":
                cmd = msg.get("command")
                return f"$ {_short(' '.join(cmd) if isinstance(cmd, list) else cmd)}\n"
            if mt == "error":
                return f"Error: {msg.get('message')}\n"
            return ""
        if t in ("thread.started", "turn.started"):
            return ""
        if t == "turn.completed":
            return "Codex finished the turn.\n"
        if t in ("turn.failed", "error"):
            err = ev.get("error") if isinstance(ev.get("error"), dict) else {}
            return f"Codex failed: {err.get('message') or ev.get('message') or 'unknown error'}\n"
        return None
