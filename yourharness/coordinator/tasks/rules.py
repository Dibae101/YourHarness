"""Rules for agent tasks that need no I/O.

Checking a run the web app sends, matching the files a task may change, and
spotting secrets in a diff. Kept free of I/O so they are easy to test.
"""

from __future__ import annotations

import math
import re
import secrets as _secrets
from pathlib import PurePosixPath

ADAPTERS = {
    "command": "Command (no AI)",
    "claude": "Claude Code",
    "codex": "Codex CLI",
    "kiro": "Kiro CLI",
}
KINDS = {"code": "Code change", "general": "Other work"}
# Changing these needs an explicit yes on the task: a workflow can read the repository's CI secrets.
PROTECTED = (".github/workflows/**", ".gitmodules")
# Results never go to these, whatever the run says.
BLOCKED_BRANCHES = frozenset({"main", "master", "mainline", "trunk"})

MAX_TASKS = 20
MAX_TEXT = 20_000
MAX_COMMAND = 10_000
MAX_FILE_MB = 50
MAX_COMMITS = 200


class RunError(ValueError):
    """A problem with a run that the person can fix. The message is shown as is."""


def new_id(prefix: str) -> str:
    return prefix + _secrets.token_hex(4)


def slug(text, max_len: int = 40) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", str(text or "").lower()).strip("-")
    return s[:max_len].strip("-") or "task"


def clean_line(v, max_len: int, fallback: str = "") -> str:
    s = re.sub(r"[\x00-\x1f\x7f]", " ", "" if v is None else str(v)).strip()
    return s[:max_len] or fallback


def clean_block(v, max_len: int) -> str:
    """Multi-line text with control characters other than newlines and tabs removed."""
    s = "" if v is None else str(v).replace("\r\n", "\n")
    s = re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "", s).strip()
    if len(s) > max_len:
        raise RunError(f"A text field is over {max_len:,} characters. Make it shorter.")
    return s


def lines_of(v, max_items: int, max_len: int) -> list[str]:
    """A list of one-line entries, from a list or from text with one entry per line."""
    items = v if isinstance(v, list) else str(v or "").splitlines()
    out = [clean_line(x, max_len) for x in items]
    out = [x for x in out if x]
    if len(out) > max_items:
        raise RunError(f"There are more than {max_items} entries in a list. Keep the most important ones.")
    return out


# ---------------------------------------------------------------- branches
def valid_branch(name: str) -> bool:
    """The parts of git's ref-name rules that matter for a branch typed by a person."""
    if not name or len(name) > 200 or name == "@":
        return False
    if name.startswith(("-", "/")) or name.endswith(("/", ".", ".lock")):
        return False
    if ".." in name or "//" in name or "@{" in name:
        return False
    if re.search(r"[\x00-\x20~^:?*\[\\\x7f]", name):
        return False
    return not any(p.startswith(".") or p.endswith(".lock") for p in name.split("/"))


def check_results_branch(branch: str, base: str, default: str | None) -> None:
    if not valid_branch(branch):
        raise RunError(f"“{branch}” can't be a branch name. Use letters, numbers, - _ . and /.")
    if branch in BLOCKED_BRANCHES or branch == base or (default and branch == default):
        raise RunError(f"Results never go straight to {branch}. Pick a new branch, such as yh/my-run; you can merge it with a pull request.")


# ---------------------------------------------------------------- file scopes
_GLOB_CACHE: dict[str, re.Pattern] = {}


def glob_regex(pattern: str) -> re.Pattern:
    """A pattern relative to the repository root.

    ``*`` matches within one folder, ``**`` across folders, ``**/`` any number of
    folders (including none). A pattern without wildcards, or ending in ``/``, also
    matches everything inside it, like a folder.
    """
    cached = _GLOB_CACHE.get(pattern)
    if cached is not None:
        return cached
    p = pattern.strip().lstrip("/")
    folder = p.endswith("/") or not any(ch in p for ch in "*?")
    p = p.rstrip("/")
    out, i = [], 0
    while i < len(p):
        c = p[i]
        if c == "*":
            if p.startswith("**", i):
                if p.startswith("**/", i):
                    out.append("(?:.*/)?")
                    i += 3
                else:
                    out.append(".*")
                    i += 2
                continue
            out.append("[^/]*")
        elif c == "?":
            out.append("[^/]")
        else:
            out.append(re.escape(c))
        i += 1
    body = "".join(out)
    rx = re.compile(f"^{body}(?:/.*)?$" if folder and body else f"^{body}$" if body else r"^.*$")
    _GLOB_CACHE[pattern] = rx
    return rx


def needs_commands(spec: dict) -> bool:
    """True when a task runs commands of the owner's choosing on the computer that takes it.

    That covers the Command agent, setup and check commands, and an AI agent allowed to
    run commands itself. A friend's computer takes such a task only if its owner started
    YourHarness with --allow-commands; otherwise it gets AI agents that may only edit files.
    """
    return (spec.get("adapter") == "command" or bool(spec.get("setup")) or bool(spec.get("checks"))
            or spec.get("autonomy") == "full")


def matches(path: str, patterns) -> bool:
    return any(glob_regex(p).match(path) for p in patterns if str(p).strip())


def symlink_escapes(link_path: str, target: str) -> bool:
    """True when a symbolic link added at link_path would point outside the repository."""
    t = target.replace("\\", "/")
    if t.startswith("/") or re.match(r"^[A-Za-z]:", t):
        return True
    parts = list(PurePosixPath(link_path).parts[:-1])
    for seg in t.split("/"):
        if seg in ("", "."):
            continue
        if seg == "..":
            if not parts:
                return True
            parts.pop()
        else:
            parts.append(seg)
    return False


# ---------------------------------------------------------------- secrets
SECRET_PATTERNS = [
    ("a private key", re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")),
    ("a GitHub token", re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{36,}\b")),
    ("a GitHub token", re.compile(r"\bgithub_pat_[A-Za-z0-9_]{50,}\b")),
    ("an AWS access key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("an API key", re.compile(r"\bsk-(?:ant-|proj-)?[A-Za-z0-9_-]{32,}\b")),
    ("a Slack token", re.compile(r"\bxox[abposr]-[A-Za-z0-9-]{10,}\b")),
    ("a Google API key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
]


def scan_secrets(diff: str, limit: int = 20) -> list[str]:
    """Lines a diff adds that look like credentials, as "path:line (what)"."""
    found: list[str] = []
    path, line_no = None, 0
    for line in diff.splitlines():
        if line.startswith("+++ "):
            name = line[4:].strip()
            path = None if name == "/dev/null" else (name[2:] if name.startswith("b/") else name)
            continue
        if line.startswith("@@"):
            m = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)", line)
            line_no = int(m.group(1)) if m else 0
            continue
        if line.startswith("+") and path:
            for what, rx in SECRET_PATTERNS:
                if rx.search(line):
                    found.append(f"{path}:{line_no} ({what})")
                    break
            line_no += 1
            if len(found) >= limit:
                break
        elif line.startswith(" "):
            line_no += 1
    return found


# ---------------------------------------------------------------- verifying a result
def verify(info: dict, spec: dict, max_file_mb: int = MAX_FILE_MB) -> list[str]:
    """Problems with a task's commits. An empty list means they may be combined and pushed."""
    if info.get("error"):
        return [info["error"]]
    problems: list[str] = []
    if info.get("count", 0) > MAX_COMMITS:
        problems.append(f"The result has {info['count']} commits, more than the {MAX_COMMITS} allowed.")
    allow = spec.get("scope") or ["**"]
    deny = spec.get("deny") or []
    outside, protected = [], []
    for status, path in info.get("changes", []):
        if path == ".git" or path.startswith(".git/") or "/.git/" in f"/{path}":
            protected.append(path)
        elif not matches(path, allow):
            outside.append(path)
        elif matches(path, deny):
            protected.append(path)
    if outside:
        problems.append("Changed files outside what this task may change: " + _few(outside))
    if protected:
        problems.append("Changed protected files: " + _few(protected))
    for path, e in (info.get("entries") or {}).items():
        if e.get("mode") == "160000":
            problems.append(f"{path} adds another Git repository inside this one (a submodule).")
        elif e.get("size", 0) > max_file_mb * 1024 * 1024:
            problems.append(f"{path} is {e['size'] / 2**20:.0f} MB, over the {max_file_mb} MB limit.")
    for path, target in (info.get("symlinks") or {}).items():
        if symlink_escapes(path, target):
            problems.append(f"{path} is a link to {target}, which is outside the repository.")
    hits = scan_secrets(info.get("diff") or "")
    if hits:
        problems.append("Looks like a secret: " + _few(hits))
    return problems


def _few(items: list[str], n: int = 8) -> str:
    shown = ", ".join(items[:n])
    return shown + (f" and {len(items) - n} more" if len(items) > n else "")


# ---------------------------------------------------------------- a new run from the web app
def _num(v, lo, hi, default):
    try:
        n = float(v)
    except (TypeError, ValueError):
        return default
    return default if not math.isfinite(n) else min(hi, max(lo, n))


def clean_task(raw: dict, n: int) -> dict:
    if not isinstance(raw, dict):
        raise RunError(f"Task {n} isn't in the right shape.")
    title = clean_line(raw.get("title"), 120)
    if not title:
        raise RunError(f"Task {n} needs a title.")
    kind = raw.get("kind") if raw.get("kind") in KINDS else "code"
    adapter = raw.get("adapter") if raw.get("adapter") in ADAPTERS else None
    if adapter is None:
        raise RunError(f"Pick what does task {n}: {', '.join(ADAPTERS.values())}.")
    instructions = clean_block(raw.get("instructions"), MAX_TEXT)
    command = clean_block(raw.get("command"), MAX_COMMAND)
    if adapter == "command" and not command:
        raise RunError(f"Task {n} runs a command. Type the command.")
    if adapter != "command" and not instructions:
        raise RunError(f"Task {n} needs instructions for {ADAPTERS[adapter]}.")
    model = clean_line(raw.get("model"), 100)
    if model and not re.fullmatch(r"[A-Za-z0-9._:/@\[\]-]+", model):
        raise RunError(f"Task {n}: the model name can only use letters, numbers and . _ : / @ -.")
    results_dir = f"results/{slug(title)}-{_secrets.token_hex(2)}" if kind == "general" else ""
    scope = lines_of(raw.get("scope"), 50, 200)
    if not scope:
        scope = [f"{results_dir}/**"] if kind == "general" else ["**"]
    deny = lines_of(raw.get("deny"), 50, 200)
    if not raw.get("allow_workflows"):
        deny = list(dict.fromkeys([*PROTECTED, *deny]))
    max_usd = raw.get("max_usd")
    max_usd = None if max_usd in (None, "", 0) else round(_num(max_usd, 0.01, 1000, 5), 2)
    device = clean_line(raw.get("device"), 40)
    if device and not re.fullmatch(r"[0-9a-f]{6,40}", device):
        raise RunError(f"Task {n}: that computer isn't in the pool.")
    return {
        "title": title,
        "kind": kind,
        "adapter": adapter,
        "model": model,
        "instructions": instructions,
        "command": command,
        # "full" runs the CLIs with their permission-bypass flags, so it has to be asked for by name
        "autonomy": "full" if raw.get("autonomy") == "full" else "edit",
        "scope": scope,
        "deny": deny,
        "checks": lines_of(raw.get("checks"), 20, 1000),
        "setup": lines_of(raw.get("setup"), 20, 1000),
        "time_limit_min": int(_num(raw.get("time_limit_min"), 1, 720, 30)),
        "max_usd": max_usd,
        "rounds": 1 if adapter == "command" else int(_num(raw.get("rounds"), 1, 5, 2)),
        "device": device,
        "results_dir": results_dir,
    }


def clean_run(body) -> dict:
    if not isinstance(body, dict):
        raise RunError("Send the run as JSON.")
    repo = body.get("repo") if isinstance(body.get("repo"), dict) else {}
    kind = repo.get("kind")
    name = clean_line(repo.get("name"), 300)
    if kind == "github":
        name = re.sub(r"^https?://github\.com/", "", name).removesuffix(".git").strip("/")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", name):
            raise RunError("Pick a GitHub repository, written as owner/name.")
    elif kind == "local":
        if not name:
            raise RunError("Type the path of a Git repository on the coordinator's computer.")
    else:
        raise RunError("Pick where the results go: a GitHub repository or a local one.")
    base = clean_line(body.get("base_branch"), 200)
    if base and not valid_branch(base):
        raise RunError(f"“{base}” isn't a branch name.")
    results = clean_line(body.get("results_branch"), 200)
    if results:
        check_results_branch(results, base, None)
    tasks = body.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise RunError("Add at least one task.")
    if len(tasks) > MAX_TASKS:
        raise RunError(f"A run can have up to {MAX_TASKS} tasks.")
    return {
        "title": clean_line(body.get("title"), 120) or clean_line(tasks[0].get("title") if isinstance(tasks[0], dict) else "", 120, "Agent run"),
        "repo": {"kind": kind, "name": name},
        "base_branch": base,
        "results_branch": results,
        "open_pr": bool(body.get("open_pr", True)) and kind == "github",
        "tasks": [clean_task(t, i + 1) for i, t in enumerate(tasks)],
        "final": clean_final(body),
    }


def clean_final(body: dict) -> dict:
    """The run's final checks: commands run on the combined branch on the coordinator's computer before it's pushed."""
    checks = lines_of(body.get("final_checks"), 20, 1000)
    setup = lines_of(body.get("final_setup"), 20, 1000)
    if setup and not checks:
        raise RunError("Final setup commands need final checks to go with them.")
    return {
        "checks": checks,
        "setup": setup,
        "time_limit_min": int(_num(body.get("final_time_limit_min"), 1, 240, 30)),
        "gate": bool(body.get("final_gate")),  # don't push or open a pull request when they fail
        "with_friends": bool(body.get("final_with_friends")),  # also run them when a task came from a friend's computer
    }


def repo_id(kind: str, name: str) -> str:
    import hashlib

    if kind == "github":
        return "gh-" + re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:80]
    return "local-" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
