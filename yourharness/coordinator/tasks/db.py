"""Saved state of agent runs and tasks: SQLite in the coordinator's data folder.

Only the coordinator writes here, from its event loop, so one connection is
enough. Runs and tasks survive a restart; a computer that was working on a task
reconnects and carries on.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

VERSION = 1
SCHEMA = [
    """CREATE TABLE IF NOT EXISTS repos (
        id TEXT PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL, default_branch TEXT, created_at REAL)""",
    """CREATE TABLE IF NOT EXISTS runs (
        id TEXT PRIMARY KEY, title TEXT, repo_id TEXT, base_branch TEXT, base_commit TEXT, results_branch TEXT,
        open_pr INTEGER DEFAULT 0, status TEXT, note TEXT, error TEXT, created_by TEXT, created_at REAL,
        updated_at REAL, pushed_commit TEXT, pr_url TEXT, spec TEXT)""",
    """CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY, run_id TEXT, seq INTEGER, title TEXT, spec TEXT, status TEXT, phase TEXT, note TEXT,
        error TEXT, attempt INTEGER DEFAULT 0, lost INTEGER DEFAULT 0, device_id TEXT, device_name TEXT,
        token_hash TEXT, start_commit TEXT, started_at REAL, finished_at REAL, updated_at REAL, result TEXT,
        integrated INTEGER DEFAULT 0)""",
    "CREATE INDEX IF NOT EXISTS tasks_by_run ON tasks (run_id, seq)",
    "CREATE INDEX IF NOT EXISTS tasks_by_status ON tasks (status)",
    """CREATE TABLE IF NOT EXISTS events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, run_id TEXT, task_id TEXT, type TEXT, data TEXT)""",
]
JSON_COLUMNS = ("spec", "result")


class TaskDB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        for stmt in SCHEMA:
            self.conn.execute(stmt)
        self.conn.execute(f"PRAGMA user_version={VERSION}")
        self.columns = {t: {r[1] for r in self.conn.execute(f"PRAGMA table_info({t})")} for t in ("repos", "runs", "tasks", "events")}

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _row(row) -> dict | None:
        if row is None:
            return None
        d = dict(row)
        for k in JSON_COLUMNS:
            if k in d:
                d[k] = json.loads(d[k]) if d[k] else {}
        return d

    def _encode(self, table: str, fields: dict) -> dict:
        unknown = set(fields) - self.columns[table]
        if unknown:
            raise KeyError(f"unknown columns for {table}: {sorted(unknown)}")
        return {k: (json.dumps(v) if k in JSON_COLUMNS and v is not None else v) for k, v in fields.items()}

    def _insert(self, table: str, fields: dict) -> None:
        f = self._encode(table, fields)
        cols = ", ".join(f)
        marks = ", ".join("?" for _ in f)
        self.conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({marks})", list(f.values()))

    def _update(self, table: str, key: str, fields: dict) -> None:
        if not fields:
            return
        f = self._encode(table, fields)
        sets = ", ".join(f"{k} = ?" for k in f)
        self.conn.execute(f"UPDATE {table} SET {sets} WHERE id = ?", [*f.values(), key])

    # ------------------------------------------------------------------ repositories
    def upsert_repo(self, repo_id: str, kind: str, name: str) -> dict:
        if self.repo(repo_id) is None:
            self._insert("repos", {"id": repo_id, "kind": kind, "name": name, "created_at": time.time()})
        return self.repo(repo_id)

    def repo(self, repo_id: str) -> dict | None:
        return self._row(self.conn.execute("SELECT * FROM repos WHERE id = ?", (repo_id,)).fetchone())

    def repos(self) -> list[dict]:
        return [self._row(r) for r in self.conn.execute("SELECT * FROM repos ORDER BY created_at DESC")]

    def update_repo(self, repo_id: str, **fields) -> None:
        self._update("repos", repo_id, fields)

    # ------------------------------------------------------------------ runs
    def add_run(self, fields: dict) -> None:
        now = time.time()
        self._insert("runs", {"created_at": now, "updated_at": now, **fields})

    def run(self, run_id: str) -> dict | None:
        return self._row(self.conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone())

    def runs(self, limit: int = 30) -> list[dict]:
        return [self._row(r) for r in self.conn.execute("SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (limit,))]

    def runs_in(self, statuses) -> list[dict]:
        marks = ", ".join("?" for _ in statuses)
        return [self._row(r) for r in self.conn.execute(f"SELECT * FROM runs WHERE status IN ({marks}) ORDER BY created_at", list(statuses))]

    def update_run(self, run_id: str, **fields) -> None:
        self._update("runs", run_id, {**fields, "updated_at": time.time()})

    # ------------------------------------------------------------------ tasks
    def add_task(self, fields: dict) -> None:
        self._insert("tasks", {"updated_at": time.time(), **fields})

    def task(self, task_id: str) -> dict | None:
        return self._row(self.conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone())

    def tasks(self, run_id: str) -> list[dict]:
        return [self._row(r) for r in self.conn.execute("SELECT * FROM tasks WHERE run_id = ? ORDER BY seq", (run_id,))]

    def tasks_in(self, statuses) -> list[dict]:
        marks = ", ".join("?" for _ in statuses)
        sql = (f"SELECT tasks.* FROM tasks JOIN runs ON runs.id = tasks.run_id WHERE tasks.status IN ({marks}) "
               "ORDER BY runs.created_at, tasks.seq")
        return [self._row(r) for r in self.conn.execute(sql, list(statuses))]

    def running_on(self, device_id: str) -> list[dict]:
        return [self._row(r) for r in self.conn.execute("SELECT * FROM tasks WHERE status = 'running' AND device_id = ?", (device_id,))]

    def busy(self, device_id: str) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM tasks WHERE status = 'running' AND device_id = ?", (device_id,)).fetchone()[0]

    def update_task(self, task_id: str, **fields) -> None:
        self._update("tasks", task_id, {**fields, "updated_at": time.time()})

    # ------------------------------------------------------------------ events (an audit trail)
    def event(self, run_id: str | None, task_id: str | None, kind: str, data: dict | None = None) -> None:
        self.conn.execute("INSERT INTO events (ts, run_id, task_id, type, data) VALUES (?, ?, ?, ?, ?)",
                          (time.time(), run_id, task_id, kind, json.dumps(data or {})))

    def events(self, run_id: str, limit: int = 500) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM events WHERE run_id = ? ORDER BY seq DESC LIMIT ?", (run_id, limit))
        return [{**dict(r), "data": json.loads(r["data"] or "{}")} for r in rows][::-1]
