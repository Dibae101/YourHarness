"""The job engine's saved state: SQLite in the coordinator's data folder.

Jobs, their stages and partitions, every attempt at a partition, datasets, stored
artifacts, throughput history and an event log survive a restart. Only the
coordinator writes, from its event loop, so one connection is enough. The schema
version lives in PRAGMA user_version; MIGRATIONS[n] takes a database from version n
to n + 1, so an old database is upgraded in place and never rebuilt.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

MIGRATIONS = [
    # 0 -> 1
    [
        """CREATE TABLE artifacts (
            sha TEXT PRIMARY KEY, size INTEGER NOT NULL, kind TEXT, created_at REAL, last_used REAL, pinned INTEGER DEFAULT 0)""",
        """CREATE TABLE datasets (
            id TEXT PRIMARY KEY, name TEXT, format TEXT, sha TEXT, size INTEGER, meta TEXT, classification TEXT,
            created_by TEXT, created_at REAL, source TEXT)""",
        """CREATE TABLE jobs (
            id TEXT PRIMARY KEY, name TEXT, spec TEXT, status TEXT, decision TEXT, plan TEXT, note TEXT, error TEXT,
            created_by TEXT, created_at REAL, updated_at REAL, started_at REAL, finished_at REAL, result TEXT,
            outputs TEXT, validation TEXT)""",
        """CREATE TABLE stages (
            id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE, seq INTEGER, name TEXT,
            kind TEXT, status TEXT, depends_on TEXT, total INTEGER, done INTEGER DEFAULT 0, spec TEXT, result TEXT,
            started_at REAL, finished_at REAL)""",
        """CREATE TABLE partitions (
            id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE, stage_id TEXT, idx INTEGER,
            input TEXT, status TEXT, attempt INTEGER DEFAULT 0, tries INTEGER DEFAULT 0, moves INTEGER DEFAULT 0,
            lease_until REAL, device_id TEXT, device_name TEXT, accepted_attempt INTEGER, output TEXT, checkpoint TEXT,
            error TEXT, started_at REAL, finished_at REAL, updated_at REAL, bytes INTEGER, records INTEGER)""",
        "CREATE INDEX partitions_ready ON partitions (status, job_id, idx)",
        "CREATE INDEX partitions_by_stage ON partitions (stage_id, idx)",
        """CREATE TABLE attempts (
            partition_id TEXT NOT NULL REFERENCES partitions(id) ON DELETE CASCADE, n INTEGER, device_id TEXT,
            device_name TEXT, token_hash TEXT, status TEXT, started_at REAL, ended_at REAL, outputs TEXT, metrics TEXT,
            error TEXT, PRIMARY KEY (partition_id, n))""",
        """CREATE TABLE events (
            seq INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, job_id TEXT, partition_id TEXT, device_id TEXT, type TEXT,
            data TEXT)""",
        "CREATE INDEX events_by_job ON events (job_id, seq)",
        "CREATE TABLE perf (device_id TEXT, workload TEXT, units INTEGER, seconds REAL, ts REAL)",
        "CREATE INDEX perf_by_device ON perf (device_id, workload, ts)",
    ],
    # 1 -> 2: models for ml.evaluate; attempts that re-run an accepted partition to check it
    [
        """CREATE TABLE models (
            id TEXT PRIMARY KEY, name TEXT, adapter TEXT, format TEXT, sha TEXT, size INTEGER, meta TEXT,
            created_by TEXT, created_at REAL)""",
        "ALTER TABLE attempts ADD COLUMN purpose TEXT NOT NULL DEFAULT 'run'",
        "ALTER TABLE attempts ADD COLUMN lease_until REAL",
        "CREATE INDEX attempts_by_purpose ON attempts (purpose, status)",
    ],
]
JSON_COLUMNS = {"meta", "spec", "plan", "result", "outputs", "validation", "depends_on", "input", "output", "checkpoint", "metrics", "data"}
TABLES = ("artifacts", "datasets", "models", "jobs", "stages", "partitions", "attempts", "events", "perf")


class EngineDB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.migrate()
        self.columns = {t: {r[1] for r in self.conn.execute(f"PRAGMA table_info({t})")} for t in TABLES}

    def migrate(self) -> int:
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        for n in range(version, len(MIGRATIONS)):
            with self.tx():
                for stmt in MIGRATIONS[n]:
                    self.conn.execute(stmt)
                self.conn.execute(f"PRAGMA user_version={n + 1}")
        return len(MIGRATIONS)

    def tx(self):
        """A transaction: everything inside commits together or not at all."""
        db = self

        class _Tx:
            def __enter__(self):
                db.conn.execute("BEGIN IMMEDIATE")

            def __exit__(self, exc_type, exc, tb):
                db.conn.execute("ROLLBACK" if exc_type else "COMMIT")
                return False

        return _Tx()

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def row(r) -> dict | None:
        if r is None:
            return None
        d = dict(r)
        for k in JSON_COLUMNS & d.keys():
            if d[k] is not None:
                d[k] = json.loads(d[k])
        return d

    def _enc(self, table: str, fields: dict) -> dict:
        unknown = set(fields) - self.columns[table]
        if unknown:
            raise KeyError(f"unknown columns for {table}: {sorted(unknown)}")
        return {k: (json.dumps(v, separators=(",", ":"), allow_nan=False) if k in JSON_COLUMNS and v is not None else v) for k, v in fields.items()}

    def insert(self, table: str, fields: dict) -> None:
        f = self._enc(table, fields)
        self.conn.execute(f"INSERT INTO {table} ({', '.join(f)}) VALUES ({', '.join('?' for _ in f)})", list(f.values()))

    def update(self, table: str, where: dict, fields: dict) -> int:
        if not fields:
            return 0
        f = self._enc(table, fields)
        w = " AND ".join(f"{k} = ?" for k in where)
        cur = self.conn.execute(f"UPDATE {table} SET {', '.join(f'{k} = ?' for k in f)} WHERE {w}", [*f.values(), *where.values()])
        return cur.rowcount

    def one(self, sql: str, args=()) -> dict | None:
        return self.row(self.conn.execute(sql, args).fetchone())

    def all(self, sql: str, args=()) -> list[dict]:
        return [self.row(r) for r in self.conn.execute(sql, args)]

    # ------------------------------------------------------------------ objects
    def job(self, job_id: str) -> dict | None:
        return self.one("SELECT * FROM jobs WHERE id = ?", (job_id,))

    def partition(self, pid: str) -> dict | None:
        return self.one("SELECT * FROM partitions WHERE id = ?", (pid,))

    def attempt(self, pid: str, n: int) -> dict | None:
        return self.one("SELECT * FROM attempts WHERE partition_id = ? AND n = ?", (pid, n))

    def dataset(self, ds_id: str) -> dict | None:
        return self.one("SELECT * FROM datasets WHERE id = ?", (ds_id,))

    def model(self, model_id: str) -> dict | None:
        return self.one("SELECT * FROM models WHERE id = ?", (model_id,))

    def stage(self, stage_id: str) -> dict | None:
        return self.one("SELECT * FROM stages WHERE id = ?", (stage_id,))

    def stages(self, job_id: str) -> list[dict]:
        return self.all("SELECT * FROM stages WHERE job_id = ? ORDER BY seq", (job_id,))

    def counts(self, job_id: str) -> dict[str, int]:
        rows = self.conn.execute("SELECT status, COUNT(*) FROM partitions WHERE job_id = ? GROUP BY status", (job_id,))
        return {r[0]: r[1] for r in rows}

    def event(self, job_id: str | None, kind: str, data: dict | None = None, partition_id: str | None = None,
              device_id: str | None = None) -> None:
        self.conn.execute("INSERT INTO events (ts, job_id, partition_id, device_id, type, data) VALUES (?, ?, ?, ?, ?, ?)",
                          (time.time(), job_id, partition_id, device_id, kind, json.dumps(data or {}, allow_nan=False)))

    def events(self, job_id: str, limit: int = 500) -> list[dict]:
        rows = self.all("SELECT * FROM events WHERE job_id = ? ORDER BY seq DESC LIMIT ?", (job_id, limit))
        return rows[::-1]
