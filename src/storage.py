"""SQLite 持久层：样本、水位/缺口、告警、抑制、规则版本、原始批次摘要、待审队列。

所有写入在同一把进程锁内完成（FastAPI 同步端点跑在线程池），
保证重启后未确认告警与序列号水位不丢失。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
    satellite_id   TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    point_id       TEXT NOT NULL,
    value          REAL NOT NULL,
    sample_time_ms INTEGER NOT NULL,
    config_version TEXT,
    time_quality   TEXT NOT NULL,
    batch_id       TEXT,
    ingest_time_ms INTEGER NOT NULL,
    PRIMARY KEY (satellite_id, seq, point_id)
);
CREATE INDEX IF NOT EXISTS ix_samples_point_time
    ON samples(satellite_id, point_id, sample_time_ms);

CREATE TABLE IF NOT EXISTS streams (
    satellite_id TEXT PRIMARY KEY,
    high_water   INTEGER NOT NULL,
    gaps_json    TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS rule_versions (
    version     TEXT PRIMARY KEY,
    rules_json  TEXT NOT NULL,
    active      INTEGER NOT NULL,
    created_ms  INTEGER NOT NULL,
    note        TEXT
);

CREATE TABLE IF NOT EXISTS alerts (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    satellite_id       TEXT NOT NULL,
    point_id           TEXT NOT NULL,
    severity           TEXT NOT NULL,
    rule_version       TEXT NOT NULL,
    config_version     TEXT,
    status             TEXT NOT NULL,            -- active|acked|suppressed|closed
    value              REAL,
    trigger_samples    TEXT NOT NULL DEFAULT '[]',
    window_start_ms    INTEGER,
    window_end_ms      INTEGER,
    first_event_ms     INTEGER NOT NULL,
    last_event_ms      INTEGER NOT NULL,
    created_ms         INTEGER NOT NULL,
    acked_by           TEXT,
    ack_time_ms        INTEGER,
    ack_note           TEXT,
    recovered_ms       INTEGER,
    suppression_id     INTEGER,
    config_switch      INTEGER NOT NULL DEFAULT 0,
    close_reason       TEXT
);
CREATE INDEX IF NOT EXISTS ix_alerts_query
    ON alerts(satellite_id, point_id, status, severity, first_event_ms);

CREATE TABLE IF NOT EXISTS suppressions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    satellite_id  TEXT NOT NULL,
    point_pattern TEXT NOT NULL,
    severities    TEXT NOT NULL,
    start_ms      INTEGER NOT NULL,
    end_ms        INTEGER NOT NULL,
    reason        TEXT NOT NULL DEFAULT '',
    created_by    TEXT NOT NULL DEFAULT '',
    created_ms    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_batches (
    batch_id      TEXT PRIMARY KEY,
    satellite_id  TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    rule_version  TEXT,
    sample_count  INTEGER NOT NULL,
    accepted      INTEGER NOT NULL,
    duplicated    INTEGER NOT NULL,
    pending       INTEGER NOT NULL,
    result_json   TEXT NOT NULL,
    received_ms   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pending_samples (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    satellite_id   TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    point_id       TEXT NOT NULL,
    value          REAL,
    config_version TEXT,
    time_quality   TEXT,
    reason         TEXT NOT NULL,
    raw_json       TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'open',  -- open|accepted|rejected
    corrected_time_ms INTEGER,
    resolution_note TEXT,
    received_ms    INTEGER NOT NULL,
    resolved_ms    INTEGER,
    UNIQUE(satellite_id, seq, point_id, reason)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms INTEGER NOT NULL,
    actor TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL,
    target TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT ''
);
"""


class Storage:
    def __init__(self, db_path: str | Path):
        self.path = str(db_path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.executescript(SCHEMA)

    # ---- 基础工具 ----
    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def query_one(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        return self.conn.execute(sql, params).fetchone()

    def query_all(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        return self.conn.execute(sql, params).fetchall()

    def audit(self, action: str, target: str, detail: str = "", actor: str = "") -> None:
        from .util import now_ms
        self.conn.execute(
            "INSERT INTO audit_log(ts_ms, actor, action, target, detail) VALUES (?,?,?,?,?)",
            (now_ms(), actor, action, target, detail),
        )

    def close(self) -> None:
        self.conn.close()
