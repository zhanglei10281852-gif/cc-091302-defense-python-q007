"""SQLite 持久化层。

保存：告警（含触发样本摘要与规则版本）、序列号水位、维护窗口去重键、
数据质量事件、待审队列、抑制规则、规则版本历史、接收批次摘要。
所有时间以 UTC epoch 秒 (REAL) 存储。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    alert_id TEXT PRIMARY KEY,
    point_id TEXT NOT NULL,
    level TEXT NOT NULL,
    state TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    config_version TEXT NOT NULL,
    opened_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    resolved_at REAL,
    ack_by TEXT, ack_at REAL, ack_note TEXT,
    suppressed_by TEXT,
    violation_count INTEGER NOT NULL DEFAULT 0,
    message TEXT NOT NULL DEFAULT '',
    stats_json TEXT NOT NULL DEFAULT '{}',
    locator_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_alerts_query ON alerts(point_id, level, state, opened_at);

CREATE TABLE IF NOT EXISTS alert_samples (
    alert_id TEXT NOT NULL,
    point_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    ts REAL,
    value REAL NOT NULL,
    config_version TEXT NOT NULL,
    PRIMARY KEY (alert_id, point_id, seq)
);

CREATE TABLE IF NOT EXISTS watermarks (
    point_id TEXT PRIMARY KEY,
    high_seq INTEGER NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dedup (
    point_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    seen_at REAL NOT NULL,
    PRIMARY KEY (point_id, seq)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    point_id TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_query ON events(kind, point_id, created_at);

CREATE TABLE IF NOT EXISTS review_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at REAL NOT NULL,
    resolved_at REAL,
    resolution_json TEXT
);

CREATE TABLE IF NOT EXISTS rules_meta (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version TEXT NOT NULL,
    content TEXT NOT NULL,
    applied_at REAL NOT NULL,
    applied_by TEXT NOT NULL DEFAULT 'system'
);

CREATE TABLE IF NOT EXISTS suppressions (
    supp_id TEXT PRIMARY KEY,
    point_pattern TEXT NOT NULL,
    level TEXT,
    starts_at REAL NOT NULL,
    ends_at REAL NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    revoked INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ingest_batches (
    batch_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    received_at REAL NOT NULL,
    summary_json TEXT NOT NULL DEFAULT '{}'
);
"""


class Storage:
    def __init__(self, path: str):
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._lock = threading.RLock()
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------------- 水位 ----------------

    def get_watermark(self, point_id: str) -> Optional[int]:
        with self._lock:
            row = self._conn.execute(
                "SELECT high_seq FROM watermarks WHERE point_id=?", (point_id,)
            ).fetchone()
        return int(row["high_seq"]) if row else None

    def all_watermarks(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT point_id, high_seq FROM watermarks").fetchall()
        return {r["point_id"]: int(r["high_seq"]) for r in rows}

    def set_watermark(self, point_id: str, high_seq: int, now: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO watermarks(point_id, high_seq, updated_at) VALUES(?,?,?) "
                "ON CONFLICT(point_id) DO UPDATE SET high_seq=excluded.high_seq, "
                "updated_at=excluded.updated_at",
                (point_id, high_seq, now),
            )

    # ---------------- 去重 ----------------

    def dedup_insert_if_absent(self, point_id: str, seq: int, seen_at: float) -> bool:
        """不存在则插入并返回 True；已存在（重复样本）返回 False。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO dedup(point_id, seq, seen_at) VALUES(?,?,?)",
                (point_id, seq, seen_at),
            )
            return cur.rowcount == 1

    def dedup_prune(self, older_than: float) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute("DELETE FROM dedup WHERE seen_at < ?", (older_than,))
            return cur.rowcount

    # ---------------- 告警 ----------------

    def insert_alert(self, a: dict[str, Any]) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO alerts(alert_id, point_id, level, state, rule_version,"
                " config_version, opened_at, updated_at, violation_count, message,"
                " stats_json, locator_json, suppressed_by)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    a["alert_id"], a["point_id"], a["level"], a["state"],
                    a["rule_version"], a["config_version"], a["opened_at"],
                    a["updated_at"], a["violation_count"], a["message"],
                    json.dumps(a.get("stats", {}), ensure_ascii=False),
                    json.dumps(a.get("locator", {}), ensure_ascii=False),
                    a.get("suppressed_by"),
                ),
            )

    def update_alert(self, alert_id: str, **fields: Any) -> None:
        json_fields = {"stats", "locator"}
        cols, vals = [], []
        for k, v in fields.items():
            cols.append(f"{k}_json=?" if k in json_fields else f"{k}=?")
            vals.append(json.dumps(v, ensure_ascii=False) if k in json_fields else v)
        vals.append(alert_id)
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE alerts SET {', '.join(cols)} WHERE alert_id=?", vals
            )

    def get_alert(self, alert_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM alerts WHERE alert_id=?", (alert_id,)
            ).fetchone()
        return self._alert_row(row) if row else None

    def query_alerts(
        self,
        point_id: Optional[str] = None,
        level: Optional[str] = None,
        state: Optional[str] = None,
        config_version: Optional[str] = None,
        since: Optional[float] = None,
        until: Optional[float] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        sql, args = "SELECT * FROM alerts WHERE 1=1", []
        if point_id:
            sql += " AND point_id=?"; args.append(point_id)
        if level:
            sql += " AND level=?"; args.append(level)
        if state:
            sql += " AND state=?"; args.append(state)
        if config_version:
            sql += " AND config_version=?"; args.append(config_version)
        if since is not None:
            sql += " AND opened_at>=?"; args.append(since)
        if until is not None:
            sql += " AND opened_at<=?"; args.append(until)
        sql += " ORDER BY opened_at DESC LIMIT ? OFFSET ?"
        args += [limit, offset]
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [self._alert_row(r) for r in rows]

    def open_alerts(self) -> list[dict[str, Any]]:
        """重启后恢复未确认（active）告警用。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM alerts WHERE state='active'"
            ).fetchall()
        return [self._alert_row(r) for r in rows]

    def _alert_row(self, row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        d["stats"] = json.loads(d.pop("stats_json") or "{}")
        d["locator"] = json.loads(d.pop("locator_json") or "{}")
        return d

    # ---------------- 告警触发样本 ----------------

    def add_alert_samples(self, alert_id: str, samples: list[dict[str, Any]]) -> None:
        with self._lock, self._conn:
            self._conn.executemany(
                "INSERT OR IGNORE INTO alert_samples(alert_id, point_id, seq, ts, value,"
                " config_version) VALUES(?,?,?,?,?,?)",
                [
                    (alert_id, s["point_id"], s["seq"], s.get("ts"), s["value"],
                     s["config_version"])
                    for s in samples
                ],
            )

    def get_alert_samples(self, alert_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT point_id, seq, ts, value, config_version FROM alert_samples"
                " WHERE alert_id=? ORDER BY ts, seq",
                (alert_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ---------------- 事件（数据质量等留痕） ----------------

    def add_event(self, kind: str, point_id: Optional[str], payload: dict[str, Any],
                  now: float) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO events(kind, point_id, payload_json, created_at)"
                " VALUES(?,?,?,?)",
                (kind, point_id, json.dumps(payload, ensure_ascii=False), now),
            )
            return int(cur.lastrowid)

    def query_events(self, kind: Optional[str] = None, point_id: Optional[str] = None,
                     since: Optional[float] = None, until: Optional[float] = None,
                     limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
        sql, args = "SELECT * FROM events WHERE 1=1", []
        if kind:
            sql += " AND kind=?"; args.append(kind)
        if point_id:
            sql += " AND point_id=?"; args.append(point_id)
        if since is not None:
            sql += " AND created_at>=?"; args.append(since)
        if until is not None:
            sql += " AND created_at<=?"; args.append(until)
        sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
        args += [limit, offset]
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["payload"] = json.loads(d.pop("payload_json") or "{}")
            out.append(d)
        return out

    # ---------------- 待审队列 ----------------

    def review_add(self, sample: dict[str, Any], reason: str, now: float) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO review_queue(sample_json, reason, status, created_at)"
                " VALUES(?,?, 'pending', ?)",
                (json.dumps(sample, ensure_ascii=False, default=str), reason, now),
            )
            return int(cur.lastrowid)

    def review_get(self, rid: int) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM review_queue WHERE id=?", (rid,)
            ).fetchone()
        return self._review_row(row) if row else None

    def review_list(self, status: Optional[str] = None, limit: int = 200,
                    offset: int = 0) -> list[dict[str, Any]]:
        sql, args = "SELECT * FROM review_queue", []
        if status:
            sql += " WHERE status=?"; args.append(status)
        sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
        args += [limit, offset]
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [self._review_row(r) for r in rows]

    def review_resolve(self, rid: int, status: str, resolution: dict[str, Any],
                       now: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE review_queue SET status=?, resolved_at=?, resolution_json=?"
                " WHERE id=?",
                (status, now, json.dumps(resolution, ensure_ascii=False), rid),
            )

    def _review_row(self, row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        d["sample"] = json.loads(d.pop("sample_json"))
        if d.get("resolution_json"):
            d["resolution"] = json.loads(d.pop("resolution_json"))
        else:
            d.pop("resolution_json", None)
            d["resolution"] = None
        return d

    # ---------------- 规则版本 ----------------

    def rules_save(self, version: str, content: str, now: float, applied_by: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO rules_meta(version, content, applied_at, applied_by)"
                " VALUES(?,?,?,?)",
                (version, content, now, applied_by),
            )

    def rules_latest(self) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM rules_meta ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def rules_history(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, version, applied_at, applied_by FROM rules_meta"
                " ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ---------------- 抑制 ----------------

    def suppression_add(self, s: dict[str, Any]) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO suppressions(supp_id, point_pattern, level, starts_at,"
                " ends_at, reason, created_by, created_at)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (s["supp_id"], s["point_pattern"], s.get("level"), s["starts_at"],
                 s["ends_at"], s.get("reason", ""), s.get("created_by", ""),
                 s["created_at"]),
            )

    def suppression_list(self, include_revoked: bool = True) -> list[dict[str, Any]]:
        sql = "SELECT * FROM suppressions"
        if not include_revoked:
            sql += " WHERE revoked=0"
        with self._lock:
            rows = self._conn.execute(sql + " ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]

    def suppression_revoke(self, supp_id: str) -> bool:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE suppressions SET revoked=1 WHERE supp_id=?", (supp_id,)
            )
            return cur.rowcount == 1

    # ---------------- 批次留痕 ----------------

    def batch_save(self, batch_id: str, source: str, received_at: float,
                   summary: dict[str, Any]) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO ingest_batches(batch_id, source, received_at, summary_json)"
                " VALUES(?,?,?,?)",
                (batch_id, source, received_at, json.dumps(summary, ensure_ascii=False)),
            )

    def batch_list(self, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM ingest_batches ORDER BY received_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["summary"] = json.loads(d.pop("summary_json") or "{}")
            out.append(d)
        return out
