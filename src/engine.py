"""核心评估引擎。

线程模型：所有公开方法持有 storage.lock，SQLite 以 autocommit + 显式事务
保证批次原子性。FastAPI 同步端点在线程池中调用，进程内单例安全。
"""
from __future__ import annotations

import hashlib
import json
import threading
from typing import Any, Optional

from .models import BatchIn, SampleIn
from .storage import Storage
from .util import is_finite_number, now_ms, parse_iso, point_matches, to_iso

OPEN_STATUSES = ("active", "acked", "suppressed")
MAX_TRIGGER_REFS = 50
GAP_RETENTION = 10_000  # 缺口表只保留距高水位最近的若干序列号，避免无限增长


class Engine:
    def __init__(self, storage: Storage, max_clock_skew_ms: int = 60_000):
        self.db = storage
        self.max_clock_skew_ms = max_clock_skew_ms
        self._active_version: Optional[str] = None
        self._load_active_rules()

    # ---------------------------------------------------------------- 规则
    def _load_active_rules(self) -> None:
        row = self.db.query_one(
            "SELECT version FROM rule_versions WHERE active=1 ORDER BY created_ms DESC LIMIT 1"
        )
        self._active_version = row["version"] if row else None

    @property
    def active_rule_version(self) -> Optional[str]:
        return self._active_version

    def get_active_rules(self) -> Optional[dict[str, Any]]:
        if not self._active_version:
            return None
        row = self.db.query_one(
            "SELECT rules_json, version FROM rule_versions WHERE version=?",
            (self._active_version,),
        )
        if not row:
            return None
        return {"version": row["version"], "rules": json.loads(row["rules_json"])}

    def list_rule_versions(self) -> list[dict[str, Any]]:
        rows = self.db.query_all(
            "SELECT version, active, created_ms, note FROM rule_versions ORDER BY created_ms DESC"
        )
        return [
            {"version": r["version"], "active": bool(r["active"]),
             "created_ms": r["created_ms"], "note": r["note"]}
            for r in rows
        ]

    @staticmethod
    def validate_rules(rules: dict[str, Any]) -> None:
        if not isinstance(rules, dict) or not rules:
            raise ValueError("rules 必须为非空映射: {point_id: rule}")
        for point, rule in rules.items():
            if not isinstance(rule, dict):
                raise ValueError(f"规则 {point} 必须是对象")
            window = rule.get("window_sec", 30)
            if not is_finite_number(window) or window <= 0:
                raise ValueError(f"规则 {point}: window_sec 必须为正数")
            warn = int(rule.get("warning_count", 1))
            crit = rule.get("critical_count")
            if warn < 1:
                raise ValueError(f"规则 {point}: warning_count 必须 >=1")
            if crit is not None:
                crit = int(crit)
                if crit < warn:
                    raise ValueError(f"规则 {point}: critical_count 不能小于 warning_count")
            has_bound = False
            for key in ("min", "max"):
                if rule.get(key) is not None:
                    if not is_finite_number(rule[key]):
                        raise ValueError(f"规则 {point}: {key} 必须为有限数值")
                    has_bound = True
            if rule.get("max_delta") is not None:
                if not is_finite_number(rule["max_delta"]) or rule["max_delta"] <= 0:
                    raise ValueError(f"规则 {point}: max_delta 必须为正数")
                has_bound = True
            if not has_bound:
                raise ValueError(f"规则 {point}: 至少需要 min/max/max_delta 之一")
            if rule.get("hysteresis", 0) and (
                not is_finite_number(rule["hysteresis"]) or rule["hysteresis"] < 0
            ):
                raise ValueError(f"规则 {point}: hysteresis 必须为非负数")

    def put_ruleset(self, version: str, rules: dict[str, Any], activate: bool,
                    note: str = "", actor: str = "") -> dict[str, Any]:
        self.validate_rules(rules)
        with self.db.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                existed = self.db.query_one(
                    "SELECT 1 FROM rule_versions WHERE version=?", (version,)
                )
                if activate:
                    self.db.execute("UPDATE rule_versions SET active=0")
                self.db.execute(
                    """INSERT INTO rule_versions(version, rules_json, active, created_ms, note)
                       VALUES(?,?,?,?,?)
                       ON CONFLICT(version) DO UPDATE SET
                         rules_json=excluded.rules_json,
                         active=excluded.active,
                         note=excluded.note""",
                    (version, json.dumps(rules, ensure_ascii=False, sort_keys=True),
                     1 if activate else 0, now_ms(), note),
                )
                switched = False
                if activate and self._active_version != version:
                    self._mark_switch(self._active_version, version)
                    self._active_version = version
                    switched = True
                elif activate:
                    self._active_version = version
                self.db.audit("rules.activate" if switched else "rules.put",
                              version, json.dumps({"switched": switched}, ensure_ascii=False),
                              actor)
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK")
                raise
        return {"version": version, "activated": activate, "switched": switched}

    def activate_ruleset(self, version: str, actor: str = "") -> dict[str, Any]:
        with self.db.lock:
            row = self.db.query_one("SELECT 1 FROM rule_versions WHERE version=?", (version,))
            if not row:
                raise KeyError(f"规则版本不存在: {version}")
            old = self._active_version
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self.db.execute("UPDATE rule_versions SET active=0")
                self.db.execute("UPDATE rule_versions SET active=1 WHERE version=?", (version,))
                switched = old != version
                if switched:
                    self._mark_switch(old, version)
                self._active_version = version
                self.db.audit("rules.activate", version,
                              json.dumps({"from": old, "switched": switched}), actor)
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK")
                raise
        return {"version": version, "switched": switched, "previous": old}

    def _mark_switch(self, old_version: Optional[str], new_version: str) -> None:
        """切换瞬间：旧版本产生的未结束告警标注 config_switch 并以 rule_switch
        关闭归档（仍可按 config_switch=1 检索）；之后新样本会产生携带新版本号
        的独立告警——切换前后结果分别标注、互不混淆。"""
        if old_version is None:
            return
        self.db.execute(
            """UPDATE alerts
                  SET config_switch=1, status='closed', close_reason='rule_switch',
                      recovered_ms=?
                WHERE rule_version != ? AND status IN ('active','acked','suppressed')""",
            (now_ms(), new_version),
        )

    def _rule_for(self, point_id: str) -> Optional[tuple[str, dict[str, Any]]]:
        ruleset = self.get_active_rules()
        if not ruleset:
            return None
        rules = ruleset["rules"]
        rule = rules.get(point_id) or rules.get("*")
        if rule is None:
            return None
        return ruleset["version"], rule

    # ---------------------------------------------------------------- 接收
    def ingest_batch(self, batch: BatchIn) -> dict[str, Any]:
        with self.db.lock:
            # 批次幂等：batch_id 重复直接回放首次结果
            if batch.batch_id:
                prev = self.db.query_one(
                    "SELECT result_json FROM raw_batches WHERE batch_id=?", (batch.batch_id,)
                )
                if prev:
                    result = json.loads(prev["result_json"])
                    result["replayed"] = True
                    return result

            import uuid
            payload = batch.model_dump_json()
            digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
            batch_id = batch.batch_id or f"auto-{uuid.uuid4().hex}"

            self.db.execute("BEGIN IMMEDIATE")
            try:
                self._sweep_expired_suppressions(batch.satellite_id)
                result = self._ingest_locked(batch, batch_id)
                self.db.execute(
                    """INSERT OR REPLACE INTO raw_batches
                       (batch_id, satellite_id, payload_sha256, rule_version, sample_count,
                        accepted, duplicated, pending, result_json, received_ms)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, batch.satellite_id, digest, self._active_version,
                     result["received"], result["accepted"], result["duplicated"],
                     result["pending"], json.dumps(result, ensure_ascii=False), now_ms()),
                )
                self.db.execute("COMMIT")
                return result
            except Exception:
                self.db.execute("ROLLBACK")
                raise

    def _sweep_expired_suppressions(self, sat: str) -> None:
        """维护窗口到期（或窗口被删除）后，被抑制告警恢复可见：
        未确认→active，已确认→acked。每批次惰性执行一次。"""
        rows = self.db.query_all(
            """SELECT a.id, a.suppression_id FROM alerts a
               WHERE a.satellite_id=? AND a.status='suppressed'
                 AND (a.suppression_id IS NULL
                      OR a.suppression_id NOT IN (SELECT id FROM suppressions)
                      OR a.suppression_id IN (
                          SELECT id FROM suppressions WHERE end_ms < ?))""",
            (sat, now_ms()),
        )
        for r in rows:
            self.db.execute(
                """UPDATE alerts SET status=CASE WHEN acked_by IS NULL
                          THEN 'active' ELSE 'acked' END
                   WHERE id=?""",
                (r["id"],),
            )
            self.db.audit("suppression.expire", str(r["id"]), "")

    def _ingest_locked(self, batch: BatchIn, batch_id: str) -> dict[str, Any]:
        sat = batch.satellite_id
        t_now = now_ms()

        accepted: list[SampleIn] = []
        sample_results: list[dict[str, Any]] = []
        pending_items: list[dict[str, Any]] = []
        duplicated = 0
        out_of_order = 0
        accepted_seqs: set[int] = set()

        for s in batch.samples:
            # 1) 时间基准确认
            t_ms = parse_iso(s.sample_time)
            reason = None
            if s.time_quality == "bad":
                reason = "time_quality_bad"
            elif s.time_quality == "uncertain":
                reason = "time_quality_uncertain"
            elif t_ms is None:
                reason = "missing_or_bad_time"
            elif t_ms > t_now + self.max_clock_skew_ms:
                reason = "future_time_beyond_skew"

            if reason:
                pid = self._put_pending(sat, s, reason, t_ms)
                pending_items.append({"id": pid, "seq": s.seq, "point_id": s.point_id,
                                      "reason": reason, "config_version": s.config_version})
                sample_results.append({"seq": s.seq, "point_id": s.point_id,
                                       "status": "pending", "reason": reason,
                                       "config_version": s.config_version})
                continue

            # 2) 重复样本（维护窗口内重发不得重复计数）
            existed = self.db.query_one(
                "SELECT value FROM samples WHERE satellite_id=? AND seq=? AND point_id=?",
                (sat, s.seq, s.point_id),
            )
            if existed is not None:
                duplicated += 1
                sample_results.append({
                    "seq": s.seq, "point_id": s.point_id, "status": "duplicate",
                    "same_value": existed["value"] == s.value,
                    "config_version": s.config_version,
                })
                continue

            self.db.execute(
                """INSERT INTO samples
                   (satellite_id, seq, point_id, value, sample_time_ms, config_version,
                    time_quality, batch_id, ingest_time_ms)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (sat, s.seq, s.point_id, s.value, t_ms, s.config_version,
                 s.time_quality, batch_id, t_now),
            )
            accepted.append(s)
            accepted_seqs.add(s.seq)
            sample_results.append({"seq": s.seq, "point_id": s.point_id,
                                   "status": "accepted", "sample_time": s.sample_time,
                                   "config_version": s.config_version})

        # 3) 序列号水位与缺口
        hw_row = self.db.query_one("SELECT high_water, gaps_json FROM streams WHERE satellite_id=?",
                                   (sat,))
        high_water = hw_row["high_water"] if hw_row else -1
        gaps: set[int] = set(json.loads(hw_row["gaps_json"])) if hw_row else set()
        gap_filled = 0
        first_seen = high_water < 0
        for seq in sorted(accepted_seqs):
            if first_seen:
                # 流的首个观测点：起始序列号未知，不臆造缺口
                high_water = seq
                first_seen = False
                continue
            if seq <= high_water:
                out_of_order += 1
                if seq in gaps:
                    gaps.discard(seq)
                    gap_filled += 1
            else:
                gaps.update(range(high_water + 1, seq))
                high_water = seq
        if high_water - GAP_RETENTION > 0:
            gaps = {g for g in gaps if g >= high_water - GAP_RETENTION}
        self.db.execute(
            """INSERT INTO streams(satellite_id, high_water, gaps_json) VALUES(?,?,?)
               ON CONFLICT(satellite_id) DO UPDATE SET
                 high_water=excluded.high_water, gaps_json=excluded.gaps_json""",
            (sat, high_water, json.dumps(sorted(gaps))),
        )

        # 4) 滑动窗口评估 + 告警生命周期
        alerts_created: list[dict] = []
        alerts_updated: list[dict] = []
        alerts_recovered: list[dict] = []
        evaluated: list[dict] = []
        by_point: dict[str, list[SampleIn]] = {}
        for s in accepted:
            by_point.setdefault(s.point_id, []).append(s)
        for point_id, samples in by_point.items():
            resolved = self._rule_for(point_id)
            if resolved is None:
                evaluated.append({"point_id": point_id, "monitored": False})
                continue
            rule_version, rule = resolved
            anchor_ms = max(parse_iso(x.sample_time) for x in samples)
            evaluation = self._evaluate_window(sat, point_id, rule, anchor_ms)
            evaluated.append({
                "point_id": point_id, "monitored": True,
                "rule_version": rule_version,
                "anchor_time": to_iso(anchor_ms),
                "violations_in_window": evaluation["count"],
                "severity": evaluation["severity"],
            })
            change = self._reconcile_alert(sat, point_id, rule_version, rule, evaluation)
            if change == "created":
                alerts_created.append(self.get_alert(self._last_alert_id))
            elif change == "updated":
                alerts_updated.append(self._alert_ref(self._last_alert_id))
            elif change == "recovered":
                alerts_recovered.append(self._alert_ref(self._last_alert_id))

        missing = sorted(gaps)
        result = {
            "batch_id": batch_id,
            "satellite_id": sat,
            "received": len(batch.samples),
            "accepted": len(accepted),
            "duplicated": duplicated,
            "out_of_order": out_of_order,
            "gap_filled": gap_filled,
            "pending": len(pending_items),
            "pending_items": pending_items,
            "high_water": high_water,
            "missing_sequences": missing[:200],
            "missing_sequence_count": len(missing),
            "rule_version": self._active_version,
            "evaluated": evaluated,
            "alerts": {
                "created": alerts_created,
                "updated": alerts_updated,
                "recovered": alerts_recovered,
            },
            "samples": sample_results,
            "replayed": False,
        }
        return result

    # ---------------------------------------------------------------- 待审
    def _put_pending(self, sat: str, s: SampleIn, reason: str, t_ms: Optional[int]) -> int:
        raw = json.dumps(s.model_dump(), ensure_ascii=False, sort_keys=True)
        cur = self.db.execute(
            """INSERT INTO pending_samples
               (satellite_id, seq, point_id, value, config_version, time_quality, reason,
                raw_json, status, received_ms)
               VALUES(?,?,?,?,?,?,?,?, 'open', ?)
               ON CONFLICT(satellite_id, seq, point_id, reason) DO UPDATE SET
                 raw_json=excluded.raw_json, status='open',
                 resolved_ms=NULL, resolution_note=NULL""",
            (sat, s.seq, s.point_id, s.value, s.config_version, s.time_quality, reason,
             raw, now_ms()),
        )
        row = self.db.query_one(
            "SELECT id FROM pending_samples WHERE satellite_id=? AND seq=? AND point_id=? AND reason=?",
            (sat, s.seq, s.point_id, reason),
        )
        return row["id"]

    def list_pending(self, status: str = "open", satellite_id: Optional[str] = None) -> list[dict]:
        sql = "SELECT * FROM pending_samples WHERE status=?"
        params: list[Any] = [status]
        if satellite_id:
            sql += " AND satellite_id=?"
            params.append(satellite_id)
        sql += " ORDER BY received_ms DESC LIMIT 500"
        return [self._pending_dict(r) for r in self.db.query_all(sql, tuple(params))]

    def resolve_pending(self, pending_id: int, action: str,
                        corrected_time: Optional[str], note: Optional[str],
                        operator: str = "") -> dict[str, Any]:
        with self.db.lock:
            row = self.db.query_one("SELECT * FROM pending_samples WHERE id=?", (pending_id,))
            if not row:
                raise KeyError(f"待审样本不存在: {pending_id}")
            if row["status"] != "open":
                return {"id": pending_id, "status": row["status"], "changed": False}
            if action == "accept":
                raw = json.loads(row["raw_json"])
                final_time = corrected_time or (
                    to_iso(row["corrected_time_ms"]) if row["corrected_time_ms"] else None
                )
                if final_time is None and row["reason"] != "time_quality_uncertain":
                    raise ValueError("该待审原因需要提供 corrected_time")
                if final_time is None:
                    final_time = to_iso(now_ms())
                if parse_iso(final_time) is None:
                    raise ValueError("corrected_time 不是合法 ISO8601")
                sample = SampleIn(
                    seq=raw["seq"], point_id=raw["point_id"], value=raw["value"],
                    sample_time=final_time, config_version=raw.get("config_version"),
                    time_quality="good",
                )
                batch = BatchIn(satellite_id=row["satellite_id"], samples=[sample],
                                batch_id=f"pending-{pending_id}")
                ingest_result = self.ingest_batch(batch)
                new_status = "accepted"
            else:
                ingest_result = None
                new_status = "rejected"
            self.db.execute(
                """UPDATE pending_samples SET status=?, corrected_time_ms=?, resolution_note=?,
                   resolved_ms=? WHERE id=?""",
                (new_status,
                 parse_iso(corrected_time) if corrected_time else None,
                 note, now_ms(), pending_id),
            )
            self.db.audit(f"pending.{new_status}", str(pending_id), note or "", operator)
            return {"id": pending_id, "status": new_status, "changed": True,
                    "ingest": _strip_replay(ingest_result)}

    # ---------------------------------------------------------------- 评估
    def _evaluate_window(self, sat: str, point_id: str, rule: dict, anchor_ms: int) -> dict:
        window_ms = int(float(rule.get("window_sec", 30)) * 1000)
        start_ms = anchor_ms - window_ms
        rows = self.db.query_all(
            """SELECT seq, value, sample_time_ms, config_version FROM samples
               WHERE satellite_id=? AND point_id=? AND sample_time_ms > ? AND sample_time_ms <= ?
               ORDER BY sample_time_ms, seq""",
            (sat, point_id, start_ms, anchor_ms),
        )
        prev = self.db.query_one(
            """SELECT value FROM samples WHERE satellite_id=? AND point_id=?
                  AND sample_time_ms <= ? ORDER BY sample_time_ms DESC, seq DESC LIMIT 1""",
            (sat, point_id, start_ms),
        )
        prev_value = prev["value"] if prev else None

        rng_min = rule.get("min")
        rng_max = rule.get("max")
        max_delta = rule.get("max_delta")
        hyst = float(rule.get("hysteresis", 0) or 0)

        violating: list[dict] = []
        all_samples: list[dict] = []
        for r in rows:
            v = r["value"]
            range_bad = False
            below = rng_min is not None and v < rng_min
            above = rng_max is not None and v > rng_max
            if below or above:
                range_bad = True
            delta_bad = max_delta is not None and prev_value is not None and \
                abs(v - prev_value) > max_delta
            bad = range_bad or delta_bad
            ref = {"seq": r["seq"], "point_id": point_id, "value": v,
                   "sample_time_ms": r["sample_time_ms"],
                   "sample_time": to_iso(r["sample_time_ms"]),
                   "config_version": r["config_version"],
                   "range_violation": range_bad, "delta_violation": bool(delta_bad)}
            all_samples.append(ref)
            if bad:
                violating.append(ref)
            prev_value = v

        count = len(violating)
        warn = int(rule.get("warning_count", 1))
        crit_cfg = rule.get("critical_count")
        crit = int(crit_cfg) if crit_cfg is not None else None
        severity = None
        if crit is not None and count >= crit:
            severity = "critical"
        elif count >= warn:
            severity = "warning"

        # 恢复判定：窗口内无违规，且最新值回到迟滞区间内（范围规则），
        # 避免贴边抖动导致告警反复开合。
        recovered = False
        latest = rows[-1] if rows else None
        if latest is not None and count == 0:
            v = latest["value"]
            recovered = True
            if rng_min is not None and v < rng_min + hyst:
                recovered = False
            if rng_max is not None and v > rng_max - hyst:
                recovered = False
        return {
            "count": count, "severity": severity, "violating": violating,
            "all": all_samples, "window_start_ms": start_ms, "anchor_ms": anchor_ms,
            "recovered": recovered, "latest_row": latest,
            "hysteresis": hyst,
        }

    # ---------------------------------------------------------------- 告警
    def _find_suppression(self, sat: str, point_id: str, severity: str,
                          event_ms: int) -> Optional[int]:
        rows = self.db.query_all(
            """SELECT id, point_pattern, severities FROM suppressions
               WHERE satellite_id=? AND start_ms<=? AND end_ms>=?""",
            (sat, event_ms, event_ms),
        )
        for r in rows:
            if severity not in json.loads(r["severities"]):
                continue
            if point_matches(r["point_pattern"], point_id):
                return r["id"]
        return None

    _last_alert_id: Optional[int] = None

    def _reconcile_alert(self, sat: str, point_id: str, rule_version: str,
                         rule: dict, ev: dict) -> Optional[str]:
        existing = self.db.query_one(
            """SELECT * FROM alerts WHERE satellite_id=? AND point_id=?
                 AND status IN ('active','acked','suppressed')
               ORDER BY id DESC LIMIT 1""",
            (sat, point_id),
        )
        if ev["severity"] is None:
            if existing and ev["recovered"]:
                self.db.execute(
                    "UPDATE alerts SET status='closed', recovered_ms=?, close_reason='recovered' WHERE id=?",
                    (ev["anchor_ms"], existing["id"]),
                )
                self.db.audit("alert.recover", str(existing["id"]), "")
                self._last_alert_id = existing["id"]
                return "recovered"
            self._last_alert_id = existing["id"] if existing else None
            return None

        latest_bad = ev["violating"][-1]
        event_ms = latest_bad["sample_time_ms"]
        first_bad_ms = ev["violating"][0]["sample_time_ms"]
        refs = [{k: r[k] for k in ("seq", "point_id", "value", "sample_time", "sample_time_ms",
                                   "range_violation", "delta_violation", "config_version")}
                for r in ev["violating"]]
        for ref in refs:
            ref["rule_version"] = rule_version
        suppression_id = self._find_suppression(sat, point_id, ev["severity"], event_ms)

        if existing:
            stored_refs = json.loads(existing["trigger_samples"])
            known_seqs = {r["seq"] for r in stored_refs}
            merged = stored_refs + [r for r in refs if r["seq"] not in known_seqs]
            merged.sort(key=lambda r: r["sample_time_ms"])
            merged = merged[-MAX_TRIGGER_REFS:]
            escalated = existing["severity"] == "warning" and ev["severity"] == "critical"
            new_status = existing["status"]
            if suppression_id:
                new_status = "suppressed"
            elif existing["status"] == "suppressed":
                # 已越过维护窗口且仍在越界：恢复可见
                new_status = "active" if existing["acked_by"] is None else "acked"
            self.db.execute(
                """UPDATE alerts SET severity=?, trigger_samples=?, last_event_ms=?,
                      window_start_ms=?, window_end_ms=?, value=?, suppression_id=?,
                      status=?, config_version=?
                   WHERE id=?""",
                (ev["severity"], json.dumps(merged, ensure_ascii=False), event_ms,
                 ev["window_start_ms"], ev["anchor_ms"], latest_bad["value"],
                 suppression_id, new_status, latest_bad.get("config_version"),
                 existing["id"]),
            )
            self.db.audit("alert.update", str(existing["id"]),
                          json.dumps({"escalated": escalated, "severity": ev["severity"]}))
            self._last_alert_id = existing["id"]
            return "updated"

        status = "suppressed" if suppression_id else "active"
        cur = self.db.execute(
            """INSERT INTO alerts
               (satellite_id, point_id, severity, rule_version, config_version, status,
                value, trigger_samples, window_start_ms, window_end_ms,
                first_event_ms, last_event_ms, created_ms, suppression_id)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sat, point_id, ev["severity"], rule_version, latest_bad.get("config_version"),
             status, latest_bad["value"], json.dumps(refs, ensure_ascii=False),
             ev["window_start_ms"], ev["anchor_ms"], first_bad_ms, event_ms, now_ms(),
             suppression_id),
        )
        self.db.audit("alert.create", str(cur.lastrowid),
                      json.dumps({"severity": ev["severity"], "suppressed": bool(suppression_id)}))
        self._last_alert_id = cur.lastrowid
        return "created"

    def _alert_ref(self, alert_id: Optional[int]) -> Optional[dict]:
        if alert_id is None:
            return None
        a = self.get_alert(alert_id)
        return {"id": a["id"], "severity": a["severity"], "status": a["status"],
                "point_id": a["point_id"], "rule_version": a["rule_version"],
                "satellite_id": a["satellite_id"],
                "trigger_sample_refs": a["trigger_sample_refs"]}

    def get_alert(self, alert_id: int) -> dict[str, Any]:
        r = self.db.query_one("SELECT * FROM alerts WHERE id=?", (alert_id,))
        if not r:
            raise KeyError(f"告警不存在: {alert_id}")
        return self._alert_dict(r)

    def ack_alert(self, alert_id: int, operator: str, note: Optional[str]) -> dict[str, Any]:
        with self.db.lock:
            alert = self.get_alert(alert_id)
            if alert["status"] not in ("active", "suppressed"):
                return {**alert, "changed": False, "message": "告警已确认或已关闭"}
            new_status = "acked"
            self.db.execute(
                "UPDATE alerts SET status=?, acked_by=?, ack_time_ms=?, ack_note=? WHERE id=?",
                (new_status, operator, now_ms(), note, alert_id),
            )
            self.db.audit("alert.ack", str(alert_id), note or "", operator)
            return {**self.get_alert(alert_id), "changed": True}

    def list_alerts(self, *, satellite_id: Optional[str] = None,
                    point_id: Optional[str] = None,
                    severity: Optional[list[str]] = None,
                    status: Optional[list[str]] = None,
                    start_ms: Optional[int] = None, end_ms: Optional[int] = None,
                    rule_version: Optional[str] = None,
                    config_switch: Optional[bool] = None,
                    limit: int = 100, offset: int = 0) -> dict[str, Any]:
        where, params = [], []
        if satellite_id:
            where.append("satellite_id=?"); params.append(satellite_id)
        if point_id:
            where.append("point_id=?"); params.append(point_id)
        if severity:
            where.append(f"severity IN ({','.join('?'*len(severity))})"); params.extend(severity)
        if status:
            where.append(f"status IN ({','.join('?'*len(status))})"); params.extend(status)
        if start_ms is not None:
            where.append("last_event_ms>=?"); params.append(start_ms)
        if end_ms is not None:
            where.append("first_event_ms<=?"); params.append(end_ms)
        if rule_version:
            where.append("rule_version=?"); params.append(rule_version)
        if config_switch is not None:
            where.append("config_switch=?"); params.append(1 if config_switch else 0)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        total = self.db.query_one(f"SELECT COUNT(*) c FROM alerts{clause}", tuple(params))["c"]
        rows = self.db.query_all(
            f"SELECT * FROM alerts{clause} ORDER BY first_event_ms DESC, id DESC LIMIT ? OFFSET ?",
            tuple(params) + (max(1, min(limit, 1000)), max(0, offset)),
        )
        return {"total": total, "limit": limit, "offset": offset,
                "items": [self._alert_dict(r) for r in rows]}

    def _alert_dict(self, r: Any) -> dict[str, Any]:
        refs = json.loads(r["trigger_samples"])
        return {
            "id": r["id"],
            "satellite_id": r["satellite_id"],
            "point_id": r["point_id"],
            "severity": r["severity"],
            "status": r["status"],
            "rule_version": r["rule_version"],
            "config_version": r["config_version"],
            "config_switch": bool(r["config_switch"]),
            "config_phase": "before_switch" if r["config_switch"] else "current",
            "value": r["value"],
            "trigger_samples": refs,
            "trigger_sample_refs": [
                {"satellite_id": r["satellite_id"], "point_id": x["point_id"],
                 "seq": x["seq"], "sample_time": x["sample_time"], "value": x["value"],
                 "locator": f"{r['satellite_id']}/{x['point_id']}#{x['seq']}"}
                for x in refs
            ],
            "window": {"start": to_iso(r["window_start_ms"]),
                       "end": to_iso(r["window_end_ms"])},
            "first_event_time": to_iso(r["first_event_ms"]),
            "last_event_time": to_iso(r["last_event_ms"]),
            "created_time": to_iso(r["created_ms"]),
            "acked_by": r["acked_by"],
            "ack_time": to_iso(r["ack_time_ms"]),
            "ack_note": r["ack_note"],
            "recovered_time": to_iso(r["recovered_ms"]),
            "suppression_id": r["suppression_id"],
            "close_reason": r["close_reason"],
        }

    # ---------------------------------------------------------------- 抑制
    def add_suppression(self, sat: str, point_pattern: str, severities: list[str],
                        start_iso: str, end_iso: str, reason: str,
                        operator: str = "") -> dict[str, Any]:
        start_ms, end_ms = parse_iso(start_iso), parse_iso(end_iso)
        if start_ms is None or end_ms is None:
            raise ValueError("时间必须为合法 ISO8601")
        if end_ms <= start_ms:
            raise ValueError("end_time 必须晚于 start_time")
        with self.db.lock:
            cur = self.db.execute(
                """INSERT INTO suppressions
                   (satellite_id, point_pattern, severities, start_ms, end_ms, reason,
                    created_by, created_ms) VALUES(?,?,?,?,?,?,?,?)""",
                (sat, point_pattern, json.dumps(severities), start_ms, end_ms, reason,
                 operator, now_ms()),
            )
            sid = cur.lastrowid
            # 立即作用于窗口内已存在的活动告警
            matched = self.db.query_all(
                """SELECT id, point_id FROM alerts
                   WHERE satellite_id=? AND status IN ('active','acked')
                     AND first_event_ms<=? AND last_event_ms>=?""",
                (sat, end_ms, start_ms),
            )
            applied = []
            for m in matched:
                if point_matches(point_pattern, m["point_id"]):
                    alert = self.get_alert(m["id"])
                    if alert["severity"] in severities:
                        self.db.execute(
                            "UPDATE alerts SET status='suppressed', suppression_id=? WHERE id=?",
                            (sid, m["id"]),
                        )
                        applied.append(m["id"])
            self.db.audit("suppression.create", str(sid),
                          json.dumps({"applied_alerts": applied}, ensure_ascii=False), operator)
            return {"id": sid, "satellite_id": sat, "point_id": point_pattern,
                    "severities": severities,
                    "start_time": to_iso(start_ms), "end_time": to_iso(end_ms),
                    "reason": reason, "applied_alert_ids": applied}

    def list_suppressions(self, active_at_ms: Optional[int] = None) -> list[dict]:
        sql = "SELECT * FROM suppressions"
        params: list[Any] = []
        if active_at_ms is not None:
            sql += " WHERE start_ms<=? AND end_ms>=?"
            params.extend([active_at_ms, active_at_ms])
        sql += " ORDER BY start_ms DESC"
        out = []
        for r in self.db.query_all(sql, tuple(params)):
            out.append({"id": r["id"], "satellite_id": r["satellite_id"],
                        "point_id": r["point_pattern"],
                        "severities": json.loads(r["severities"]),
                        "start_time": to_iso(r["start_ms"]),
                        "end_time": to_iso(r["end_ms"]),
                        "reason": r["reason"], "created_by": r["created_by"]})
        return out

    def delete_suppression(self, suppression_id: int, operator: str = "") -> None:
        with self.db.lock:
            self.db.execute("DELETE FROM suppressions WHERE id=?", (suppression_id,))
            self.db.audit("suppression.delete", str(suppression_id), "", operator)

    # ---------------------------------------------------------------- 查询
    def stream_state(self, sat: Optional[str] = None) -> list[dict]:
        sql = "SELECT satellite_id, high_water, gaps_json FROM streams"
        params: tuple = ()
        if sat:
            sql += " WHERE satellite_id=?"; params = (sat,)
        return [{"satellite_id": r["satellite_id"], "high_water": r["high_water"],
                 "missing_sequences": json.loads(r["gaps_json"])}
                for r in self.db.query_all(sql, params)]

    def list_samples(self, sat: str, point_id: Optional[str] = None,
                     start_ms: Optional[int] = None, end_ms: Optional[int] = None,
                     limit: int = 200) -> dict:
        where = ["satellite_id=?"]; params: list[Any] = [sat]
        if point_id:
            where.append("point_id=?"); params.append(point_id)
        if start_ms is not None:
            where.append("sample_time_ms>=?"); params.append(start_ms)
        if end_ms is not None:
            where.append("sample_time_ms<=?"); params.append(end_ms)
        clause = " WHERE " + " AND ".join(where)
        rows = self.db.query_all(
            f"SELECT * FROM samples{clause} ORDER BY sample_time_ms DESC, seq DESC LIMIT ?",
            tuple(params) + (max(1, min(limit, 2000)),),
        )
        return {"items": [{
            "satellite_id": r["satellite_id"], "seq": r["seq"], "point_id": r["point_id"],
            "value": r["value"], "sample_time": to_iso(r["sample_time_ms"]),
            "config_version": r["config_version"], "time_quality": r["time_quality"],
            "batch_id": r["batch_id"],
        } for r in rows]}

    def get_batch(self, batch_id: str) -> Optional[dict]:
        r = self.db.query_one("SELECT * FROM raw_batches WHERE batch_id=?", (batch_id,))
        if not r:
            return None
        return {"batch_id": r["batch_id"], "satellite_id": r["satellite_id"],
                "payload_sha256": r["payload_sha256"], "rule_version": r["rule_version"],
                "sample_count": r["sample_count"], "accepted": r["accepted"],
                "duplicated": r["duplicated"], "pending": r["pending"],
                "received_time": to_iso(r["received_ms"]),
                "result": json.loads(r["result_json"])}

    @staticmethod
    def _pending_dict(r: Any) -> dict[str, Any]:
        return {"id": r["id"], "satellite_id": r["satellite_id"], "seq": r["seq"],
                "point_id": r["point_id"], "value": r["value"],
                "config_version": r["config_version"], "time_quality": r["time_quality"],
                "reason": r["reason"], "status": r["status"],
                "corrected_time": to_iso(r["corrected_time_ms"]),
                "resolution_note": r["resolution_note"],
                "received_time": to_iso(r["received_ms"])}


def _strip_replay(result: Optional[dict]) -> Optional[dict]:
    if result:
        result.pop("replayed", None)
    return result
